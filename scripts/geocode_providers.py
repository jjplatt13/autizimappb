import csv
import requests
import time
import re
import sys
import os
from difflib import SequenceMatcher
from datetime import datetime
from urllib.parse import quote  # For URL encoding addresses

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
MAPBOX_TOKEN = "pk.eyJ1IjoienViYnkxOTA3IiwiYSI6ImNtaXJ3MmF4cjEzbWwzZXBxcWVzYWUweTgifQ.LpSbgj8g1njHtHCLutZW8g"

INPUT_FILE = "test_providers.csv"

# Archive system - auto-creates results folder
RESULTS_FOLDER = "results"
if not os.path.exists(RESULTS_FOLDER):
    os.makedirs(RESULTS_FOLDER)

# Timestamped output for each run + master file
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_FILE = f"{RESULTS_FOLDER}/geocoded_{TIMESTAMP}.csv"
MASTER_FILE = f"{RESULTS_FOLDER}/master_all_providers.csv"
CHECKPOINT_FILE = "geocode_checkpoint.csv"  # Save progress

GARBAGE_PREFIXES = (">", "<", "#", "//", "--")

# Processing config
CHECKPOINT_INTERVAL = 50  # Save progress every N rows
MAX_RETRIES = 3  # Retry failed API calls
API_TIMEOUT = 15  # Increased from 10
RATE_LIMIT_DELAY = 0.25  # 250ms between calls
AUTO_CLEANUP = True  # Automatically clean output (remove empty columns, extra spaces)

# ---------------------------------------------------
# Regex Helpers
# ---------------------------------------------------
PHONE_RE = re.compile(r"(\+?\d[\d\-\(\) ]{7,}\d)")
EMAIL_RE = re.compile(r"[^@]+@[^@]+\.[^@]+")
URL_RE = re.compile(r"https?://[^\s]+|www\.[^\s]+")
ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")
STATE_RE = re.compile(
    r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b"
)

STREET_WORDS = [
    "st", "street", "ave", "avenue", "road", "rd", "drive", "dr",
    "lane", "ln", "court", "ct", "blvd", "boulevard", "terrace",
    "place", "parkway", "pkwy", "way", "circle", "plaza"
]

# ---------------------------------------------------
# Smart Column Matching
# ---------------------------------------------------
COLUMN_PATTERNS = {
    "name": ["name", "title", "business", "provider", "facility", "organization", "company"],
    "address": ["address", "addr", "location", "street_address", "full_address", "fulladdress"],
    "street": ["street", "street_address", "addr1", "address1", "address_line"],
    "city": ["city", "town", "municipality"],
    "state": ["state", "st", "province"],
    "zipcode": ["zip", "zipcode", "postal", "postalcode", "postal_code"],
    "latitude": ["lat", "latitude", "y", "coord_lat"],
    "longitude": ["lon", "lng", "long", "longitude", "x", "coord_lon", "coord_lng"],
    "phone": ["phone", "tel", "telephone", "contact", "mobile", "cell"],
    "email": ["email", "e-mail", "mail", "contact_email"],
    "website": ["website", "site", "web", "url", "homepage", "link"],
    "rating": ["rating", "stars", "score", "review_score"],
    "reviews": ["reviews", "review_count", "num_reviews", "total_reviews"],
    "place_id": ["place_id", "google_id", "id", "placeid"],
}

def fuzzy_match_column(col_name, patterns, threshold=0.6):
    """Match column name to patterns with fuzzy matching."""
    col_clean = col_name.lower().strip().replace("_", "").replace(" ", "")
    for pattern in patterns:
        pattern_clean = pattern.replace("_", "").replace(" ", "")
        if pattern_clean in col_clean or col_clean in pattern_clean:
            return True
        if SequenceMatcher(None, col_clean, pattern_clean).ratio() > threshold:
            return True
    return False

def find_column(header, field_name):
    """Intelligently find column index for a field."""
    patterns = COLUMN_PATTERNS.get(field_name, [field_name])
    for i, col in enumerate(header):
        if fuzzy_match_column(col, patterns):
            return i
    return None

# ---------------------------------------------------
# Smart Data Detection
# ---------------------------------------------------
def looks_like_name(v):
    """Detect if value looks like a business/provider name."""
    if not v or len(v) < 3:
        return False
    if EMAIL_RE.search(v) or URL_RE.search(v):
        return False
    if not re.search(r'[a-zA-Z]{3,}', v):
        return False
    if len(re.findall(r'[^\w\s\-\&\.]', v)) > 3:
        return False
    return True

def looks_like_address(v):
    """Enhanced address detection."""
    if not v or len(v) < 5:
        return False
    v_lower = v.lower()
    
    has_street = any(w in v_lower.split() for w in STREET_WORDS)
    has_number = bool(re.search(r'\b\d+\b', v))
    has_zip = bool(ZIP_RE.search(v))
    has_state = bool(STATE_RE.search(v))
    
    return (has_street and has_number) or (has_zip and has_state)

def is_valid_coordinate(lat, lon, strict_us=True):
    """Validate coordinates."""
    if lat is None or lon is None:
        return False
    try:
        lat, lon = float(lat), float(lon)
    except:
        return False
    
    if strict_us:
        return 18 <= lat <= 72 and -180 <= lon <= -66
    return -90 <= lat <= 90 and -180 <= lon <= 180

def extract_best_value(row_dict, header, row, field_name, validator=None):
    """Extract the best value for a field across all columns."""
    col_idx = find_column(header, field_name)
    if col_idx is not None and col_idx < len(row):
        val = row[col_idx].strip()
        if val and (not validator or validator(val)):
            return val
    
    for val in row:
        val = val.strip()
        if val and (not validator or validator(val)):
            return val
    return ""

# ---------------------------------------------------
# Geocoding Functions with Retry Logic
# ---------------------------------------------------
def forward_geocode(address, retry_count=0):
    """Forward geocode with retry logic."""
    if not address:
        return None, None, None  # Return error message too
    
    # URL encode the address to handle special characters (#, spaces, etc.)
    encoded_address = quote(address)
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{encoded_address}.json"
    params = {"access_token": MAPBOX_TOKEN, "limit": 1}
    
    try:
        response = requests.get(url, params=params, timeout=API_TIMEOUT)
        
        # Check for rate limiting
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                time.sleep(2 ** retry_count)  # Exponential backoff
                return forward_geocode(address, retry_count + 1)
            return None, None, "Rate limited"
        
        # Check for API errors
        if response.status_code != 200:
            return None, None, f"API error: {response.status_code}"
        
        data = response.json()
        if data.get("features"):
            lon, lat = data["features"][0]["center"]
            return lat, lon, None
        
        return None, None, "No results found"
        
    except requests.Timeout:
        if retry_count < MAX_RETRIES:
            return forward_geocode(address, retry_count + 1)
        return None, None, "Timeout"
    
    except requests.RequestException as e:
        if retry_count < MAX_RETRIES:
            time.sleep(1)
            return forward_geocode(address, retry_count + 1)
        return None, None, f"Network error: {str(e)}"
    
    except Exception as e:
        return None, None, f"Error: {str(e)}"

def reverse_geocode(lat, lon, retry_count=0):
    """Reverse geocode with retry logic."""
    if lat is None or lon is None:
        return None, None
    
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
    params = {"access_token": MAPBOX_TOKEN, "limit": 1}
    
    try:
        response = requests.get(url, params=params, timeout=API_TIMEOUT)
        
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                time.sleep(2 ** retry_count)
                return reverse_geocode(lat, lon, retry_count + 1)
            return None, "Rate limited"
        
        if response.status_code != 200:
            return None, f"API error: {response.status_code}"
        
        data = response.json()
        if data.get("features"):
            return data["features"][0]["place_name"], None
        
        return None, "No results found"
        
    except Exception as e:
        if retry_count < MAX_RETRIES:
            time.sleep(1)
            return reverse_geocode(lat, lon, retry_count + 1)
        return None, f"Error: {str(e)}"

def extract_city_state_zip(address):
    """Extract city, state, zip from address."""
    if not address:
        return None, None, None

    zipcode = ZIP_RE.search(address)
    zipcode = zipcode.group(0) if zipcode else None

    state = STATE_RE.search(address)
    state = state.group(0) if state else None

    parts = [p.strip() for p in address.split(",")]
    city = None
    if len(parts) >= 2 and state:
        city = parts[-2]

    return city, state, zipcode

def extract_street(address):
    """Extract street from address."""
    if not address:
        return None
    return address.split(",")[0].strip()

# ---------------------------------------------------
# Progress Tracking
# ---------------------------------------------------
def print_progress(current, total, start_time, geocoded_count, skipped_count):
    """Print progress bar and stats."""
    percent = (current / total) * 100
    elapsed = time.time() - start_time
    rate = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / rate if rate > 0 else 0
    
    bar_length = 40
    filled = int(bar_length * current / total)
    bar = '█' * filled + '░' * (bar_length - filled)
    
    sys.stdout.write(f'\r[{bar}] {current}/{total} ({percent:.1f}%) | '
                     f'New: {geocoded_count} | Skipped: {skipped_count} | '
                     f'Rate: {rate:.1f} rows/s | '
                     f'ETA: {int(eta)}s    ')
    sys.stdout.flush()

def save_checkpoint(output_rows, fields, filename=CHECKPOINT_FILE):
    """Save progress to checkpoint file."""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(output_rows)

# ---------------------------------------------------
# CSV Cleanup Functions
# ---------------------------------------------------
def clean_cell_value(value):
    """Clean individual cell values."""
    if not value:
        return ""
    
    # Remove extra whitespace
    value = str(value).strip()
    
    # Remove multiple spaces
    value = re.sub(r'\s+', ' ', value)
    
    return value

def cleanup_output_rows(rows, fields):
    """Clean up output rows and remove empty columns."""
    if not rows:
        return rows, fields
    
    # Clean all cell values
    for row in rows:
        for key in row.keys():
            if row[key]:
                row[key] = clean_cell_value(row[key])
    
    # Find which columns are completely empty
    empty_fields = set()
    for field in fields:
        if all(not row.get(field) or not str(row.get(field)).strip() for row in rows):
            empty_fields.add(field)
    
    # Remove empty columns
    new_fields = [f for f in fields if f not in empty_fields]
    
    # Filter rows to only include remaining fields
    filtered_rows = []
    for row in rows:
        filtered_row = {k: v for k, v in row.items() if k in new_fields}
        filtered_rows.append(filtered_row)
    
    return filtered_rows, new_fields

# ---------------------------------------------------
# DYNAMIC SCHEMA BUILDER
# ---------------------------------------------------
# Geocoding columns (always added if missing)
GEOCODING_COLUMNS = ["street", "city", "state", "zipcode", "latitude", "longitude"]
STATUS_COLUMNS = ["status", "error_message"]

def build_dynamic_schema(input_header):
    """Build output schema dynamically based on input columns."""
    # Start with all input columns
    output_fields = list(input_header)
    
    # Add geocoding columns if not present
    for col in GEOCODING_COLUMNS:
        col_idx = find_column(input_header, col)
        if col_idx is None and col not in output_fields:
            output_fields.append(col)
    
    # Add status columns at the end
    for col in STATUS_COLUMNS:
        if col not in output_fields:
            output_fields.append(col)
    
    return output_fields

# ---------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------
def main():
    print(f"\n🚀 Starting geocoding: {INPUT_FILE}")
    print(f"⚙️  Config: Checkpoint every {CHECKPOINT_INTERVAL} rows, {MAX_RETRIES} retries")
    print(f"💡 Tip: Rows with valid coordinates will be skipped (no API calls)")
    print(f"🧹 Cleaning: Error messages in lat/lon columns will be ignored")
    print(f"🔧 Fixed: Addresses with special characters (#, etc.) now properly encoded")
    print(f"✨ Auto-cleanup: {'Enabled' if AUTO_CLEANUP else 'Disabled'} (removes empty columns)\n")
    
    output_rows = []
    geocoded_count = 0
    skipped_count = 0  # Already have valid coordinates
    error_count = 0
    start_time = time.time()
    
    with open(INPUT_FILE, encoding="utf-8") as f:
        # Find header
        header = None
        for raw_line in f:
            clean_line = raw_line.strip().lstrip("\ufeff")
            if clean_line == "" or clean_line.startswith(GARBAGE_PREFIXES):
                continue
            row = next(csv.reader([clean_line]))
            if row:
                header = [h.strip().lower() for h in row]
                break

        if not header:
            print("❌ ERROR: Could not find header row.")
            return

        # Build dynamic output schema
        output_fields = build_dynamic_schema(header)
        print(f"   📋 Input: {len(header)} columns")
        print(f"   📐 Output: {len(output_fields)} columns (preserving all input + geocoding)\n")

        f.seek(0)
        for raw_line in f:
            cleaned = raw_line.strip().lstrip("\ufeff")
            if cleaned.startswith(GARBAGE_PREFIXES):
                continue
            if any(h in cleaned.lower() for h in header[:2]):
                break

        reader = csv.reader(f)
        
        # Count total rows first
        all_rows = list(reader)
        total_rows = len(all_rows)
        print(f"📊 Found {total_rows} rows to process\n")
        
        # Process rows
        for row_num, row in enumerate(all_rows, 1):
            if not any(cell.strip() for cell in row):
                continue

            if len(row) < len(header):
                row += [""] * (len(header) - len(row))

            row_dict = {header[i]: row[i].strip() for i in range(min(len(header), len(row)))}
            values = [v.strip() for v in row if v.strip()]

            # Smart extraction
            name = extract_best_value(row_dict, header, row, "name", looks_like_name)
            if not name:
                for v in values:
                    if looks_like_name(v):
                        name = v
                        break

            # Contact info
            phone = email = website = ""
            for v in values:
                if not phone and PHONE_RE.search(v):
                    phone = PHONE_RE.search(v).group(0)
                if not email and EMAIL_RE.search(v):
                    email = EMAIL_RE.search(v).group(0)
                if not website and URL_RE.search(v):
                    website = URL_RE.search(v).group(0)

            # Address detection
            full_address = extract_best_value(row_dict, header, row, "address", looks_like_address)
            if not full_address:
                for v in values:
                    if looks_like_address(v):
                        full_address = v
                        break

            # Coordinates - with robust error detection
            lat_idx = find_column(header, "latitude")
            lon_idx = find_column(header, "longitude")
            
            lat = lon = None
            had_error_text = False  # Track if we cleaned error text
            
            if lat_idx is not None and lon_idx is not None:
                try:
                    lat_val = row[lat_idx].strip()
                    lon_val = row[lon_idx].strip()
                    
                    # Ignore if contains error messages or non-numeric text
                    if lat_val and lon_val:
                        # Check for common error patterns
                        error_patterns = ['error', 'api', '404', '429', 'timeout', 'failed', 'null', 'none', 'n/a']
                        lat_has_error = any(pattern in lat_val.lower() for pattern in error_patterns)
                        lon_has_error = any(pattern in lon_val.lower() for pattern in error_patterns)
                        
                        if lat_has_error or lon_has_error:
                            had_error_text = True  # Mark that we cleaned error text
                        
                        if not lat_has_error and not lon_has_error:
                            lat, lon = float(lat_val), float(lon_val)
                            if not is_valid_coordinate(lat, lon):
                                lat = lon = None
                except:
                    pass

            # Fallback coordinate detection
            if lat is None or lon is None:
                numeric = []
                for v in values:
                    try:
                        numeric.append(float(v))
                    except:
                        pass
                
                for i, num in enumerate(numeric):
                    if -90 <= num <= 90 and i + 1 < len(numeric):
                        if is_valid_coordinate(num, numeric[i + 1]):
                            lat, lon = num, numeric[i + 1]
                            break

            # Geocoding logic with error tracking and status
            error_msg = None
            status = ""
            
            if not full_address and lat is None:
                street = city = state = zipcode = None
                error_msg = "No address or coordinates found"
                status = "❌ Error - no data"
            else:
                if is_valid_coordinate(lat, lon):
                    skipped_count += 1
                    if had_error_text:
                        status = "🧹 Cleaned (removed error text)"
                    else:
                        status = "⏭️ Already geocoded - skipped"
                elif full_address:
                    lat, lon, error_msg = forward_geocode(full_address)
                    if lat is not None:
                        geocoded_count += 1
                        status = "🆕 Newly geocoded"
                    else:
                        error_count += 1
                        status = "❌ Geocoding failed"
                    time.sleep(RATE_LIMIT_DELAY)
                elif lat and lon:
                    full_address, error_msg = reverse_geocode(lat, lon)
                    if full_address:
                        geocoded_count += 1
                        status = "🆕 Reverse geocoded"
                    time.sleep(RATE_LIMIT_DELAY)

            street = extract_street(full_address)
            city, state, zipcode = extract_city_state_zip(full_address)

            # Extract other fields
            place_id = extract_best_value(row_dict, header, row, "place_id")
            url = extract_best_value(row_dict, header, row, "url")
            rating = extract_best_value(row_dict, header, row, "rating")
            reviews = extract_best_value(row_dict, header, row, "reviews")
            
            # Build output - start with ALL input columns
            output_row = dict(row_dict)  # Copy all input data first
            
            # Add/update geocoding columns
            output_row.update({
                "street": street or output_row.get("street", ""),
                "city": city or output_row.get("city", ""),
                "state": state or output_row.get("state", ""),
                "zipcode": zipcode or output_row.get("zipcode", ""),
                "latitude": lat if lat is not None else "",
                "longitude": lon if lon is not None else "",
                "status": status,
                "error_message": error_msg or ""
            })
            
            # Ensure all output fields exist
            for field in output_fields:
                if field not in output_row:
                    output_row[field] = ""
            
            output_rows.append(output_row)
            
            # Print progress
            print_progress(row_num, total_rows, start_time, geocoded_count, skipped_count)
            
            # Save checkpoint
            if row_num % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(output_rows, output_fields)

    # Final save - segregate errors at bottom
    print("\n\n💾 Saving final output...")
    
    # Separate good rows from error rows
    # A row is an error if: has error_message OR missing valid coordinates
    good_rows = []
    error_rows = []
    
    for row in output_rows:
        has_error_msg = row.get("error_message") and row.get("error_message").strip()
        has_valid_coords = is_valid_coordinate(row.get("latitude"), row.get("longitude"))
        
        if has_error_msg or not has_valid_coords:
            error_rows.append(row)
        else:
            good_rows.append(row)
    
    # Auto-cleanup if enabled
    fields_to_write = output_fields
    if AUTO_CLEANUP:
        print("   🧹 Auto-cleanup: Removing empty columns and extra whitespace...")
        output_rows, fields_to_write = cleanup_output_rows(output_rows, output_fields)
        good_rows, _ = cleanup_output_rows(good_rows, fields_to_write)
        error_rows, _ = cleanup_output_rows(error_rows, fields_to_write)
        removed_count = len(output_fields) - len(fields_to_write)
        if removed_count > 0:
            print(f"   ✨ Removed {removed_count} empty column(s)")
    
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields_to_write)
        writer.writeheader()
        
        # Write good rows first
        writer.writerows(good_rows)
        
        # Add separator row if there are errors
        if error_rows:
            separator = {field: "=" * 20 for field in fields_to_write}
            separator["name"] = "=" * 50 + " ERRORS BELOW (scroll down) " + "=" * 50
            writer.writerow(separator)
            
            # Write error rows
            writer.writerows(error_rows)

    # Update master file (accumulates all data across runs)
    print(f"   📚 Updating master file...")
    master_exists = os.path.exists(MASTER_FILE)
    
    with open(MASTER_FILE, "a" if master_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields_to_write)
        if not master_exists:
            writer.writeheader()
        # Only add good rows to master (no errors, no separator)
        writer.writerows(good_rows)
    
    print(f"   ✅ Added {len(good_rows)} records to master file")

    elapsed = time.time() - start_time
    print(f"\n✅ DONE! Processed {len(output_rows)} rows in {elapsed:.1f}s")
    print(f"   ⏭️  Skipped (already geocoded): {len(good_rows) - geocoded_count}")
    print(f"   🆕 Newly geocoded: {geocoded_count}")
    print(f"   ✗ Errors (at bottom): {len(error_rows)}")
    print(f"\n📂 Files created:")
    print(f"   🕒 This run: {OUTPUT_FILE}")
    print(f"   📚 Master (all runs): {MASTER_FILE}")
    print(f"   💡 Tip: Check 'status' column to see what changed per row\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted! Progress saved to checkpoint file.")
        sys.exit(0)
