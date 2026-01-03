import os
import pandas as pd
import re

folder = r"C:\Users\zubby\AUTIZIM BOT"
output_file = os.path.join(folder, "abafinder_GLIDE_READY.csv")

print("🔍 Loading CSVs from:", folder)

csv_files = [
    f for f in os.listdir(folder)
    if f.startswith("abafinder_") and f.endswith(".csv")
]

print(f"📄 Found {len(csv_files)} files.")

rows = []

# helper functions
def clean_phone(value):
    if pd.isna(value): return ""
    digits = re.sub(r"[^\d]", "", str(value))
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:]}"
    return digits

def clean_text(value):
    if pd.isna(value): return ""
    return str(value).replace("\n", " ").replace("\r", " ").strip()

def extract_address_parts(full):
    """Extract address, city, state, zip from messy combined fields"""
    if pd.isna(full) or full.strip() == "":
        return "", "", "", ""

    parts = [p.strip() for p in full.split(",")]

    address = parts[0] if len(parts) > 0 else ""
    city = parts[-2] if len(parts) > 2 else ""
    state_zip = parts[-1] if len(parts) > 1 else ""

    state_match = re.match(r"([A-Z]{2})\s*(\d{5})?", state_zip)
    if state_match:
        state = state_match.group(1)
        zipcode = state_match.group(2) if state_match.group(2) else ""
    else:
        state = ""
        zipcode = ""

    return address, city, state, zipcode


for file in csv_files:
    print("➡️ Reading:", file)
    df = pd.read_csv(os.path.join(folder, file), dtype=str)

    for _, row in df.iterrows():
        name = clean_text(row.get("name", ""))
        phone = clean_phone(row.get("phone", ""))
        website = clean_text(row.get("website", ""))
        email = clean_text(row.get("email", ""))
        location = clean_text(row.get("location", ""))

        address, city, state_guess, zip_code = extract_address_parts(location)

        source_state = file.replace("abafinder_", "").replace(".csv", "")

        rows.append({
            "name": name,
            "phone": phone,
            "website": website,
            "email": email,
            "address": address,
            "city": city,
            "state": state_guess if state_guess else source_state,
            "zip": zip_code,
            "source_state": source_state
        })

cleaned_df = pd.DataFrame(rows)

# Remove exact duplicates
cleaned_df.drop_duplicates(subset=["name", "address"], inplace=True)

cleaned_df.to_csv(output_file, index=False)

print("\n✅ GLIDE-READY CSV CREATED!")
print("📁 File saved:", output_file)
print("📊 Total rows:", len(cleaned_df))
