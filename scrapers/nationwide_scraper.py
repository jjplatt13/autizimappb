import requests
import csv
import time

# ---------------------------
# ENTER YOUR RAPIDAPI KEY HERE
# ---------------------------
RAPIDAPI_KEY = "c380e9259fmshb31d9313b25279bp15657djsn2c7cc12159b9"

API_URL = "https://yellow-page-us.p.rapidapi.com/search"

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "yellow-page-us.p.rapidapi.com"
}

# ---------------------------------------------------
# FUNCTION: Fetch listings for 1 keyword nationwide
# ---------------------------------------------------
def fetch_listings(keyword, location="United States"):
    params = {
        "ypkeyword": keyword,
        "yplocation": location,
        "ypage": "1"
    }

    print(f"\n🔍 Fetching: {keyword} (Nationwide)...")

    try:
        response = requests.get(API_URL, headers=HEADERS, params=params)
        data = response.json()

        if "business_listings" not in data:
            print("❌ No listings found for:", keyword)
            return []

        print(f"✔ Found {len(data['business_listings'])} listings for {keyword}")
        return data["business_listings"]

    except Exception as e:
        print("❌ Error:", e)
        return []


# ----------------------------------------------
# FUNCTION: Clean + standardize each listing
# ----------------------------------------------
def clean_record(item):
    name = item.get("name", "")
    phone = item.get("phone", "")
    website = item.get("website", "")

    # Add missing optional fields
    email = ""
    insurance = ""
    treatment_setting = ""
    services = ", ".join(item.get("headings", []))
    languages = ""
    ages = ""

    city = item.get("city", "")
    state = item.get("state", "")
    zip_code = item.get("zip", "")
    address = item.get("address", "")

    full_location = f"{address}, {city}, {state} {zip_code}".strip()

    lat = item.get("latitude", "")
    lon = item.get("longitude", "")

    return [
        name,
        phone,
        website,
        email,
        full_location,
        insurance,
        treatment_setting,
        services,
        languages,
        ages,
        lat,
        lon
    ]


# ----------------------------------------------------
# FUNCTION: Save results to CSV (Glide-compatible)
# ----------------------------------------------------
def save_csv(filename, records):
    headers = [
        "name", "phone", "website", "email", "location",
        "insurance", "treatmentSetting", "services",
        "languages", "agesServicing", "latitude", "longitude"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(records)

    print(f"📁 Saved: {filename} ({len(records)} rows)")


# ----------------------------------------------------
# MAIN SCRIPT — 2 Nationwide Calls
# ----------------------------------------------------
if __name__ == "__main__":
    
    # ============================
    # 1️⃣ NATIONWIDE ABA
    # ============================
    aba_keywords = ["aba therapy"]

    aba_results = []
    for kw in aba_keywords:
        listings = fetch_listings(kw)
        time.sleep(1)
        for item in listings:
            aba_results.append(clean_record(item))

    # Deduplicate by name + address
    unique_aba = list({(r[0], r[4]): r for r in aba_results}.values())
    save_csv("nationwide_aba.csv", unique_aba)

    # ============================
    # 2️⃣ NATIONWIDE SPEECH
    # ============================
    speech_keywords = ["speech therapy"]

    speech_results = []
    for kw in speech_keywords:
        listings = fetch_listings(kw)
        time.sleep(1)
        for item in listings:
            speech_results.append(clean_record(item))

    unique_speech = list({(r[0], r[4]): r for r in speech_results}.values())
    save_csv("nationwide_speech.csv", unique_speech)

    print("\n✅ ALL DONE! Your nationwide data is ready.")
