import pandas as pd
import requests
import time
import math

INPUT_FILE = r"C:\Users\zubby\AUTIZIM BOT\abafinder_GLIDE_READY.csv"
OUTPUT_FILE = r"C:\Users\zubby\AUTIZIM BOT\abafinder_GLIDE_PRO.csv"

def geocode(address):
    """Uses free Nominatim API to convert address → lat/lon"""
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }
        headers = {
            "User-Agent": "AutizimBot-Geocoder/1.0"
        }

        r = requests.get(url, params=params, headers=headers)
        data = r.json()

        if data:
            return data[0]["lat"], data[0]["lon"]

    except Exception as e:
        print("❌ Error:", e)

    return "", ""  # fallback if no match

# Load Glide-ready CSV
df = pd.read_csv(INPUT_FILE)

# Create lat/lng columns
df["lat"] = ""
df["lng"] = ""

print("🌍 Starting geocoding...")

for i, row in df.iterrows():
    # Handle missing ZIP (nan)
    zip_code = "" if (pd.isna(row["zip"]) or str(row["zip"]) == "nan") else str(row["zip"])

    # Build full address safely
    full_address = f"{row['address']}, {row['city']}, {row['state']} {zip_code}"

    print(f"➡️ {i+1}/{len(df)} Geocoding: {full_address}")

    # CALL CORRECT FUNCTION: geocode()
    lat, lng = geocode(full_address)

    df.at[i, "lat"] = lat
    df.at[i, "lng"] = lng

    time.sleep(1)  # required by Nominatim usage policy

print("✅ Geocoding finished. Saving file...")

df.to_csv(OUTPUT_FILE, index=False)

print(f"📁 Done! File saved as: {OUTPUT_FILE}")
