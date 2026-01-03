import csv
import time
import os
from dotenv import load_dotenv
import googlemaps

# Load .env file
load_dotenv()

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not API_KEY or API_KEY.strip() == "":
    raise ValueError("❌ ERROR: API key not found. Make sure GOOGLE_MAPS_API_KEY is in your .env file.")

gmaps = googlemaps.Client(key=API_KEY)

# YOUR FILE HERE:
INPUT_FILE = "abafinder_GLIDE_READY.csv"
OUTPUT_FILE = "abafinder_GLIDE_READY_with_geo.csv"

def geocode_address(address):
    try:
        result = gmaps.geocode(address)
        if result:
            location = result[0]["geometry"]["location"]
            return location["lat"], location["lng"]
    except Exception as e:
        print(f"❌ Geocoding error: {e}")

    return None, None

print("🌍 Starting fast geocoding...\n")

with open(INPUT_FILE, "r", encoding="utf-8") as infile, \
     open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ["latitude", "longitude"]
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for i, row in enumerate(reader, start=1):
        address_parts = [
            row.get("address", ""),
            row.get("city", ""),
            row.get("state", ""),
            row.get("zip", "")
        ]

        full_address = ", ".join([x for x in address_parts if x])
        print(f"➡️  {i} / geocoding: {full_address}")

        lat, lng = geocode_address(full_address)
        row["latitude"] = lat
        row["longitude"] = lng

        writer.writerow(row)

        time.sleep(0.15)  # SAFE speed for Google free tier

print("\n✅ Done! Saved to:", OUTPUT_FILE)
