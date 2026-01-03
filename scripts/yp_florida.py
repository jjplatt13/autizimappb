import requests
import json
import time

API_KEY = "c380e9259fmshb31d9313b25279bp15657djsn2c7cc12159b9"

BASE_URL = "https://yellow-pages-api.p.rapidapi.com/search"

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "yellow-pages-api.p.rapidapi.com"
}

CITIES = [
    "Miami", "Orlando", "Tampa", "Jacksonville", "St. Petersburg",
    "Hialeah", "Fort Lauderdale", "Tallahassee", "Cape Coral",
    "Port St. Lucie", "Pembroke Pines", "Hollywood", "Gainesville",
    "Miramar", "Coral Springs", "Clearwater", "Palm Bay",
    "Pompano Beach", "West Palm Beach", "Lakeland", "Brandon",
    "Boca Raton", "Sunrise", "Deltona", "Daytona Beach"
]


def scrape_category(search_term, outfile):
    print(f"\n📡 Scraping Florida for: {search_term}")

    all_results = []
    seen_ids = set()

    for city in CITIES:
        print(f"\n🏙️ City: {city}")
        page = 1

        while True:
            params = {
                "search_terms": search_term,
                "geo_location_terms": city + ", FL",
                "page": page
            }

            response = requests.get(BASE_URL, headers=HEADERS, params=params)
            print(f"   ➡ Page {page}: Status {response.status_code}")

            if response.status_code != 200:
                print("   ❌ Stopping this city — non-200 response.")
                break

            data = response.json()
            results = data.get("business_listings", [])

            if not results:
                print("   ✔ No more results for this city.")
                break

            for item in results:
                item_id = item.get("listing_id") or item.get("id")
                if item_id and item_id not in seen_ids:
                    all_results.append(item)
                    seen_ids.add(item_id)

            page += 1
            time.sleep(0.4)

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print(f"\n💾 Saved {len(all_results)} unique records → {outfile}")


# Run both categories
scrape_category("aba therapy", "florida_aba.json")
scrape_category("speech therapy", "florida_speech.json")

print("\n🎉 DONE!")
