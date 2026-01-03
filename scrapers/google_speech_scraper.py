import os
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()

SERPAPI_KEY = os.getenv("SERPAPI_KEY")

# ------------------------------------------
# USA STATES + OPTIONAL METRO CITIES
# ------------------------------------------

US_STATES = {
    "AL": ["Birmingham", "Mobile", "Montgomery"],
    "AK": ["Anchorage"],
    "AZ": ["Phoenix", "Tucson"],
    "AR": ["Little Rock"],
    "CA": ["Los Angeles", "San Diego", "San Francisco", "Sacramento", "Fresno"],
    "CO": ["Denver", "Colorado Springs"],
    "CT": ["Hartford", "Stamford"],
    "DE": ["Wilmington"],
    "FL": ["Jacksonville", "Miami", "Tampa", "Orlando", "St Petersburg", "Fort Lauderdale"],
    "GA": ["Atlanta", "Savannah", "Augusta"],
    "HI": ["Honolulu"],
    "ID": ["Boise"],
    "IL": ["Chicago", "Naperville", "Peoria"],
    "IN": ["Indianapolis", "Fort Wayne"],
    "IA": ["Des Moines"],
    "KS": ["Wichita", "Kansas City"],
    "KY": ["Louisville", "Lexington"],
    "LA": ["New Orleans", "Baton Rouge"],
    "ME": ["Portland"],
    "MD": ["Baltimore"],
    "MA": ["Boston", "Worcester"],
    "MI": ["Detroit", "Grand Rapids"],
    "MN": ["Minneapolis", "St Paul"],
    "MS": ["Jackson"],
    "MO": ["St Louis", "Kansas City"],
    "MT": ["Billings"],
    "NE": ["Omaha", "Lincoln"],
    "NV": ["Las Vegas", "Reno"],
    "NH": ["Manchester"],
    "NJ": ["Newark", "Jersey City"],
    "NM": ["Albuquerque"],
    "NY": ["New York City", "Buffalo", "Rochester"],
    "NC": ["Charlotte", "Raleigh"],
    "ND": ["Fargo"],
    "OH": ["Columbus", "Cleveland", "Cincinnati"],
    "OK": ["Oklahoma City", "Tulsa"],
    "OR": ["Portland", "Eugene"],
    "PA": ["Philadelphia", "Pittsburgh"],
    "RI": ["Providence"],
    "SC": ["Charleston", "Columbia"],
    "SD": ["Sioux Falls"],
    "TN": ["Nashville", "Memphis", "Knoxville"],
    "TX": ["Houston", "Dallas", "San Antonio", "Austin"],
    "UT": ["Salt Lake City", "Provo"],
    "VT": ["Burlington"],
    "VA": ["Richmond", "Virginia Beach"],
    "WA": ["Seattle", "Spokane"],
    "WV": ["Charleston"],
    "WI": ["Milwaukee", "Madison"],
    "WY": ["Cheyenne"]
}

# ------------------------------------------
# SERPAPI SEARCH FUNCTION
# ------------------------------------------

def serpapi_search(query):
    url = "https://serpapi.com/search"

    params = {
        "engine": "google_maps",
        "q": query,
        "type": "search",
        "api_key": SERPAPI_KEY
    }

    resp = requests.get(url, params=params)
    data = resp.json()

    return data.get("local_results", [])


# ------------------------------------------
# MAIN SCRAPER
# ------------------------------------------

all_results = {}
total_saved = 0

print("🚀 Starting NATIONAL Speech Therapist Scraper (Option B)\n")

for state, metros in US_STATES.items():
    print(f"\n===============================")
    print(f"📍 STATE: {state}")
    print("===============================")

    base_query = f"speech therapist {state}"

    # 1) STATE-LEVEL SCRAPE
    providers = serpapi_search(base_query)
    print(f"   • Found {len(providers)} from state-level search")

    # Store temporary
    state_providers = {p.get("place_id"): p for p in providers}

    # 2) If LOW RESULTS — run metro cities (Option B logic)
    if len(providers) < 15:
        print("   ⚠️ Low results — running metro cities...")
        for city in metros:
            q = f"speech therapist {city} {state}"
            city_providers = serpapi_search(q)
            print(f"      → {city}: {len(city_providers)} found")
            for p in city_providers:
                state_providers[p.get("place_id")] = p

    # Merge into global dataset
    for pid, pdata in state_providers.items():
        all_results[pid] = pdata

    print(f"   ✅ Total unique for {state}: {len(state_providers)}")

# ------------------------------------------
# SAVE TO CSV
# ------------------------------------------

FILENAME = "speech_therapists_USA.csv"
print(f"\n💾 Saving all results to {FILENAME}...")

fieldnames = [
    "title",
    "address",
    "phone",
    "website",
    "rating",
    "reviews",
    "type",
    "place_id",
]

with open(FILENAME, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    for p in all_results.values():
        writer.writerow({
            "title": p.get("title", ""),
            "address": p.get("address", ""),
            "phone": p.get("phone", ""),
            "website": p.get("website", ""),
            "rating": p.get("rating", ""),
            "reviews": p.get("reviews", ""),
            "type": p.get("type", ""),
            "place_id": p.get("place_id", "")
        })

print(f"\n🎉 DONE! TOTAL NATIONWIDE UNIQUE PROVIDERS: {len(all_results)}")
