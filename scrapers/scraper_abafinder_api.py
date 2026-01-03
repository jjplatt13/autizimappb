import aiohttp
import asyncio
import csv

BASE_URL = "https://api.abafinder.com/api/v1/provider"
LIMIT = 100  # max per request
CONCURRENT_REQUESTS = 10  # safe async limit

# All 50 U.S. state codes
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE",
    "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY"
]


# ----------------------------------------------------
#                FETCH PAGE (ASYNC)
# ----------------------------------------------------
async def fetch_page(session, page):
    url = f"{BASE_URL}?sortBy=title&order=ASC&limit={LIMIT}&page={page}"

    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status != 200:
                print(f"⚠️ API ERROR {resp.status} on page {page}")
                return None
            return await resp.json()
    except Exception as e:
        print(f"⚠️ Connection error on page {page}: {e}")
        return None


# ----------------------------------------------------
#         FIXED: STATE FILTER BASED ON REAL API
# ----------------------------------------------------
def provider_is_in_state(provider, state_code):

    if not isinstance(provider, dict):
        return False

    # ABAFinder uses "location", not "locations"
    loc = provider.get("location", {})
    if not isinstance(loc, dict):
        return False

    state_obj = loc.get("state", {})
    if not isinstance(state_obj, dict):
        return False

    short = state_obj.get("shortName", "")

    # Match "GA", "FL", "CA", etc.
    return short.upper() == state_code.upper()


# ----------------------------------------------------
#           CLEAN PROVIDER INTO CSV FIELDS
# ----------------------------------------------------
def extract_provider_row(provider):
    title = provider.get("title", "").strip()

    contact = provider.get("contact", {})
    phone = contact.get("phone", "")
    website = contact.get("site", "")
    email = contact.get("email", "")

    location = provider.get("location", {})
    address = location.get("address", "")
    city = location.get("city", "")

    state_obj = location.get("state", {})
    state_short = state_obj.get("shortName", "")
    zip_code = location.get("zipCode", "")

    full_location = f"{address}, {city}, {state_short} {zip_code}".strip()

    details = provider.get("providerDetails", {})

    insurance = details.get("insurance", [])
    insurance_list = "; ".join(insurance)

    settings = details.get("treatmentSetting", [])
    settings_list = "; ".join(settings)

    services = details.get("otherRelatedServices", [])
    services_list = "; ".join(services)

    languages = details.get("languages", [])
    languages_list = "; ".join(languages)

    ages = details.get("agesServicing", [])
    ages_list = "; ".join(ages)

    return {
        "name": title,
        "phone": phone,
        "website": website,
        "email": email,
        "location": full_location,
        "insurance": insurance_list,
        "treatmentSetting": settings_list,
        "services": services_list,
        "languages": languages_list,
        "agesServicing": ages_list,
    }


# ----------------------------------------------------
#               SCRAPE ONE STATE
# ----------------------------------------------------
async def scrape_state(session, state_code):
    print(f"\n🔍 Scraping state: {state_code}")

    all_results = []
    page = 0

    while True:
        data = await fetch_page(session, page)
        if not data:
            break

        providers = data.get("data", {}).get("result", [])
        total = data.get("data", {}).get("total", 0)

        if not providers:
            break

        print(f"📦 {state_code}: Page {page} returned {len(providers)} providers")

        for p in providers:
            if provider_is_in_state(p, state_code):
                row = extract_provider_row(p)
                all_results.append(row)

        page += 1
        await asyncio.sleep(0.1)

        if page * LIMIT > total:
            break

    print(f"🎯 {state_code}: Total providers found = {len(all_results)}")

    # Save CSV
    fieldnames = [
        "name", "phone", "website", "email", "location",
        "insurance", "treatmentSetting", "services",
        "languages", "agesServicing"
    ]

    output_file = f"abafinder_{state_code}.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"💾 Saved {state_code} providers to {output_file}")


# ----------------------------------------------------
#                 RUN ALL STATES
# ----------------------------------------------------
async def main():
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = []
        for state_code in US_STATES:
            tasks.append(scrape_state(session, state_code))

        await asyncio.gather(*tasks)


# ----------------------------------------------------
#                     EXECUTION
# ----------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting 50-state async ABAFinder scraper...")
    asyncio.run(main())
    print("\n✅ DONE! All states scraped successfully.")
