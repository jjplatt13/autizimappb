import asyncio
from playwright.async_api import async_playwright
import csv
import time
import urllib.parse
import requests

# ================================================================
# CONFIGURATION
# ================================================================

LOCATION = "Florida"
RELAY_KEY = "AUTIZIM123"
RELAY_ENDPOINT = "https://api.autizim-relay.com/yp"

GOOGLE_API_KEY = "AIzaSyA29z_Kl9AceIK2EE6MXBJjeY88Gf3C65o"

SEARCH_CATEGORIES = {
    "speech_therapists_florida.csv": [
        "speech therapist",
        "speech pathology",
        "speech therapy",
    ],
    "aba_therapy_florida.csv": [
        "aba therapy",
        "applied behavior analysis",
        "autism aba",
        "behavior therapist",
    ],
    "autism_therapy_florida.csv": [
        "autism therapy",
        "autism services",
        "autism support center",
        "child developmental therapy",
    ],
}

OUTPUT_COLUMNS = [
    "name","phone","website","email","full_address","city","state","zip","country",
    "lat","lon","insurance","treatmentSetting","services","languages","agesServicing"
]

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
window.chrome = { runtime: {} };
"""


# ================================================================
# GOOGLE GEOCODING
# ================================================================
def geocode_address(address):
    try:
        url = (
            "https://maps.googleapis.com/maps/api/geocode/json"
            f"?address={urllib.parse.quote(address)}"
            f"&key={GOOGLE_API_KEY}"
        )
        r = requests.get(url)
        data = r.json()

        if data["status"] != "OK":
            return "", "", "", "", "", ""

        result = data["results"][0]

        lat = result["geometry"]["location"]["lat"]
        lon = result["geometry"]["location"]["lng"]

        city = state = zip_code = country = ""

        for comp in result["address_components"]:
            if "locality" in comp["types"]:
                city = comp["long_name"]
            if "administrative_area_level_1" in comp["types"]:
                state = comp["short_name"]
            if "postal_code" in comp["types"]:
                zip_code = comp["long_name"]
            if "country" in comp["types"]:
                country = comp["long_name"]

        return city, state, zip_code, country, lat, lon

    except:
        return "", "", "", "", "", ""


# ================================================================
# CLOUDLFARE RELAY FETCH
# ================================================================
async def relay_fetch(url):
    payload = {
        "key": RELAY_KEY,
        "url": url,
        "render": False,
        "mobile": False
    }

    try:
        res = requests.post(RELAY_ENDPOINT, json=payload, timeout=30)
        if res.status_code != 200:
            print(f"⚠ Relay returned status {res.status_code}")
            return None
        return res.text
    except Exception as e:
        print(f"❌ Relay error: {e}")
        return None


# ================================================================
# PROCESS A SINGLE LISTING
# ================================================================
async def process_listing(element):
    try:
        name = await element.locator("a.business-name span").inner_text()
    except:
        name = ""

    try:
        phone = await element.locator(".phones").inner_text()
    except:
        phone = ""

    try:
        street = await element.locator(".street-address").inner_text()
    except:
        street = ""

    try:
        locality = await element.locator(".locality").inner_text()
    except:
        locality = ""

    try:
        website = await element.locator("a.track-visit-website").get_attribute("href")
    except:
        website = ""

    full_address = f"{street}, {locality}".strip(", ")

    # Geo
    city, state, zip_code, country, lat, lon = geocode_address(full_address)

    return {
        "name": name,
        "phone": phone,
        "website": website or "",
        "email": "",
        "full_address": full_address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "country": country,
        "lat": lat,
        "lon": lon,
        "insurance": "",
        "treatmentSetting": "",
        "services": "",
        "languages": "",
        "agesServicing": ""
    }


# ================================================================
# SCRAPE CATEGORY
# ================================================================
async def scrape_category(page, search_term, output_rows):
    base_url = (
        "https://www.yellowpages.com/search?"
        f"search_terms={search_term.replace(' ', '+')}&"
        f"geo_location_terms={LOCATION.replace(' ', '+')}"
    )

    print(f"\n🌎 Relay Fetch → {base_url}")
    html = await relay_fetch(base_url)

    if not html:
        print("❌ Relay returned empty HTML — skipping keyword.")
        return

    await page.set_content(html)

    elements = await page.locator("div.result").all()
    total = len(elements)
    print(f"🔍 Listings found: {total}")

    for i, element in enumerate(elements):
        print(f"   → Processing listing {i+1}/{total}")
        row = await process_listing(element)
        output_rows.append(row)

        await asyncio.sleep(0.05)  # small cooldown


# ================================================================
# MAIN RUNNER
# ================================================================
async def run_scraper():

    print("""
=====================================
  YELLOWPAGES RELAY SCRAPER (FL)
           CF BYPASS
=====================================
""")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        page = await context.new_page()
        await page.add_init_script(STEALTH_JS)

        # Loop categories
        for filename, keywords in SEARCH_CATEGORIES.items():

            print(f"\n==============================")
            print(f" SCRAPING FILE: {filename}")
            print(f"==============================\n")

            rows = []

            for term in keywords:
                print(f"\n🔎 Searching: {term}")
                await scrape_category(page, term, rows)

            # Save CSV
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
                writer.writeheader()
                writer.writerows(rows)

            print(f"\n💾 Saved {len(rows)} rows → {filename}")

        await browser.close()

    print("\n🎉 DONE — All Florida CSV files generated!\n")


# ================================================================
# ENTRY
# ================================================================
if __name__ == "__main__":
    asyncio.run(run_scraper())
