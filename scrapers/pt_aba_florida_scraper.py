import requests
from bs4 import BeautifulSoup
import csv
import time
import urllib.parse

# ---------------------------
# SETTINGS
# ---------------------------

STATE = "Florida"
BASE_URL = "https://www.psychologytoday.com/us/therapists/aba/fl"

OUTPUT_CSV = "florida_aba_providers.csv"

# OpenStreetMap Geocode API
GEOCODE_URL = "https://nominatim.openstreetmap.org/search"


# ---------------------------
# LAT / LON FUNCTION
# ---------------------------
def get_lat_lon(address):
    try:
        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }
        r = requests.get(GEOCODE_URL, params=params, headers={"User-Agent": "Mozilla/5.0"})
        data = r.json()

        if len(data) == 0:
            return "", ""

        return data[0]["lat"], data[0]["lon"]

    except:
        return "", ""


# ---------------------------
# SCRAPE ONE PAGE
# ---------------------------
def scrape_page(url):
    print(f"🔍 Scraping page: {url}")
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(r.text, "html.parser")

    providers = soup.select(".results-row")

    data_list = []

    for p in providers:
        try:
            name = p.select_one(".result-name").get_text(strip=True) if p.select_one(".result-name") else ""
            phone = p.select_one(".result-phone").get_text(strip=True) if p.select_one(".result-phone") else ""
            website = p.select_one(".result-website a")["href"] if p.select_one(".result-website a") else ""
            email = p.select_one("a[data-umami-event='send email']")["href"].replace("mailto:", "") if p.select_one("a[data-umami-event='send email']") else ""

            # Address
            address_block = p.select_one(".address")
            address = address_block.get_text(" ", strip=True) if address_block else ""

            # Details Section
            description = p.select_one(".teaser").get_text(" ", strip=True) if p.select_one(".teaser") else ""

            # lat / lon lookup
            lat, lon = get_lat_lon(address)
            time.sleep(1)  # protect geocode API

            row = {
                "name": name,
                "phone": phone,
                "email": email,
                "website": website,
                "address": address,
                "lat": lat,
                "lon": lon,
                "description": description,
            }

            data_list.append(row)

        except Exception as e:
            print(f"⚠ Error on provider: {e}")
            continue

    return data_list


# ---------------------------
# SCRAPE ALL PAGES
# ---------------------------
def scrape_florida():
    all_data = []

    page_num = 1
    while True:
        url = f"{BASE_URL}?page={page_num}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

        if "No Results Found" in r.text or r.status_code != 200:
            break

        page_data = scrape_page(url)

        if len(page_data) == 0:
            break

        all_data.extend(page_data)

        page_num += 1
        time.sleep(1)

    return all_data


# ---------------------------
# SAVE CSV
# ---------------------------
def save_csv(data):
    keys = ["name", "phone", "email", "website", "address", "lat", "lon", "description"]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

    print(f"✅ Saved {len(data)} providers to {OUTPUT_CSV}")


# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    print("🚀 Scraping PsychologyToday ABA providers — FLORIDA")
    results = scrape_florida()
    save_csv(results)
    print("🎉 Done!")
