import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# All U.S. states (2-letter codes)
STATE_CODES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY"
]

BASE_URL = "https://abafinder.com/directory?state="

def fetch_page(url):
    """Download HTML and convert to BeautifulSoup."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Could not fetch {url}: {e}")
        return None


def parse_provider_card(card, state):
    """Extract provider details from a single result card."""
    name = card.find("h3")
    name = name.get_text(strip=True) if name else ""

    address_div = card.find("div", string=lambda t: t and ("GA" in t or "FL" in t or "," in t))
    address = address_div.get_text(strip=True) if address_div else ""

    phone_div = card.find("a", href=lambda x: x and "tel:" in x)
    phone = phone_div.get_text(strip=True) if phone_div else ""

    website_div = card.find("a", href=lambda x: x and x.startswith("http"))
    website = website_div["href"] if website_div else ""

    profile_link = card.find("a", href=lambda x: x and "/provider/" in x)
    profile = "https://abafinder.com" + profile_link["href"] if profile_link else ""

    return {
        "name": name,
        "address": address,
        "phone": phone,
        "website": website,
        "state": state,
        "profile_url": profile
    }


def scrape_state(state_code):
    """Scrape one state’s results."""
    url = BASE_URL + state_code
    print(f"🔎 Scraping {state_code}: {url}")

    soup = fetch_page(url)
    if soup is None:
        return []

    results = []

    # Provider cards are under <div class="col-sm-12 col-md-6 col-lg-4 ...">
    provider_cards = soup.find_all("div", class_=lambda c: c and "col" in c)

    for card in provider_cards:
        data = parse_provider_card(card, state_code)
        if data["name"]:  # Ignore empty cards
            results.append(data)

    return results


def run_scraper():
    all_results = []

    for state in STATE_CODES:
        data = scrape_state(state)
        all_results.extend(data)
        time.sleep(1)  # Be polite

    if not all_results:
        print("❌ No results found — structure may need adjustment.")
        return

    df = pd.DataFrame(all_results)
    df.drop_duplicates(subset=["name", "state"], inplace=True)

    df.to_csv("abafinder_providers.csv", index=False)
    print("✅ DONE! Saved to abafinder_providers.csv")


if __name__ == "__main__":
    run_scraper()
