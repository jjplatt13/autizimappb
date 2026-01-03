import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# ============================================
# SETTINGS
# ============================================

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Add all autism-related URLs here (directories, state pages, etc.)
TARGET_URLS = [
    # Example:
    # "https://example-aba-directory.com/georgia",
    # "https://example-aba-directory.com/florida",
]

# Extraction rules (generic; we will customize them per website)
EXTRACTION_RULES = {
    "name": ["h1", "h2", "h3"],
    "address": ["p", "span", "div"],
    "phone": ["p", "span", "div", "a"],
    "email": ["a"]
}

# ============================================
# SCRAPER CORE FUNCTIONS
# ============================================

def fetch_page(url: str):
    """Downloads a webpage and returns a BeautifulSoup object."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] Could not fetch {url}: {e}")
        return None


def extract_text(elements, keywords):
    """Find text inside elements that contains certain keywords."""
    for tag in elements:
        text = tag.get_text(" ", strip=True)
        if any(key.lower() in text.lower() for key in keywords):
            return text
    return ""


def scrape_page(url: str):
    """Scrapes one page and extracts provider information."""
    print(f"🔎 Scraping: {url}")
    soup = fetch_page(url)

    if soup is None:
        return []

    providers = []

    # Generic provider blocks — will adjust based on real sites you send
    provider_blocks = soup.find_all("div")

    for block in provider_blocks:
        raw = block.get_text(" ", strip=True)

        if len(raw) < 50:
            continue  # skip useless blocks

        name = extract_text(block.find_all(EXTRACTION_RULES["name"]), ["aba", "autism", "therapy", "center"])
        address = extract_text(block.find_all(EXTRACTION_RULES["address"]), ["st", "ave", "blvd", "dr", "road"])
        phone = extract_text(block.find_all(EXTRACTION_RULES["phone"]), ["("])
        email = extract_text(block.find_all(EXTRACTION_RULES["email"]), ["@"])

        if name or phone or address or email:
            providers.append({
                "name": name,
                "address": address,
                "phone": phone,
                "email": email,
                "source_url": url
            })

    return providers


# ============================================
# MAIN EXECUTION LOOP
# ============================================

def run_scraper():
    all_data = []

    for url in TARGET_URLS:
        results = scrape_page(url)
        all_data.extend(results)
        time.sleep(1)  # be polite to servers

    if not all_data:
        print("❌ No results found. Add URLs or adjust selectors.")
        return

    df = pd.DataFrame(all_data)
    df = df.drop_duplicates()

    df.to_csv("autism_providers.csv", index=False)
    print("✅ DONE! Saved to autism_providers.csv")


if __name__ == "__main__":
    run_scraper()
