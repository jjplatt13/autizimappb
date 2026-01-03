import time
import csv
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ==========================================
# CONFIG
# ==========================================

START_URL = "https://abafinder.com/directory?state=GA"
BASE_URL = "https://abafinder.com"
OUTPUT_FILE = "abafinder_providers.csv"


# ==========================================
# SETUP SELENIUM
# ==========================================

options = Options()
# IMPORTANT: NOT HEADLESS (site hides content in headless)
# options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,2000")

service = Service("chromedriver.exe")
driver = webdriver.Chrome(service=service, options=options)
wait = WebDriverWait(driver, 20)


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def safe_text(el):
    try:
        return el.text.strip()
    except:
        return ""


def scroll_internal_container():
    """Scrolls the provider list container until all results load."""
    try:
        container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.overflow-y-auto"))
        )
    except:
        print("❌ Could not find scrolling container.")
        return

    last_height = 0

    while True:
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight;", container
        )
        time.sleep(1.0)
        new_height = driver.execute_script("return arguments[0].scrollTop;", container)

        if new_height == last_height:
            break

        last_height = new_height


def extract_provider_details(url: str) -> dict:
    driver.get(url)
    print(f"   ↳ Scraping: {url}")
    time.sleep(2)

    details = {
        "url": url,
        "name": "",
        "address": "",
        "phone": "",
        "website": "",
        "email": "",
        "overview": "",
        "insurance": "",
        "treatment_settings": "",
        "languages": "",
        "ages_served": "",
        "services_provided": "",
        "clinicians": "",
        "contact_form": "",
    }

    # NAME
    try:
        name_el = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        details["name"] = safe_text(name_el)
    except:
        pass

    # TOP INFO BLOCK
    try:
        info_block = driver.find_element(By.CSS_SELECTOR, "div.space-y-2")
        block_text = safe_text(info_block)

        if "\n" in block_text:
            details["address"] = block_text.split("\n")[0]

        try:
            phone_el = info_block.find_element(By.XPATH, ".//a[contains(@href,'tel')]")
            details["phone"] = safe_text(phone_el)
        except:
            pass

        try:
            website_el = info_block.find_element(By.XPATH, ".//a[starts-with(@href,'http')]")
            details["website"] = website_el.get_attribute("href")
        except:
            pass

        try:
            email_el = info_block.find_element(By.XPATH, ".//a[contains(@href,'mailto')]")
            details["email"] = email_el.get_attribute("href").replace("mailto:", "")
        except:
            pass

    except:
        pass

    # COLLAPSIBLE SECTIONS
    sections = driver.find_elements(By.CSS_SELECTOR, "div.collapse")
    for section in sections:
        try:
            title = section.find_element(By.CSS_SELECTOR, ".collapse-title").text.lower()
            body = section.find_element(By.CSS_SELECTOR, ".collapse-content").text.strip()

            if "overview" in title:
                details["overview"] = body
            elif "insurance" in title:
                details["insurance"] = body
            elif "treatment" in title:
                details["treatment_settings"] = body
            elif "language" in title:
                details["languages"] = body
            elif "ages" in title:
                details["ages_served"] = body
            elif "service" in title:
                details["services_provided"] = body
            elif "clinician" in title:
                details["clinicians"] = body

        except:
            continue

    # CONTACT FORM
    try:
        cf = driver.find_element(By.XPATH, "//a[contains(text(),'Contact')]")
        details["contact_form"] = cf.get_attribute("href")
    except:
        pass

    return details


# ==========================================
# COLLECT PROVIDER LINKS
# ==========================================

def collect_provider_links():
    print("🔍 Loading GA listing...")
    driver.get(START_URL)
    time.sleep(3)

    print("📜 Scrolling internal provider list...")
    scroll_internal_container()

    cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/directory/']")
    links = []

    for c in cards:
        href = c.get_attribute("href")
        if href and "/directory/" in href and not href.endswith("/directory"):
            links.append(href)

    unique = sorted(set(links))
    print(f"📦 Found {len(unique)} providers.")
    return unique


# ==========================================
# MAIN
# ==========================================

def main():
    links = collect_provider_links()

    rows = []
    for i, link in enumerate(links, start=1):
        print(f"\n[{i}/{len(links)}] Processing provider…")
        info = extract_provider_details(link)
        rows.append(info)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        fieldnames = rows[0].keys() if rows else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"\n✅ DONE — Saved {len(rows)} providers to {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        main()
    finally:
        driver.quit()
