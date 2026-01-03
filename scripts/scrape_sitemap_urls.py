import requests
import xml.etree.ElementTree as ET

SITEMAPS = [
    "https://www.appliedbehavioranalysisedu.org/post-sitemap.xml",
    "https://www.appliedbehavioranalysisedu.org/page-sitemap.xml"
]

OUTPUT = "abaedu_urls.txt"

KEYWORDS = [
    "aba", "autism", "behavior", "behaviour", "therapy",
    "bcba", "clinic", "center", "services", "development"
]

def fetch_urls(url):
    print(f"🟦 Fetching sitemap: {url}")
    r = requests.get(url)
    urls = []

    root = ET.fromstring(r.text)

    for child in root:
        for loc in child:
            if loc.tag.endswith("loc"):
                urls.append(loc.text.strip())
    return urls

def main():
    all_urls = []

    for sm in SITEMAPS:
        urls = fetch_urls(sm)
        all_urls.extend(urls)

    print(f"\n📌 Total URLs found: {len(all_urls)}")

    # Filter relevant pages
    filtered = [u for u in all_urls if any(k in u.lower() for k in KEYWORDS)]
    print(f"🎯 ABA-related pages found: {len(filtered)}")

    with open(OUTPUT, "w") as f:
        for url in filtered:
            f.write(url + "\n")

    print("\n✅ Saved ABA-related URLs to:", OUTPUT)


if __name__ == "__main__":
    main()
