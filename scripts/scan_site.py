import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import time

BASE = "https://www.appliedbehavioranalysisedu.org/"
visited = set()
results = []

def is_internal(url):
    return urlparse(url).netloc == urlparse(BASE).netloc

def crawl(url):
    if url in visited:
        return

    print("🟦 Visiting:", url)
    visited.add(url)

    try:
        r = requests.get(url, timeout=10)
    except:
        return

    soup = BeautifulSoup(r.text, "html.parser")

    # Check for ABA-related content
    text = soup.get_text().lower()

    keywords = [
        "aba", "applied behavior", "bcba", "autism", "clinic",
        "center", "therapy", "address", "suite", "street", "phone"
    ]
    if any(k in text for k in keywords):
        results.append(url)

    # Find links
    for a in soup.find_all("a", href=True):
        link = urljoin(BASE, a["href"])
        if is_internal(link):
            crawl(link)

# Start
crawl(BASE)

print("\n✅ Scan complete")
print("Found pages with ABA-like content:", len(results))

with open("abaedu_found_pages.txt", "w") as f:
    for page in results:
        f.write(page + "\n")

print("Saved URLs to: abaedu_found_pages.txt")
