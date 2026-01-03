import requests

URLS = [
    "https://www.appliedbehavioranalysisedu.org/sitemap.xml",
    "https://www.appliedbehavioranalysisedu.org/sitemap_index.xml",
    "https://www.appliedbehavioranalysisedu.org/sitemap",
    "https://www.appliedbehavioranalysisedu.org/sitemap1.xml",
    "https://www.appliedbehavioranalysisedu.org/sitemap.txt",
]

print("🔍 Checking for sitemaps on AppliedBehaviorAnalysisEDU...\n")

any_found = False

for url in URLS:
    print(f"🟦 Trying: {url}")
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            print("   ✅ FOUND!")
            print("   Preview:")
            print("-----------------------------------")
            print(r.text[:1000])
            print("-----------------------------------\n")
            any_found = True
        else:
            print(f"   ❌ Not found (status {r.status_code})\n")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}\n")

if not any_found:
    print("⚠️ No sitemap files exist on the server.")
    print("This confirms the site may have removed all directory structure.\n")
