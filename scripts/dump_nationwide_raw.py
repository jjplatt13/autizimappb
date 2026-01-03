import json
import requests

RAPIDAPI_KEY = "YOUR_KEY_HERE"

BASE_URL = "https://yellow-page-us.p.rapidapi.com/"

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "yellow-page-us.p.rapidapi.com"
}

# Single-state version (Florida)
LOCATION = "florida"


def dump_category(keyword, outfile):
    print(f"\n📡 Requesting Florida data for: {keyword} ...")

    params = {
        "ypkeyword": keyword,
        "yplocation": LOCATION,
        "yppage": 1
    }

    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    print(f"➡ Status: {response.status_code}")

    try:
        data = response.json()
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved: {outfile}")
    except Exception as e:
        print("❌ JSON error:", e)
        print("Raw response:", response.text)


# ---- RUN 2 CALLS ----

dump_category("aba therapy", "florida_aba_raw.json")
dump_category("speech therapy", "florida_speech_raw.json")

print("\n🎉 DONE! Only 2 credits used.")
