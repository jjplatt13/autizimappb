import asyncio
import csv
import random
import time
from playwright.async_api import async_playwright

STATES = [
    "alabama","alaska","arizona","arkansas","california","colorado",
    "connecticut","delaware","florida","georgia","hawaii","idaho",
    "illinois","indiana","iowa","kansas","kentucky","louisiana",
    "maine","maryland","massachusetts","michigan","minnesota",
    "mississippi","missouri","montana","nebraska","nevada",
    "new hampshire","new jersey","new mexico","new york",
    "north carolina","north dakota","ohio","oklahoma","oregon",
    "pennsylvania","rhode island","south carolina","south dakota",
    "tennessee","texas","utah","vermont","virginia","washington",
    "west virginia","wisconsin","wyoming"
]

OUTPUT = "bhcoe_all_states.csv"


async def human_type(element, text):
    """Type text into a field like a real human."""
    for char in text:
        await element.type(char, delay=random.randint(50, 150))
        await asyncio.sleep(random.uniform(0.05, 0.2))


async def human_move_mouse(page):
    """Move mouse around randomly to appear human."""
    for _ in range(4):
        x = random.randint(50, 800)
        y = random.randint(100, 900)
        await page.mouse.move(x, y, steps=15)
        await asyncio.sleep(random.uniform(0.1, 0.4))


async def click_load_more(page):
    """Repeatedly click LOAD MORE."""
    while True:
        btn = await page.query_selector("button:has-text('LOAD MORE')")
        if not btn:
            return
        await btn.click()
        await page.wait_for_timeout(2000)


async def scrape_one_state(page, state):
    print(f"\n📍 Scraping state: {state.upper()}")

    await page.goto("https://www.bhcoe.org/aba-therapy-directory/", timeout=60000)

    await human_move_mouse(page)
    await page.wait_for_timeout(1500)

    # Focus the search box
    search_box = await page.wait_for_selector("input[type='search']", timeout=25000)
    await search_box.click()
    await human_move_mouse(page)

    # Clear
    await search_box.fill("")
    await asyncio.sleep(0.8)

    # Human-like typing of state name
    await human_type(search_box, state)
    await asyncio.sleep(1.8)

    # Scroll to trigger JS events
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight/3);")
    await asyncio.sleep(1.5)

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
    await asyncio.sleep(1.5)

    # Load more
    await click_load_more(page)

    cards = await page.query_selector_all(".provider-card")
    print(f"   ➜ Found: {len(cards)} providers")

    results = []

    for card in cards:
        name_tag = await card.query_selector("h3")
        name = await name_tag.inner_text() if name_tag else ""

        loc_tag = await card.query_selector("p")
        location = await loc_tag.inner_text() if loc_tag else ""

        desc_tag = await card.query_selector("div")
        description = await desc_tag.inner_text() if desc_tag else ""

        link_tag = await card.query_selector("a")
        link = await link_tag.get_attribute("href") if link_tag else ""

        city = ""
        st = ""

        if "," in location:
            parts = location.split(",")
            city = parts[0].strip()
            st = parts[1].strip()

        results.append({
            "name": name,
            "city": city,
            "state": st,
            "description": description,
            "website": link,
            "source_state": state,
            "latitude": "",
            "longitude": ""
        })

    return results


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, slow_mo=80)
        page = await browser.new_page()

        all_data = []

        for state in STATES:
            try:
                state_results = await scrape_one_state(page, state)
                all_data.extend(state_results)
            except Exception as e:
                print(f"❌ Error in {state}: {e}")

        with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "name","city","state","description",
                "website","source_state","latitude","longitude"
            ])
            writer.writeheader()
            for row in all_data:
                writer.writerow(row)

        print("\n✅ COMPLETE!")
        print("➡️ Saved to:", OUTPUT)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
