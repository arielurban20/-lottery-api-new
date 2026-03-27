from playwright.sync_api import sync_playwright
from scripts.scrape_all_states_dom_v2 import clean, extract_page_level_extras

TARGETS = [
    "Mega Millions",
    "Powerball",
    "Powerball Double Play",
]

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
    page = browser.new_page()
    page.goto("https://www.lotterypost.com/results/wy", wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)

    full_page_text = clean(page.locator("body").inner_text())

    for title in TARGETS:
        data = extract_page_level_extras(full_page_text, title)
        print("\n" + "=" * 100)
        print("TITLE:", title)
        print(data)

    browser.close()