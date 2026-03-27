from playwright.sync_api import sync_playwright

def clean(text: str) -> str:
    import re
    return re.sub(r"\s+", " ", text or "").strip()

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
    page = browser.new_page()
    page.goto("https://www.lotterypost.com/results/wy", wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)

    full_page_text = clean(page.locator("body").inner_text())

    keywords = [
        "Mega Millions",
        "Jackpot",
        "Next Drawing",
        "Estimated Jackpot",
        "Cash Value",
        "Friday",
        "Tuesday",
        "Eastern Time",
        "from now",
    ]

    print("\n" + "=" * 140)
    print("FULL PAGE TEXT PREVIEW")
    print("=" * 140)

    for kw in keywords:
        idx = full_page_text.lower().find(kw.lower())
        if idx != -1:
            start = max(0, idx - 250)
            end = min(len(full_page_text), idx + 500)
            print("\n" + "-" * 120)
            print(f"KEYWORD: {kw}")
            print("-" * 120)
            print(full_page_text[start:end])

    browser.close()