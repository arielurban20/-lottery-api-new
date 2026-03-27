from playwright.sync_api import sync_playwright

def clean(text: str) -> str:
    import re
    return re.sub(r"\s+", " ", text or "").strip()

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
    page = browser.new_page()
    page.goto("https://www.lotterypost.com/results/wy", wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)

    sections = page.locator("section")

    for i in range(sections.count()):
        section = sections.nth(i)

        if section.locator("h2").count() == 0:
            continue

        try:
            title = clean(section.locator("h2").first.inner_text())
        except Exception:
            continue

        if "mega millions" in title.lower():
            print("\n" + "=" * 120)
            print("TITLE:", title)
            print("=" * 120)
            try:
                print(clean(section.inner_text()))
            except Exception as e:
                print("ERROR READING SECTION:", e)

    browser.close()