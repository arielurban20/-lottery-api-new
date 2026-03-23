import json
import re
from datetime import datetime
from playwright.sync_api import sync_playwright

URL = "https://www.lotterypost.com/results/ks"

GAME_RULES = {
    "Pick 3 Midday": {"main_count": 3},
    "Pick 3 Evening": {"main_count": 3},
    "Super Kansas Cash": {"main_count": 5},
    "2by2": {"main_count": 4},
    "Lotto America": {"main_count": 5},
    "Millionaire for Life": {"main_count": 5},
    "Mega Millions": {"main_count": 5},
    "Powerball": {"main_count": 5},
    "Powerball Double Play": {"main_count": 5},
}

def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()

def parse_date(text):
    try:
        return datetime.strptime(clean(text), "%A, %B %d, %Y").date().isoformat()
    except:
        return None

def extract_all_li_numbers(section):
    nums = []
    items = section.locator("ul.resultsnums li")
    for i in range(items.count()):
        try:
            txt = clean(items.nth(i).inner_text())
            if txt.isdigit():
                nums.append(int(txt))
        except:
            pass
    return nums

def extract_extras(section):
    full_text = clean(section.inner_text())

    bonus_number = None
    multiplier = None

    bonus_patterns = [
        r"Cash Ball:\s*(\d{1,2})",
        r"Star Ball:\s*(\d{1,2})",
        r"Powerball:\s*(\d{1,2})",
        r"Mega Ball:\s*(\d{1,2})",
        r"Millionaire Ball:\s*(\d{1,2})",
    ]

    mult_patterns = [
        r"All Star Bonus:\s*(\d{1,2})",
        r"Power Play:\s*([Xx]?\d+)",
        r"Megaplier:\s*([Xx]?\d+)",
    ]

    for pattern in bonus_patterns:
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            bonus_number = m.group(1).strip()
            break

    for pattern in mult_patterns:
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            value = m.group(1).strip().upper()
            multiplier = value if value.startswith("X") else f"X{value}"
            break

    return bonus_number, multiplier, full_text

def split_main_numbers(title, raw_numbers):
    rule = GAME_RULES.get(title)
    if not rule:
        return raw_numbers
    return raw_numbers[: rule["main_count"]]

def scrape():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False)
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(6000)

        sections = page.locator("section")
        count = sections.count()

        print("SECTIONS FOUND:", count)

        results = []

        for i in range(count):
            section = sections.nth(i)

            if section.locator("h2").count() == 0:
                continue

            try:
                title = clean(section.locator("h2").first.inner_text())
            except:
                continue

            if not title:
                continue

            draw_date = None
            if section.locator("time").count() > 0:
                try:
                    draw_date = parse_date(section.locator("time").first.inner_text())
                except:
                    pass

            raw_numbers = extract_all_li_numbers(section)
            bonus_number, multiplier, debug_text = extract_extras(section)
            main_numbers = split_main_numbers(title, raw_numbers)

            results.append({
                "title": title,
                "date": draw_date,
                "main_numbers": main_numbers,
                "bonus_number": bonus_number,
                "multiplier": multiplier,
                "raw_numbers": raw_numbers,
                "debug_text": debug_text[:1000]
            })

        browser.close()
        return results

def main():
    data = scrape()

    print("\nRESULTADOS KANSAS")
    print("=" * 60)
    print(json.dumps(data, indent=2, ensure_ascii=False))

    with open("kansas_dom_results.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("\nSUMMARY")
    print("=" * 60)
    print("Games found:", len(data))
    print("Report saved: kansas_dom_results.json")

if __name__ == "__main__":
    main()