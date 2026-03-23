import json
import re
from datetime import datetime

from playwright.sync_api import sync_playwright
from sqlalchemy import select, text

from app.database import SessionLocal
from app.models import Draw, Game


HEADER_STOP_MARKERS = {
    "raffle results and special draws",
    "latest lottery news",
    "drawing schedule",
}

TITLE_TO_BASE_SLUG = {
    "pick 2": "pick-2",
    "pick 2 midday": "pick-2",
    "pick 2 evening": "pick-2",
    "pick 2 day": "pick-2",
    "pick 2 night": "pick-2",
    "pick 3": "pick-3",
    "pick 3 midday": "pick-3",
    "pick 3 evening": "pick-3",
    "pick 3 day": "pick-3",
    "pick 3 night": "pick-3",
    "pick 3 daytime": "pick-3",
    "pick 3 morning": "pick-3",
    "pick 4": "pick-4",
    "pick 4 midday": "pick-4",
    "pick 4 evening": "pick-4",
    "pick 4 day": "pick-4",
    "pick 4 night": "pick-4",
    "pick 4 daytime": "pick-4",
    "pick 4 morning": "pick-4",
    "pick 5": "pick-5",
    "pick 5 midday": "pick-5",
    "pick 5 evening": "pick-5",
    "pick 5 day": "pick-5",
    "pick 5 night": "pick-5",
    "daily 3": "daily-3",
    "daily 3 midday": "daily-3",
    "daily 3 evening": "daily-3",
    "daily 4": "daily-4",
    "daily 4 midday": "daily-4",
    "daily 4 evening": "daily-4",
    "play 3 day": "play-3",
    "play 3 night": "play-3",
    "play 4 day": "play-4",
    "play 4 night": "play-4",
    "play 5 day": "play-5",
    "play 5 night": "play-5",
    "numbers midday": "numbers",
    "numbers evening": "numbers",
    "numbers game midday": "numbers-game",
    "numbers game evening": "numbers-game",
    "win 4 midday": "win-4",
    "win 4 evening": "win-4",
    "take 5 midday": "take-5",
    "take 5 evening": "take-5",
    "cash 3": "cash-3",
    "cash 3 midday": "cash-3",
    "cash 3 evening": "cash-3",
    "cash 3 morning": "cash-3",
    "cash 3 night": "cash-3",
    "cash 4": "cash-4",
    "cash 4 midday": "cash-4",
    "cash 4 evening": "cash-4",
    "cash 4 morning": "cash-4",
    "cash 4 night": "cash-4",
    "cash 5": "cash-5",
    "cash five": "cash-5",
    "fantasy 5": "fantasy-5",
    "pick 6": "pick-6",
    "lotto": "lotto",
    "lotto america": "lotto-america",
    "powerball": "powerball",
    "powerball double play": "powerball-double-play",
    "mega millions": "mega-millions",
    "millionaire for life": "millionaire-for-life",
    "2by2": "2by2",
    "cash pop": "cash-pop",
    "cash pop morning": "cash-pop",
    "cash pop matinee": "cash-pop",
    "cash pop afternoon": "cash-pop",
    "cash pop evening": "cash-pop",
    "cash pop late night": "cash-pop",
    "cash pop midday": "cash-pop",
    "cash pop early bird": "cash-pop",
    "cash pop brunch": "cash-pop",
    "cash pop drive time": "cash-pop",
    "cash pop primetime": "cash-pop",
    "cash pop prime time": "cash-pop",
    "cash pop night owl": "cash-pop",
    "cash pop morning buzz": "cash-pop",
    "cash pop lunch rush": "cash-pop",
    "cash pop clock out cash": "cash-pop",
    "cash pop primetime pop": "cash-pop",
    "cash pop midnight money": "cash-pop",
    "cash pop lunch break": "cash-pop",
    "cash pop coffee break": "cash-pop",
    "cash pop rush hour": "cash-pop",
    "cash pop after hours": "cash-pop",
    "cash pop 9am": "cash-pop",
    "cash pop 1pm": "cash-pop",
    "cash pop 6pm": "cash-pop",
    "cash pop 11pm": "cash-pop",
    "pega 2 día": "pega-2",
    "pega 2 noche": "pega-2",
    "pega 3 día": "pega-3",
    "pega 3 noche": "pega-3",
    "pega 4 día": "pega-4",
    "pega 4 noche": "pega-4",
    "all or nothing morning": "all-or-nothing",
    "all or nothing day": "all-or-nothing",
    "all or nothing evening": "all-or-nothing",
    "all or nothing night": "all-or-nothing",
    "all or nothing mid": "all-or-nothing",
    "all or nothing eve": "all-or-nothing",
    "jersey cash 5": "jersey-cash-5",
    "georgia five midday": "georgia-five",
    "georgia five evening": "georgia-five",
    "lucky day lotto midday": "lucky-day-lotto",
    "lucky day lotto evening": "lucky-day-lotto",
    "dc-3 1:50pm": "dc-3",
    "dc-3 7:50pm": "dc-3",
    "dc-3 11:30pm": "dc-3",
    "dc-4 1:50pm": "dc-4",
    "dc-4 7:50pm": "dc-4",
    "dc-4 11:30pm": "dc-4",
    "dc-5 1:50pm": "dc-5",
    "dc-5 7:50pm": "dc-5",
}

MULTI_STATE_SLUGS = {
    "powerball",
    "mega-millions",
    "millionaire-for-life",
    "lotto-america",
    "2by2",
}

GAME_RULES = {
    "powerball": {"main_count": 5, "bonus_label": "Powerball", "mult_label": "Power Play"},
    "powerball-double-play": {"main_count": 5, "bonus_label": "Powerball", "mult_label": None},
    "mega-millions": {"main_count": 5, "bonus_label": "Mega Ball", "mult_label": "Megaplier"},
    "millionaire-for-life": {"main_count": 5, "bonus_label": "Millionaire Ball", "mult_label": None},
    "lotto-america": {"main_count": 5, "bonus_label": "Star Ball", "mult_label": "All Star Bonus"},
    "2by2": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "pick-2": {"main_count": 2, "bonus_label": None, "mult_label": None},
    "pick-3": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "pick-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "pick-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "daily-3": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "daily-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "play-3": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "play-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "play-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "numbers": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "numbers-game": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "win-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "take-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "cash-3": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "cash-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "cash-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "fantasy-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "pick-6": {"main_count": 6, "bonus_label": None, "mult_label": None},
    "lotto": {"main_count": 6, "bonus_label": None, "mult_label": None},
    "cash-pop": {"main_count": 1, "bonus_label": None, "mult_label": None},
    "pega-2": {"main_count": 2, "bonus_label": None, "mult_label": None},
    "pega-3": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "pega-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "all-or-nothing": {"main_count": 12, "bonus_label": None, "mult_label": None},
    "jersey-cash-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "georgia-five": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "lucky-day-lotto": {"main_count": 5, "bonus_label": None, "mult_label": None},
    "dc-3": {"main_count": 3, "bonus_label": None, "mult_label": None},
    "dc-4": {"main_count": 4, "bonus_label": None, "mult_label": None},
    "dc-5": {"main_count": 5, "bonus_label": None, "mult_label": None},
}


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def slugify(text: str) -> str:
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")


def parse_date(text: str):
    try:
        return datetime.strptime(clean(text), "%A, %B %d, %Y").date()
    except Exception:
        return None


def get_states():
    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id, name, slug, source_url
                FROM states
                WHERE is_active = true
                ORDER BY name
            """)
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_games():
    db = SessionLocal()
    try:
        games = db.execute(select(Game).where(Game.is_active == True)).scalars().all()
        return {g.slug.lower(): g for g in games}
    finally:
        db.close()


def resolve_game_slug(title: str, state_slug: str) -> str | None:
    low = clean(title).lower()

    base_slug = TITLE_TO_BASE_SLUG.get(low)
    if not base_slug:
        base_slug = slugify(low)

    if base_slug in MULTI_STATE_SLUGS:
        return base_slug

    if base_slug == "powerball-double-play":
        return f"{base_slug}-{state_slug}"

    return f"{base_slug}-{state_slug}"


def infer_draw_type(title: str) -> str:
    low = clean(title).lower()

    checks = [
        ("midday", "midday"),
        ("evening", "evening"),
        ("morning", "morning"),
        ("night", "night"),
        ("daytime", "daytime"),
        (" day", "day"),
        (" matinee", "matinee"),
        (" afternoon", "afternoon"),
        (" late night", "late-night"),
        ("early bird", "early-bird"),
        ("brunch", "brunch"),
        ("drive time", "drive-time"),
        ("prime time", "prime-time"),
        ("primetime", "primetime"),
        ("night owl", "night-owl"),
        ("morning buzz", "morning-buzz"),
        ("lunch rush", "lunch-rush"),
        ("clock out cash", "clock-out-cash"),
        ("primetime pop", "primetime-pop"),
        ("midnight money", "midnight-money"),
        ("lunch break", "lunch-break"),
        ("coffee break", "coffee-break"),
        ("rush hour", "rush-hour"),
        ("after hours", "after-hours"),
        ("9am", "9am"),
        ("1pm", "1pm"),
        ("4pm", "4pm"),
        ("6pm", "6pm"),
        ("7pm", "7pm"),
        ("10pm", "10pm"),
        ("11pm", "11pm"),
        ("día", "dia"),
        ("noche", "noche"),
        ("double play", "double-play"),
        (" mid", "mid"),
        (" eve", "eve"),
    ]

    for needle, value in checks:
        if needle in low:
            return value

    return "main"


def extract_all_li_numbers(section) -> list[int]:
    nums = []
    items = section.locator("ul.resultsnums li")
    for i in range(items.count()):
        try:
            txt = clean(items.nth(i).inner_text())
            if txt.isdigit():
                nums.append(int(txt))
        except Exception:
            pass
    return nums


def extract_text_extras(section) -> tuple[str | None, str | None, str]:
    full_text = clean(section.inner_text())

    bonus_number = None
    multiplier = None

    bonus_patterns = [
        r"Cash Ball:\s*(\d{1,2})",
        r"Star Ball:\s*(\d{1,2})",
        r"Powerball:\s*(\d{1,2})",
        r"Mega Ball:\s*(\d{1,2})",
        r"Millionaire Ball:\s*(\d{1,2})",
        r"Bullseye:\s*(\d{1,2})",
        r"Fireball:\s*(\d{1,2})",
    ]

    mult_patterns = [
        r"All Star Bonus:\s*(\d{1,2})",
        r"Power Play:\s*([Xx]?\d+)",
        r"Megaplier:\s*([Xx]?\d+)",
        r"Multiplier:\s*([Xx]?\d+)",
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


def split_main_numbers(title: str, game_slug: str, raw_numbers: list[int]) -> list[int]:
    base = game_slug
    for suffix in [
        "-az", "-ar", "-ca", "-co", "-ct", "-de", "-fl", "-ga", "-id", "-il", "-in", "-ia",
        "-ks", "-ky", "-la", "-me", "-md", "-ma", "-mi", "-mn", "-ms", "-mo", "-mt", "-ne",
        "-nh", "-nj", "-nm", "-ny", "-nc", "-nd", "-oh", "-ok", "-or", "-pa", "-pr", "-ri",
        "-sc", "-sd", "-tn", "-tx", "-vt", "-va", "-wa", "-dc", "-wv", "-wi", "-wy"
    ]:
        if base.endswith(suffix):
            base = base[:-len(suffix)]
            break

    rule = GAME_RULES.get(base)
    if not rule:
        return raw_numbers

    return raw_numbers[: rule["main_count"]]


def validate_entry(title: str, game_slug: str, main_numbers: list[int]) -> bool:
    base = game_slug
    for suffix in [
        "-az", "-ar", "-ca", "-co", "-ct", "-de", "-fl", "-ga", "-id", "-il", "-in", "-ia",
        "-ks", "-ky", "-la", "-me", "-md", "-ma", "-mi", "-mn", "-ms", "-mo", "-mt", "-ne",
        "-nh", "-nj", "-nm", "-ny", "-nc", "-nd", "-oh", "-ok", "-or", "-pa", "-pr", "-ri",
        "-sc", "-sd", "-tn", "-tx", "-vt", "-va", "-wa", "-dc", "-wv", "-wi", "-wy"
    ]:
        if base.endswith(suffix):
            base = base[:-len(suffix)]
            break

    rule = GAME_RULES.get(base)
    if not rule:
        return len(main_numbers) > 0

    return len(main_numbers) == rule["main_count"]


def save_draw(game: Game, draw_date, draw_type: str, main_numbers: list[int], bonus_number, multiplier, source_url: str, raw_payload: dict):
    db = SessionLocal()
    try:
        existing = db.execute(
            select(Draw).where(
                Draw.game_id == game.id,
                Draw.draw_date == draw_date,
                Draw.draw_type == draw_type,
            )
        ).scalar_one_or_none()

        if existing:
            existing.main_numbers = main_numbers
            existing.bonus_number = bonus_number
            existing.multiplier = multiplier
            existing.source_url = source_url
            if hasattr(existing, "raw_payload"):
                existing.raw_payload = raw_payload
            if hasattr(existing, "source_provider"):
                existing.source_provider = "Lottery Post"
            if hasattr(existing, "verification_status"):
                existing.verification_status = "verified"
            if hasattr(existing, "confidence_score"):
                existing.confidence_score = 90
            if hasattr(existing, "needs_review"):
                existing.needs_review = False
            db.commit()
            return "updated"

        row = Draw(
            game_id=game.id,
            draw_date=draw_date,
            draw_type=draw_type,
            draw_time=None,
            main_numbers=main_numbers,
            bonus_number=bonus_number,
            multiplier=multiplier,
            jackpot=None,
            cash_payout=None,
            secondary_draws=None,
            notes="Scraped from Lottery Post DOM sections",
            source_url=source_url,
        )

        if hasattr(row, "raw_payload"):
            row.raw_payload = raw_payload
        if hasattr(row, "source_provider"):
            row.source_provider = "Lottery Post"
        if hasattr(row, "verification_status"):
            row.verification_status = "verified"
        if hasattr(row, "confidence_score"):
            row.confidence_score = 90
        if hasattr(row, "needs_review"):
            row.needs_review = False

        db.add(row)
        db.commit()
        return "created"
    finally:
        db.close()


def scrape_state(page, state: dict, games_by_slug: dict[str, Game]):
    page.goto(state["source_url"], wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)

    sections = page.locator("section")
    results = []

    for i in range(sections.count()):
        section = sections.nth(i)

        if section.locator("h2").count() == 0:
            continue

        try:
            title = clean(section.locator("h2").first.inner_text())
        except Exception:
            continue

        if not title:
            continue

        if "next drawing" in title.lower():
            continue

        draw_date = None
        if section.locator("time").count() > 0:
            try:
                draw_date = parse_date(section.locator("time").first.inner_text())
            except Exception:
                pass

        if not draw_date:
            continue

        game_slug = resolve_game_slug(title, state["slug"])
        if not game_slug:
            continue

        game = games_by_slug.get(game_slug.lower())

        if not game and game_slug == "jersey-cash-5-nj":
            game = games_by_slug.get("jersey-cash-5")

        if not game:
            results.append({
                "status": "unmatched",
                "title": title,
                "resolved_slug": game_slug,
                "state": state["slug"],
            })
            continue

        raw_numbers = extract_all_li_numbers(section)
        bonus_number, multiplier, debug_text = extract_text_extras(section)
        main_numbers = split_main_numbers(title, game.slug, raw_numbers)
        draw_type = infer_draw_type(title)

        ok = validate_entry(title, game.slug, main_numbers)

        payload = {
            "title": title,
            "state_slug": state["slug"],
            "raw_numbers": raw_numbers,
            "main_numbers": main_numbers,
            "bonus_number": bonus_number,
            "multiplier": multiplier,
            "debug_text": debug_text[:2000],
        }

        if not ok:
            results.append({
                "status": "invalid",
                "title": title,
                "resolved_slug": game.slug,
                "draw_date": str(draw_date),
                "payload": payload,
            })
            continue

        action = save_draw(
            game=game,
            draw_date=draw_date,
            draw_type=draw_type,
            main_numbers=main_numbers,
            bonus_number=bonus_number,
            multiplier=multiplier,
            source_url=state["source_url"],
            raw_payload=payload,
        )

        results.append({
            "status": action,
            "title": title,
            "resolved_slug": game.slug,
            "draw_date": str(draw_date),
            "main_numbers": main_numbers,
            "bonus_number": bonus_number,
            "multiplier": multiplier,
        })

    return results


def main():
    states = get_states()
    games_by_slug = get_games()

    created = 0
    updated = 0
    unmatched = 0
    invalid = 0
    report = []

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
        page = browser.new_page()

        for state in states:
            print("\n" + "=" * 90)
            print(f"STATE: {state['name']} ({state['slug']})")
            print(f"URL: {state['source_url']}")
            print("=" * 90)

            try:
                rows = scrape_state(page, state, games_by_slug)
            except Exception as e:
                print(f"ERROR: {e}")
                report.append({
                    "state": state["slug"],
                    "state_name": state["name"],
                    "error": str(e),
                    "rows": [],
                })
                continue

            for row in rows:
                status = row["status"]
                if status == "created":
                    created += 1
                    print(f"CREATED: {row['title']} -> {row['resolved_slug']} -> {row['main_numbers']}")
                elif status == "updated":
                    updated += 1
                    print(f"UPDATED: {row['title']} -> {row['resolved_slug']} -> {row['main_numbers']}")
                elif status == "unmatched":
                    unmatched += 1
                    print(f"UNMATCHED: {row['title']} -> {row['resolved_slug']}")
                elif status == "invalid":
                    invalid += 1
                    print(f"INVALID: {row['title']} -> {row['resolved_slug']}")

            report.append({
                "state": state["slug"],
                "state_name": state["name"],
                "rows": rows,
            })

        browser.close()

    with open("all_states_dom_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\nSUMMARY")
    print("=" * 90)
    print(f"Created: {created}")
    print(f"Updated: {updated}")
    print(f"Unmatched: {unmatched}")
    print(f"Invalid: {invalid}")
    print("Report saved: all_states_dom_report.json")


if __name__ == "__main__":
    main()