import json
import re
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright
from sqlalchemy import select

from app.database import SessionLocal
from app.models import Draw, Game

BASE_URL = "https://www.lotterypost.com/results"

MULTI_STATE_GAMES = {
    "powerball": {
        "title": "Powerball",
        "source_url": BASE_URL,
        "main_count": 5,
        "main_min": 1,
        "main_max": 69,
        "bonus_min": 1,
        "bonus_max": 26,
        "has_multiplier": True,
    },
    "mega-millions": {
        "title": "Mega Millions",
        "source_url": BASE_URL,
        "main_count": 5,
        "main_min": 1,
        "main_max": 70,
        "bonus_min": 1,
        "bonus_max": 25,
        "has_multiplier": True,
    },
    "millionaire-for-life": {
        "title": "Millionaire For Life",
        "source_url": BASE_URL,
        "main_count": 5,
        "main_min": 1,
        "main_max": 60,
        "bonus_min": 1,
        "bonus_max": 4,
        "has_multiplier": False,
    },
    "lotto-america": {
        "title": "Lotto America",
        "source_url": BASE_URL,
        "main_count": 5,
        "main_min": 1,
        "main_max": 52,
        "bonus_min": 1,
        "bonus_max": 10,
        "has_multiplier": False,
    },
    "2by2": {
        "title": "2by2",
        "source_url": BASE_URL,
        "main_count": 4,
        "main_min": 1,
        "main_max": 26,
        "bonus_min": None,
        "bonus_max": None,
        "has_multiplier": False,
    },
}

DATE_PATTERN = r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"


def clean_line(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_date(value: str):
    return datetime.strptime(clean_line(value), "%A, %B %d, %Y").date()


def get_game(db, slug: str):
    return db.execute(select(Game).where(Game.slug == slug)).scalar_one_or_none()


def fetch_body_lines():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=100)
        page = browser.new_page()
        page.goto(BASE_URL, wait_until="load", timeout=120000)
        page.wait_for_timeout(4000)
        raw_text = page.locator("body").inner_text(timeout=30000)
        browser.close()

    lines = [clean_line(x) for x in raw_text.splitlines()]
    lines = [x for x in lines if x]
    return lines


def find_game_block(lines: list[str], game_title: str) -> Optional[list[str]]:
    titles = [cfg["title"] for cfg in MULTI_STATE_GAMES.values()]
    start_idx = None

    for i, line in enumerate(lines):
        if clean_line(line).lower() == game_title.lower():
            start_idx = i
            break

    if start_idx is None:
        return None

    block = []
    for i in range(start_idx, len(lines)):
        line = clean_line(lines[i])

        if i > start_idx and any(line.lower() == t.lower() for t in titles):
            break

        block.append(line)

    return block if block else None


def parse_multiplier(block_text: str):
    m = re.search(r"(Power Play|Megaplier)\s+([Xx]?\d+)", block_text, re.IGNORECASE)
    if not m:
        return None
    raw = m.group(2).upper()
    return raw if raw.startswith("X") else f"X{raw}"


def parse_draw_for_game(slug: str, lines: list[str]):
    cfg = MULTI_STATE_GAMES[slug]
    block_lines = find_game_block(lines, cfg["title"])

    if not block_lines:
        return None

    block_text = "\n".join(block_lines)

    date_match = re.search(DATE_PATTERN, block_text, re.IGNORECASE)
    if not date_match:
        return None

    draw_date = parse_date(date_match.group(0))

    date_line_index = None
    for i, line in enumerate(block_lines):
        if re.fullmatch(DATE_PATTERN, line):
            date_line_index = i
            break

    if date_line_index is None:
        return None

    after_date_lines = block_lines[date_line_index + 1 : date_line_index + 10]
    after_date_text = " ".join(after_date_lines)

    nums = [int(x) for x in re.findall(r"\b\d{1,2}\b", after_date_text)]

    needed = cfg["main_count"] + (1 if cfg["bonus_min"] is not None else 0)
    if len(nums) < needed:
        return None

    main_numbers = nums[: cfg["main_count"]]
    bonus_number = None

    if cfg["bonus_min"] is not None:
        bonus_number = str(nums[cfg["main_count"]])

    multiplier = parse_multiplier(block_text) if cfg["has_multiplier"] else None

    return {
        "draw_date": draw_date,
        "draw_type": "main",
        "main_numbers": main_numbers,
        "bonus_number": bonus_number,
        "multiplier": multiplier,
        "raw_payload": {
            "game_title": cfg["title"],
            "block_lines": block_lines,
            "numbers_found": nums,
        },
    }


def validate_draw(slug: str, data: dict):
    cfg = MULTI_STATE_GAMES[slug]
    nums = data["main_numbers"]

    if len(nums) != cfg["main_count"]:
        return False, f"count inválido: {len(nums)}"

    if len(set(nums)) != len(nums):
        return False, f"números duplicados: {nums}"

    for n in nums:
        if not (cfg["main_min"] <= n <= cfg["main_max"]):
            return False, f"fuera de rango: {n}"

    if cfg["bonus_min"] is not None:
        if data["bonus_number"] is None:
            return False, "bonus faltante"

        b = int(data["bonus_number"])
        if not (cfg["bonus_min"] <= b <= cfg["bonus_max"]):
            return False, f"bonus fuera de rango: {b}"

    return True, "ok"


def save_or_update_verified_draw(db, game: Game, source_url: str, data: dict):
    existing = db.execute(
        select(Draw).where(
            Draw.game_id == game.id,
            Draw.draw_date == data["draw_date"],
            Draw.draw_type == data["draw_type"],
        )
    ).scalar_one_or_none()

    if existing:
        existing.main_numbers = data["main_numbers"]
        existing.bonus_number = data["bonus_number"]
        existing.multiplier = data["multiplier"]
        existing.source_url = source_url
        existing.source_provider = "Lottery Post"
        existing.raw_payload = data["raw_payload"]
        existing.verification_status = "verified"
        existing.confidence_score = 95
        existing.needs_review = False
        db.commit()
        return "updated"

    row = Draw(
        game_id=game.id,
        draw_date=data["draw_date"],
        draw_type=data["draw_type"],
        draw_time=None,
        main_numbers=data["main_numbers"],
        bonus_number=data["bonus_number"],
        multiplier=data["multiplier"],
        jackpot=None,
        cash_payout=None,
        secondary_draws=None,
        notes="Verified multi-state scrape from Lottery Post isolated block",
        source_url=source_url,
        source_provider="Lottery Post",
        raw_payload=data["raw_payload"],
        verification_status="verified",
        confidence_score=95,
        needs_review=False,
    )
    db.add(row)
    db.commit()
    return "created"


def main():
    db = SessionLocal()
    lines = fetch_body_lines()

    created = 0
    updated = 0
    conflicts = 0

    print("\nVERIFIED MULTI-STATE CHECK")
    print("=" * 80)

    for slug, cfg in MULTI_STATE_GAMES.items():
        game = get_game(db, slug)
        if not game:
            print(f"{slug}: no existe en games")
            continue

        parsed = parse_draw_for_game(slug, lines)
        if not parsed:
            print(f"{slug}: no se pudo parsear")
            continue

        ok, reason = validate_draw(slug, parsed)
        if not ok:
            print(f"{slug}: CONFLICT -> {reason}")
            conflicts += 1
            continue

        status = save_or_update_verified_draw(db, game, cfg["source_url"], parsed)
        print(
            f"{slug}: {status} -> "
            f"{parsed['main_numbers']} bonus={parsed['bonus_number']} multiplier={parsed['multiplier']}"
        )

        if status == "created":
            created += 1
        else:
            updated += 1

    db.close()

    print("\nSUMMARY")
    print("=" * 80)
    print(f"Created: {created}")
    print(f"Updated: {updated}")
    print(f"Conflicts: {conflicts}")


if __name__ == "__main__":
    main()