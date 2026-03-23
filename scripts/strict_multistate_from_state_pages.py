import json
import re
from datetime import datetime
from typing import Optional

from playwright.sync_api import sync_playwright
from sqlalchemy import text

from app.database import SessionLocal

TARGET_GAMES = {
    "powerball": {
        "titles": ["Powerball"],
        "main_count": 5,
        "main_min": 1,
        "main_max": 69,
        "bonus_min": 1,
        "bonus_max": 26,
        "has_multiplier": True,
    },
    "mega-millions": {
        "titles": ["Mega Millions"],
        "main_count": 5,
        "main_min": 1,
        "main_max": 70,
        "bonus_min": 1,
        "bonus_max": 25,
        "has_multiplier": True,
    },
    "millionaire-for-life": {
        "titles": ["Millionaire for Life", "Millionaire For Life"],
        "main_count": 5,
        "main_min": 1,
        "main_max": 48,
        "bonus_min": 1,
        "bonus_max": 4,
        "has_multiplier": False,
    },
    "lotto-america": {
        "titles": ["Lotto America"],
        "main_count": 5,
        "main_min": 1,
        "main_max": 52,
        "bonus_min": 1,
        "bonus_max": 10,
        "has_multiplier": False,
    },
    "2by2": {
        "titles": ["2by2"],
        "main_count": 4,
        "main_min": 1,
        "main_max": 26,
        "bonus_min": None,
        "bonus_max": None,
        "has_multiplier": False,
    },
}

DATE_PATTERN = r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"
HEADER_STOP_MARKERS = {
    "raffle results and special draws",
    "latest lottery news",
    "drawing schedule",
}


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_date(value: str):
    return datetime.strptime(clean_text(value), "%A, %B %d, %Y").date()


def get_active_states():
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


def fetch_state_page_lines(context, url: str):
    page = context.new_page()
    try:
        page.goto(url, wait_until="load", timeout=120000)
        page.wait_for_timeout(3000)
        raw_text = page.locator("body").inner_text(timeout=30000)
        lines = [clean_text(x) for x in raw_text.splitlines()]
        lines = [x for x in lines if x]
        return lines
    finally:
        page.close()


def crop_latest_results(lines: list[str]) -> list[str]:
    start_idx = 0
    for i, line in enumerate(lines):
        if line.lower() == "latest results":
            start_idx = i + 1
            break

    stop_idx = len(lines)
    for i in range(start_idx, len(lines)):
        low = lines[i].lower()
        if any(marker in low for marker in HEADER_STOP_MARKERS):
            stop_idx = i
            break

    return lines[start_idx:stop_idx]


def find_title_index(lines: list[str], titles: list[str]) -> Optional[int]:
    low_titles = {t.lower() for t in titles}
    for i, line in enumerate(lines):
        if line.lower() in low_titles:
            return i
    return None


def find_game_block(lines: list[str], titles: list[str]) -> Optional[list[str]]:
    latest = crop_latest_results(lines)
    idx = find_title_index(latest, titles)
    if idx is None:
        return None

    all_titles = []
    for cfg in TARGET_GAMES.values():
        all_titles.extend(cfg["titles"])
    all_titles = {t.lower() for t in all_titles}

    block = []
    for pos in range(idx, len(latest)):
        line = latest[pos]
        if pos > idx and line.lower() in all_titles:
            break
        block.append(line)

    return block if block else None


def find_date_index(block_lines: list[str]) -> Optional[int]:
    for i, line in enumerate(block_lines):
        if re.fullmatch(DATE_PATTERN, line):
            return i
    return None


def parse_multiplier_from_lines(block_lines: list[str]) -> Optional[str]:
    joined = " ".join(block_lines)
    m = re.search(r"(Power Play|Megaplier)\s+([Xx]?\d+)", joined, re.IGNORECASE)
    if not m:
        return None
    raw = m.group(2).upper()
    return raw if raw.startswith("X") else f"X{raw}"


def extract_candidate_numbers(block_lines: list[str], date_idx: int) -> list[int]:
    """
    Regla estricta:
    - Solo mirar pocas líneas después de la fecha
    - Ignorar líneas con AM/PM o etiquetas obvias
    - Extraer números aislados
    """
    candidates = []
    for line in block_lines[date_idx + 1 : date_idx + 8]:
        low = line.lower()

        if "power play" in low or "megaplier" in low:
            continue
        if "double play" in low:
            continue
        if "am" in low or "pm" in low:
            continue
        if "next" in low or "jackpot" in low:
            continue

        hits = re.findall(r"\b\d{1,2}\b", line)
        if hits:
            candidates.extend(int(x) for x in hits)

    return candidates


def validate_numbers(cfg: dict, main_numbers: list[int], bonus_number: Optional[str]) -> tuple[bool, str]:
    if len(main_numbers) != cfg["main_count"]:
        return False, f"count inválido: {len(main_numbers)}"

    if len(set(main_numbers)) != len(main_numbers):
        return False, f"duplicados en main_numbers: {main_numbers}"

    for n in main_numbers:
        if not (cfg["main_min"] <= n <= cfg["main_max"]):
            return False, f"main fuera de rango: {n}"

    if cfg["bonus_min"] is not None:
        if bonus_number is None:
            return False, "bonus faltante"
        b = int(bonus_number)
        if not (cfg["bonus_min"] <= b <= cfg["bonus_max"]):
            return False, f"bonus fuera de rango: {b}"

    return True, "ok"


def parse_game_from_state_lines(state: dict, lines: list[str], game_slug: str) -> Optional[dict]:
    cfg = TARGET_GAMES[game_slug]
    block = find_game_block(lines, cfg["titles"])
    if not block:
        return None

    date_idx = find_date_index(block)
    if date_idx is None:
        return {
            "state": state["slug"],
            "game": game_slug,
            "status": "NO_DATE",
            "block_preview": block[:20],
        }

    draw_date = parse_date(block[date_idx])
    candidates = extract_candidate_numbers(block, date_idx)

    need = cfg["main_count"] + (1 if cfg["bonus_min"] is not None else 0)
    if len(candidates) < need:
        return {
            "state": state["slug"],
            "game": game_slug,
            "status": "NOT_ENOUGH_NUMBERS",
            "draw_date": str(draw_date),
            "candidates": candidates,
            "block_preview": block[:20],
        }

    main_numbers = candidates[:cfg["main_count"]]
    bonus_number = None
    if cfg["bonus_min"] is not None:
        bonus_number = str(candidates[cfg["main_count"]])

    multiplier = parse_multiplier_from_lines(block) if cfg["has_multiplier"] else None

    ok, reason = validate_numbers(cfg, main_numbers, bonus_number)

    return {
        "state": state["slug"],
        "state_name": state["name"],
        "game": game_slug,
        "status": "OK" if ok else "INVALID",
        "reason": reason,
        "draw_date": str(draw_date),
        "main_numbers": main_numbers,
        "bonus_number": bonus_number,
        "multiplier": multiplier,
        "candidates": candidates,
        "block_preview": block[:20],
        "source_url": state["source_url"],
    }


def main():
    states = get_active_states()
    if not states:
        print("No hay estados activos en la tabla states.")
        return

    report = []
    ok_count = 0
    bad_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=40)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        )

        for state in states:
            print("\n" + "=" * 90)
            print(f"STATE: {state['name']} ({state['slug']})")
            print(f"URL: {state['source_url']}")
            print("=" * 90)

            try:
                lines = fetch_state_page_lines(context, state["source_url"])
            except Exception as e:
                print(f"ERROR ABRIENDO ESTADO: {e}")
                report.append({
                    "state": state["slug"],
                    "state_name": state["name"],
                    "status": "STATE_ERROR",
                    "error": str(e),
                })
                bad_count += 1
                continue

            for game_slug in TARGET_GAMES:
                result = parse_game_from_state_lines(state, lines, game_slug)

                if result is None:
                    continue

                report.append(result)

                if result["status"] == "OK":
                    ok_count += 1
                    print(
                        f"{game_slug}: OK | {result['draw_date']} | "
                        f"{result['main_numbers']} | bonus={result.get('bonus_number')} | mult={result.get('multiplier')}"
                    )
                else:
                    bad_count += 1
                    print(
                        f"{game_slug}: {result['status']} | "
                        f"{result.get('reason')} | candidates={result.get('candidates')}"
                    )

        context.close()
        browser.close()

    output_path = "strict_multistate_state_pages_report.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\nSUMMARY")
    print("=" * 90)
    print(f"States checked: {len(states)}")
    print(f"OK results: {ok_count}")
    print(f"Need review: {bad_count}")
    print(f"Report saved: {output_path}")


if __name__ == "__main__":
    main()
