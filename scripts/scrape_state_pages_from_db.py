import json
import re
from datetime import datetime

from playwright.sync_api import sync_playwright
from sqlalchemy import text

from app.database import SessionLocal

DATE_PATTERN = r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"
HEADER_STOP_MARKERS = {
    "raffle results and special draws",
    "latest lottery news",
    "drawing schedule",
}


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_display_date(value: str):
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


def parse_latest_blocks(lines: list[str]):
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

    scope = lines[start_idx:stop_idx]
    blocks = []

    i = 0
    while i < len(scope):
        line = scope[i]
        next_line = scope[i + 1] if i + 1 < len(scope) else ""

        if re.fullmatch(DATE_PATTERN, next_line):
            title = line
            date_line = next_line
            i += 2

            payload = []
            while i < len(scope):
                current = scope[i]
                current_next = scope[i + 1] if i + 1 < len(scope) else ""

                if re.fullmatch(DATE_PATTERN, current_next):
                    break
                if current.lower() in HEADER_STOP_MARKERS:
                    break

                payload.append(current)
                i += 1

            blocks.append({
                "title": title,
                "date_line": date_line,
                "payload": payload,
            })
            continue

        i += 1

    return blocks


def extract_numbers_from_payload(payload_lines: list[str]):
    joined = " ".join(payload_lines)
    numbers = [int(x) for x in re.findall(r"\b\d{1,2}\b", joined)]
    return numbers


def build_preview_record(state: dict, block: dict):
    numbers = extract_numbers_from_payload(block["payload"])

    try:
        draw_date = str(parse_display_date(block["date_line"]))
    except Exception:
        draw_date = block["date_line"]

    return {
        "state_name": state["name"],
        "state_slug": state["slug"],
        "state_url": state["source_url"],
        "title": block["title"],
        "draw_date": draw_date,
        "numbers_preview": numbers[:12],
        "payload_preview": block["payload"][:12],
    }


def main():
    states = get_active_states()

    if not states:
        print("No hay estados activos en la tabla states.")
        return

    report = []

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=50)
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
                blocks = parse_latest_blocks(lines)
            except Exception as e:
                print(f"ERROR: no se pudo abrir la página. Detalle: {e}")
                report.append({
                    "state_name": state["name"],
                    "state_slug": state["slug"],
                    "state_url": state["source_url"],
                    "error": str(e),
                    "games_found": 0,
                    "games": [],
                })
                continue

            print(f"GAMES FOUND: {len(blocks)}")

            preview_games = []
            for block in blocks:
                item = build_preview_record(state, block)
                preview_games.append(item)

                print(f"- {item['title']} | {item['draw_date']} | nums: {item['numbers_preview']}")

            report.append({
                "state_name": state["name"],
                "state_slug": state["slug"],
                "state_url": state["source_url"],
                "error": None,
                "games_found": len(blocks),
                "games": preview_games,
            })

        context.close()
        browser.close()

    output_path = "state_pages_preview_report.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\nSUMMARY")
    print("=" * 90)
    print(f"States checked: {len(report)}")
    print(f"Report saved: {output_path}")


if __name__ == "__main__":
    main()
