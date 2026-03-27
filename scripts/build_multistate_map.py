import json
from pathlib import Path

from playwright.sync_api import sync_playwright
from sqlalchemy import text

from app.database import SessionLocal
from app.utils.game_normalizer import canonical_game_info

MULTI_STATE_SLUGS = {
    "powerball",
    "powerball-double-play",
    "mega-millions",
    "millionaire-for-life",
    "lotto-america",
    "2by2",
}


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


def main():
    states = get_states()
    result = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
        page = browser.new_page()

        for state in states:
            state_slug = state["slug"].lower()
            state_url = state["source_url"]
            found = set()

            print(f"\nSTATE: {state['name']} ({state_slug})")
            print(f"URL: {state_url}")

            try:
                page.goto(state_url, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(5000)

                sections = page.locator("section")
                for i in range(sections.count()):
                    section = sections.nth(i)

                    if section.locator("h2").count() == 0:
                        continue

                    try:
                        title = section.locator("h2").first.inner_text().strip()
                    except Exception:
                        continue

                    if not title:
                        continue

                    info = canonical_game_info(title, state_code=state_slug)
                    canonical_slug = info["canonical_slug"]
                    final_slug = info["final_slug"]

                    # Para multi-state el final_slug debe quedar global
                    if canonical_slug in MULTI_STATE_SLUGS or final_slug in MULTI_STATE_SLUGS:
                        found.add(canonical_slug)

                result[state_slug] = sorted(found)
                print(f"FOUND MULTI-STATE: {sorted(found)}")

            except Exception as e:
                print(f"ERROR IN {state_slug}: {e}")
                result[state_slug] = []

        browser.close()

    out_dir = Path("app/config")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "multistate_by_state.json"
    out_file.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\nDONE")
    print(f"Saved: {out_file}")


if __name__ == "__main__":
    main()