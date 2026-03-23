from playwright.sync_api import sync_playwright
from sqlalchemy import text

from app.database import SessionLocal

BASE_URL = "https://www.lotterypost.com/results"

# Mapa robusto nombre -> slug para USA
US_STATE_SLUGS = {
    "Arizona": "az",
    "Arkansas": "ar",
    "California": "ca",
    "Colorado": "co",
    "Connecticut": "ct",
    "Delaware": "de",
    "Florida": "fl",
    "Georgia": "ga",
    "Idaho": "id",
    "Illinois": "il",
    "Indiana": "in",
    "Iowa": "ia",
    "Kansas": "ks",
    "Kentucky": "ky",
    "Louisiana": "la",
    "Maine": "me",
    "Maryland": "md",
    "Massachusetts": "ma",
    "Michigan": "mi",
    "Minnesota": "mn",
    "Mississippi": "ms",
    "Missouri": "mo",
    "Montana": "mt",
    "Nebraska": "ne",
    "New Hampshire": "nh",
    "New Jersey": "nj",
    "New Mexico": "nm",
    "New York": "ny",
    "North Carolina": "nc",
    "North Dakota": "nd",
    "Ohio": "oh",
    "Oklahoma": "ok",
    "Oregon": "or",
    "Pennsylvania": "pa",
    "Puerto Rico": "pr",
    "Rhode Island": "ri",
    "South Carolina": "sc",
    "South Dakota": "sd",
    "Tennessee": "tn",
    "Texas": "tx",
    "Vermont": "vt",
    "Virginia": "va",
    "Washington": "wa",
    "Washington, D.C.": "dc",
    "West Virginia": "wv",
    "Wisconsin": "wi",
    "Wyoming": "wy",
}

def get_us_states_from_lotterypost():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=50)
        page = browser.new_page()
        page.goto(BASE_URL, wait_until="load", timeout=120000)
        page.wait_for_timeout(2500)

        links = page.locator("a").all()
        rows = []

        for link in links:
            try:
                text_value = (link.inner_text() or "").strip()
                href = link.get_attribute("href") or ""
            except Exception:
                continue

            if not text_value or not href:
                continue

            if text_value not in US_STATE_SLUGS:
                continue

            # Normalizar href relativo
            if href.startswith("/"):
                full_url = f"https://www.lotterypost.com{href}"
            elif href.startswith("http"):
                full_url = href
            else:
                full_url = f"{BASE_URL}/{US_STATE_SLUGS[text_value]}"

            rows.append({
                "country_code": "US",
                "name": text_value,
                "slug": US_STATE_SLUGS[text_value],
                "source_url": full_url,
                "is_active": True,
            })

        browser.close()

    # quitar duplicados por slug
    dedup = {}
    for row in rows:
        dedup[row["slug"]] = row

    return list(dedup.values())


def upsert_states(states):
    db = SessionLocal()
    created = 0
    updated = 0

    try:
        for state in states:
            existing = db.execute(
                text("""
                    SELECT id, name, source_url, is_active
                    FROM states
                    WHERE slug = :slug
                """),
                {"slug": state["slug"]}
            ).mappings().first()

            if existing:
                changed = False

                if existing["name"] != state["name"]:
                    changed = True
                if existing["source_url"] != state["source_url"]:
                    changed = True
                if existing["is_active"] != state["is_active"]:
                    changed = True

                if changed:
                    db.execute(
                        text("""
                            UPDATE states
                            SET name = :name,
                                source_url = :source_url,
                                is_active = :is_active
                            WHERE slug = :slug
                        """),
                        state
                    )
                    updated += 1
                    print(f"UPDATED: {state['name']} ({state['slug']})")
                else:
                    print(f"EXISTING: {state['name']} ({state['slug']})")

            else:
                db.execute(
                    text("""
                        INSERT INTO states (country_code, name, slug, source_url, is_active)
                        VALUES (:country_code, :name, :slug, :source_url, :is_active)
                    """),
                    state
                )
                created += 1
                print(f"CREATED: {state['name']} ({state['slug']})")

        db.commit()
        return created, updated

    finally:
        db.close()


def main():
    states = get_us_states_from_lotterypost()
    print(f"\nEstados detectados desde Lottery Post: {len(states)}\n")

    created, updated = upsert_states(states)

    print("\nSUMMARY")
    print("=" * 70)
    print(f"Detected: {len(states)}")
    print(f"Created: {created}")
    print(f"Updated: {updated}")


if __name__ == "__main__":
    main()
