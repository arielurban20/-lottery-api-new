import re
from datetime import datetime

from playwright.sync_api import sync_playwright
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Game, GameSource, SourceProvider

BASE_URL = "https://www.lotterypost.com/results"

STATE_CODES = [
    "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "id", "il", "in", "ia",
    "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne",
    "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri",
    "sc", "sd", "tn", "tx", "vt", "va", "wa", "dc", "wv", "wi", "wy",
]

DRAW_LABELS = {
    "morning", "matinee", "afternoon", "day", "midday", "evening", "night", "late night"
}

WEEKDAY_PATTERN = r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"

MULTI_STATE_TITLE_TO_SLUG = {
    "powerball": "powerball",
    "mega millions": "mega-millions",
    "millionaire for life": "millionaire-for-life",
    "lucky for life": "lucky-for-life",
    "cash4life": "cash4life",
    "cash 4 life": "cash4life",
    "lotto america": "lotto-america",
    "2by2": "2by2",
}

TITLE_OVERRIDES = {
    "cash five": "cash-5",
    "pick 3": "pick-3",
    "pick 4": "pick-4",
    "pick 5": "pick-5",
    "play 3": "play-3",
    "play 4": "play-4",
    "play 5": "play-5",
    "daily 3": "daily-3",
    "daily 4": "daily-4",
    "cash 3": "cash-3",
    "cash 4": "cash-4",
    "numbers": "numbers",
    "win 4": "win-4",
    "take 5": "take-5",
    "cash pop": "cash-pop",
    "lotto": "lotto",
    "cash 5": "cash-5",
    "cash 5 with ezmatch": "cash-5",
    "fantasy 5": "fantasy-5",
    "pick 6": "pick-6",
    "pick 2": "pick-2",
    "georgia five": "georgia-five",
    "the pick": "the-pick",
}

STATE_NAME = {
    "az": "Arizona", "ar": "Arkansas", "ca": "California", "co": "Colorado", "ct": "Connecticut",
    "de": "Delaware", "fl": "Florida", "ga": "Georgia", "id": "Idaho", "il": "Illinois",
    "in": "Indiana", "ia": "Iowa", "ks": "Kansas", "ky": "Kentucky", "la": "Louisiana",
    "me": "Maine", "md": "Maryland", "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota",
    "ms": "Mississippi", "mo": "Missouri", "mt": "Montana", "ne": "Nebraska", "nh": "New Hampshire",
    "nj": "New Jersey", "nm": "New Mexico", "ny": "New York", "nc": "North Carolina",
    "nd": "North Dakota", "oh": "Ohio", "ok": "Oklahoma", "or": "Oregon", "pa": "Pennsylvania",
    "pr": "Puerto Rico", "ri": "Rhode Island", "sc": "South Carolina", "sd": "South Dakota",
    "tn": "Tennessee", "tx": "Texas", "vt": "Vermont", "va": "Virginia", "wa": "Washington",
    "dc": "District of Columbia", "wv": "West Virginia", "wi": "Wisconsin", "wy": "Wyoming",
}


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def slugify(value: str) -> str:
    value = value.lower()
    value = value.replace("&", " and ")
    value = value.replace("+plus", " plus")
    value = value.replace("+", " plus ")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def strip_draw_label_from_title(title: str):
    title_lower = clean_text(title).lower()
    for label in sorted(DRAW_LABELS, key=len, reverse=True):
        if title_lower.endswith(label):
            return clean_text(title[: -len(label)])
    return clean_text(title)


def normalize_base_slug(title: str):
    title = clean_text(title).lower()
    if title in MULTI_STATE_TITLE_TO_SLUG:
        return MULTI_STATE_TITLE_TO_SLUG[title]
    if title in TITLE_OVERRIDES:
        return TITLE_OVERRIDES[title]
    return slugify(title)


def infer_has_multiple_daily_draws(title: str):
    title_lower = clean_text(title).lower()
    return any(title_lower.endswith(label) for label in DRAW_LABELS)


def infer_metadata(base_title: str, state_code: str):
    name = clean_text(base_title)
    normalized_slug = normalize_base_slug(name)

    is_multi_state = normalized_slug in {
        "powerball", "mega-millions", "millionaire-for-life",
        "lucky-for-life", "cash4life", "lotto-america", "2by2"
    }

    slug = normalized_slug if is_multi_state else f"{normalized_slug}-{state_code}"

    meta = {
        "name": name.title() if name.islower() else name,
        "slug": slug,
        "game_type": "multi-state" if is_multi_state else "state-specific",
        "is_multi_state": is_multi_state,
        "has_bonus_ball": False,
        "has_multiplier": False,
        "has_secondary_draws": False,
        "has_multiple_daily_draws": False,
        "main_ball_count": None,
        "main_ball_min": None,
        "main_ball_max": None,
        "bonus_ball_min": None,
        "bonus_ball_max": None,
    }

    low = normalized_slug

    if low in {"powerball", "mega-millions", "millionaire-for-life", "cash4life", "lotto-america"}:
        meta["has_bonus_ball"] = True

    if low in {"powerball", "mega-millions"}:
        meta["has_multiplier"] = True

    if low in {"powerball", "pick-6"}:
        meta["has_secondary_draws"] = True

    if any(x in low for x in ["pick-2", "pick-3", "pick-4", "pick-5", "play-3", "play-4", "play-5", "daily-3", "daily-4", "cash-3", "cash-4", "numbers", "win-4", "take-5", "cash-pop"]):
        meta["has_multiple_daily_draws"] = True

    return meta


def parse_latest_titles(text_value: str):
    lines = [clean_text(x) for x in text_value.splitlines()]
    lines = [x for x in lines if x]

    start_idx = 0
    for i, line in enumerate(lines):
        if line.lower() == "latest results":
            start_idx = i + 1
            break

    end_markers = {"raffle results and special draws", "latest lottery news"}
    stop_idx = len(lines)
    for i in range(start_idx, len(lines)):
        low = lines[i].lower()
        if any(marker in low for marker in end_markers) or "drawing schedule" in low:
            stop_idx = i
            break

    scope = lines[start_idx:stop_idx]
    titles = []

    i = 0
    while i < len(scope) - 1:
        line = scope[i]
        next_line = scope[i + 1]

        if re.fullmatch(WEEKDAY_PATTERN, next_line):
            titles.append(line)
            i += 2
            continue

        i += 1

    return titles


def ensure_provider(db: Session, name: str, base_url: str):
    provider = db.execute(select(SourceProvider).where(SourceProvider.name == name)).scalar_one_or_none()
    if provider:
        return provider

    provider = SourceProvider(name=name, base_url=base_url, is_active=True)
    db.add(provider)
    db.commit()
    db.refresh(provider)
    return provider


def ensure_game(db: Session, meta: dict, state_code: str, source_url: str):
    game = db.execute(select(Game).where(Game.slug == meta["slug"])).scalar_one_or_none()
    if game:
        changed = False
        for field in [
            "name", "game_type", "is_multi_state", "has_bonus_ball", "has_multiplier",
            "has_secondary_draws", "has_multiple_daily_draws"
        ]:
            new_val = meta[field]
            if getattr(game, field) != new_val:
                setattr(game, field, new_val)
                changed = True

        if game.source_result_url != source_url:
            game.source_result_url = source_url
            changed = True

        if changed:
            db.commit()
        return game, False

    game = Game(
        state_id=None,
        name=meta["name"],
        slug=meta["slug"],
        game_type=meta["game_type"],
        is_multi_state=meta["is_multi_state"],
        has_bonus_ball=meta["has_bonus_ball"],
        has_multiplier=meta["has_multiplier"],
        has_secondary_draws=meta["has_secondary_draws"],
        has_multiple_daily_draws=meta["has_multiple_daily_draws"],
        main_ball_count=meta["main_ball_count"],
        main_ball_min=meta["main_ball_min"],
        main_ball_max=meta["main_ball_max"],
        bonus_ball_min=meta["bonus_ball_min"],
        bonus_ball_max=meta["bonus_ball_max"],
        source_result_url=source_url,
        supports_history=True,
        supports_stats=False,
        is_active=True,
    )
    db.add(game)
    db.commit()
    db.refresh(game)
    return game, True


def ensure_game_source(db: Session, game_id: int, provider_id: int, source_url: str):
    existing = db.execute(
        select(GameSource).where(
            GameSource.game_id == game_id,
            GameSource.provider_id == provider_id,
            GameSource.source_role == "results",
            GameSource.source_url == source_url,
        )
    ).scalar_one_or_none()

    if existing:
        return False

    row = GameSource(
        game_id=game_id,
        provider_id=provider_id,
        source_url=source_url,
        source_role="results",
        priority=1,
        is_active=True,
    )
    db.add(row)
    db.commit()
    return True


def fetch_page_text(context, url: str):
    page = context.new_page()
    try:
        page.goto(url, wait_until="load", timeout=120000)
        page.wait_for_timeout(2500)
        return page.locator("body").inner_text(timeout=20000)
    finally:
        page.close()


def main():
    db = SessionLocal()
    provider = ensure_provider(db, "Lottery Post", BASE_URL)

    created_games = 0
    existing_games = 0
    created_sources = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=100)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        )

        for state_code in STATE_CODES:
            state_url = f"{BASE_URL}/{state_code}"
            state_name = STATE_NAME.get(state_code, state_code.upper())
            print(f"\n{'=' * 90}\nSEED STATE: {state_name} ({state_code.upper()})\nURL: {state_url}\n{'=' * 90}")

            try:
                text_value = fetch_page_text(context, state_url)
                titles = parse_latest_titles(text_value)
            except Exception as e:
                print(f"{state_code.upper()}: no se pudo abrir la página. Detalle: {e}")
                continue

            seen = set()
            cleaned_titles = []
            for title in titles:
                base_title = strip_draw_label_from_title(title)
                key = clean_text(base_title).lower()
                if key not in seen:
                    seen.add(key)
                    cleaned_titles.append(base_title)

            print(f"Titulos detectados: {len(cleaned_titles)}")

            for base_title in cleaned_titles:
                meta = infer_metadata(base_title, state_code)
                game, was_created = ensure_game(db, meta, state_code, state_url)

                if was_created:
                    created_games += 1
                    print(f"CREATED GAME: {game.slug}")
                else:
                    existing_games += 1
                    print(f"EXISTING GAME: {game.slug}")

                if ensure_game_source(db, game.id, provider.id, state_url):
                    created_sources += 1
                    print(f"  + SOURCE: {state_url}")

        context.close()
        browser.close()

    db.close()

    print("\nSUMMARY")
    print("=" * 90)
    print(f"Created games: {created_games}")
    print(f"Existing games: {existing_games}")
    print(f"Created sources: {created_sources}")


if __name__ == "__main__":
    main()
