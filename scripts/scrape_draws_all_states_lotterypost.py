import json
import re
from datetime import datetime

from playwright.sync_api import sync_playwright
from sqlalchemy import select

from app.database import SessionLocal
from app.models import Draw, Game, GameSource

BASE_URL = "https://www.lotterypost.com/results"

STATE_CODES = [
    "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "id", "il", "in", "ia",
    "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne",
    "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri",
    "sc", "sd", "tn", "tx", "vt", "va", "wa", "dc", "wv", "wi", "wy",
]

DRAW_LABELS = {
    "morning", "matinee", "afternoon", "day", "midday", "evening", "night", "late night",
    "daytime", "early bird", "brunch", "suppertime", "prime time", "drive time",
    "night owl", "lunch rush", "morning buzz", "clock out cash", "primetime pop",
    "midnight money", "lunch break", "coffee break", "rush hour", "after hours",
    "1pm", "4pm", "7pm", "10pm", "9am", "6pm", "11pm", "mid", "eve",
    "día", "noche", "d-a"
}

WEEKDAY_PATTERN = r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}"
HEADER_STOP_MARKERS = {
    "raffle results and special draws",
    "latest lottery news",
    "drawing schedule",
}

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
    "cash 5": "cash-5",
    "pick 2": "pick-2",
    "pick 3": "pick-3",
    "pick 4": "pick-4",
    "pick 5": "pick-5",
    "pick 6": "pick-6",
    "play 3": "play-3",
    "play 4": "play-4",
    "play 5": "play-5",
    "daily 3": "daily-3",
    "daily 4": "daily-4",
    "cash 3": "cash-3",
    "cash 4": "cash-4",
    "numbers": "numbers",
    "numbers game": "numbers-game",
    "win 4": "win-4",
    "take 5": "take-5",
    "cash pop": "cash-pop",
    "fantasy 5": "fantasy-5",
    "pick 10": "pick-10",
    "lotto": "lotto",
    "pega 2": "pega-2",
    "pega 3": "pega-3",
    "pega 4": "pega-4",
    "all or nothing": "all-or-nothing",
}

BONUS_LABELS = {
    "fireball": "bonus_number",
    "mega ball": "bonus_number",
    "powerball": "bonus_number",
    "millionaire ball": "bonus_number",
    "cash ball": "bonus_number",
    "bullseye": "bonus_number",
    "extra": "bonus_number",
    "xtra": "multiplier",
    "power play": "multiplier",
    "megaplier": "multiplier",
    "multiplier": "multiplier",
    "multiplier*": "multiplier",
    "megaball": "bonus_number",
}

DIRECT_DOUBLE_PLAY_TITLES = {"powerball double play"}

STATE_SUFFIX_RE = re.compile(
    r"-(az|ar|ca|co|ct|de|fl|ga|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|pr|ri|sc|sd|tn|tx|vt|va|wa|dc|wv|wi|wy)$"
)

CASH_POP_NAMED_BY_STATE = {
    "md": {"9am", "1pm", "6pm", "11pm"},
    "nc": {"morning-buzz", "lunch-rush", "clock-out-cash", "primetime-pop", "midnight-money"},
    "pa": {"morning-buzz", "lunch-break", "prime-time", "night-owl"},
    "va": {"coffee-break", "lunch-break", "rush-hour", "prime-time", "after-hours"},
    "ga": {"early-bird", "drive-time", "primetime", "night-owl"},
    "me": {"early-bird", "brunch", "suppertime", "night-owl"},
    "mo": {"early-bird", "late", "prime-time", "night-owl"},
}

PICK4_NAMED_BY_STATE = {
    "or": {"1pm", "4pm", "7pm", "10pm"},
}

ALL_OR_NOTHING_NAMED_BY_STATE = {
    "wi": {"mid", "eve"},
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
            base = clean_text(title[: -len(label)])
            if base:
                return base
    return clean_text(title)


def normalize_base_slug(title: str):
    title = clean_text(title).lower()
    if title in MULTI_STATE_TITLE_TO_SLUG:
        return MULTI_STATE_TITLE_TO_SLUG[title]
    if title in TITLE_OVERRIDES:
        return TITLE_OVERRIDES[title]
    return slugify(title)


def parse_display_date(value: str):
    return datetime.strptime(clean_text(value), "%A, %B %d, %Y").date()


def choose_source_url(game: Game, source_rows: list[GameSource]):
    if source_rows:
        rows = sorted(source_rows, key=lambda x: ((x.priority if x.priority is not None else 999999), x.id))
        return rows[0].source_url
    return game.source_result_url or BASE_URL


def fetch_page_text(context, url: str):
    page = context.new_page()
    try:
        page.goto(url, wait_until="load", timeout=120000)
        page.wait_for_timeout(2500)
        return page.locator("body").inner_text(timeout=30000)
    finally:
        page.close()


def load_games_and_sources():
    db = SessionLocal()
    try:
        games = db.execute(select(Game).where(Game.is_active == True)).scalars().all()
        sources = db.execute(
            select(GameSource).where(GameSource.is_active == True, GameSource.source_role == "results")
        ).scalars().all()

        by_slug = {g.slug.lower(): g for g in games}
        source_by_game = {}
        for s in sources:
            source_by_game.setdefault(s.game_id, []).append(s)

        return by_slug, source_by_game
    finally:
        db.close()


def parse_latest_blocks(text_value: str):
    lines = [clean_text(x) for x in text_value.splitlines()]
    lines = [x for x in lines if x]

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

        if re.fullmatch(WEEKDAY_PATTERN, next_line):
            title = line
            date_line = next_line
            i += 2

            payload = []
            while i < len(scope):
                current = scope[i]
                current_next = scope[i + 1] if i + 1 < len(scope) else ""

                if re.fullmatch(WEEKDAY_PATTERN, current_next):
                    break
                if current.lower() in HEADER_STOP_MARKERS:
                    break

                payload.append(current)
                i += 1

            blocks.append({
                "title": title,
                "base_title": strip_draw_label_from_title(title),
                "date_line": date_line,
                "payload": payload,
            })
            continue

        i += 1

    return blocks


def block_to_slug_candidates(block: dict, state_code: str):
    title_lower = clean_text(block["title"]).lower()
    base_title = clean_text(block["base_title"])
    base_slug = normalize_base_slug(base_title)

    if title_lower in DIRECT_DOUBLE_PLAY_TITLES:
        return [f"{slugify(title_lower)}-{state_code.lower()}"]

    if base_slug in {
        "powerball", "mega-millions", "millionaire-for-life",
        "lucky-for-life", "cash4life", "lotto-america", "2by2"
    }:
        return [base_slug]

    candidates = [f"{base_slug}-{state_code.lower()}"]

    draw_type_hint = infer_draw_type_from_title(block["title"])
    if base_slug == "cash-pop" and draw_type_hint in CASH_POP_NAMED_BY_STATE.get(state_code.lower(), set()):
        candidates.insert(0, f"cash-pop-{draw_type_hint}-{state_code.lower()}")

    if base_slug == "pick-4" and draw_type_hint in PICK4_NAMED_BY_STATE.get(state_code.lower(), set()):
        candidates.insert(0, f"pick-4-{draw_type_hint}-{state_code.lower()}")

    if base_slug == "all-or-nothing" and draw_type_hint in ALL_OR_NOTHING_NAMED_BY_STATE.get(state_code.lower(), set()):
        candidates.insert(0, f"all-or-nothing-{draw_type_hint}-{state_code.lower()}")

    if base_slug in {"pega-2", "pega-3", "pega-4"} and draw_type_hint in {"dia", "noche"}:
        candidates.insert(0, f"{base_slug}-{draw_type_hint}-{state_code.lower()}")
        candidates.insert(1, f"{base_slug}-d-a-{state_code.lower()}" if draw_type_hint == "dia" else f"{base_slug}-noche-{state_code.lower()}")

    return list(dict.fromkeys(candidates))


def infer_draw_type_from_title(title: str):
    low = clean_text(title).lower()
    replacements = {
        "prime time": "prime-time",
        "drive time": "drive-time",
        "late night": "late-night",
        "night owl": "night-owl",
        "early bird": "early-bird",
        "clock out cash": "clock-out-cash",
        "morning buzz": "morning-buzz",
        "lunch rush": "lunch-rush",
        "primetime pop": "primetime-pop",
        "midnight money": "midnight-money",
        "lunch break": "lunch-break",
        "coffee break": "coffee-break",
        "rush hour": "rush-hour",
        "after hours": "after-hours",
    }
    for k, v in replacements.items():
        low = low.replace(k, v)

    if "día" in low:
        return "dia"
    if "noche" in low:
        return "noche"

    for label in [
        "early-bird", "brunch", "suppertime", "drive-time", "primetime-pop", "prime-time",
        "primetime", "night-owl", "morning-buzz", "lunch-rush", "clock-out-cash",
        "midnight-money", "lunch-break", "coffee-break", "rush-hour", "after-hours",
        "late-night", "daytime", "midday", "evening", "morning", "matinee", "afternoon",
        "night", "day", "9am", "1pm", "4pm", "6pm", "7pm", "10pm", "11pm", "mid", "eve", "late"
    ]:
        if low.endswith(label):
            return label

    return "main"


def infer_draw_type_from_slug(game_slug: str):
    low = game_slug.lower()

    if low.startswith("cash-pop-"):
        suffix = low.replace("cash-pop-", "", 1)
        suffix = STATE_SUFFIX_RE.sub("", suffix)
        if suffix in {"fl", "ga", "in", "ms", "sc", "wa", "me", "md", "mo", "nc", "pa", "va"}:
            return "main"
        return suffix

    base = STATE_SUFFIX_RE.sub("", low)

    for marker in [
        "-daytime", "-mid", "-eve", "-morning", "-matinee", "-afternoon", "-day", "-midday", "-evening",
        "-night", "-late-night", "-early-bird", "-brunch", "-drive-time", "-primetime", "-prime-time",
        "-night-owl", "-suppertime", "-morning-buzz", "-lunch-rush", "-clock-out-cash", "-primetime-pop",
        "-midnight-money", "-lunch-break", "-coffee-break", "-rush-hour", "-after-hours", "-1pm", "-4pm",
        "-7pm", "-10pm", "-9am", "-6pm", "-11pm", "-late", "-dia", "-noche", "-d-a"
    ]:
        if base.endswith(marker):
            return marker[1:]

    return "main"


def parse_payload_to_numbers(block: dict, game_slug: str):
    title_low = clean_text(block["title"]).lower()
    payload_lines = block["payload"]
    combined = clean_text(" ".join(payload_lines))

    draw_date = parse_display_date(block["date_line"])
    draw_time = None
    bonus_number = None
    multiplier = None
    jackpot = None
    cash_payout = None
    secondary_draws = None

    numbers = []
    i = 0
    while i < len(payload_lines):
        line = payload_lines[i]
        low = line.lower().rstrip(":")

        if low in BONUS_LABELS and i + 1 < len(payload_lines):
            nxt = payload_lines[i + 1]
            if re.fullmatch(r"[Xx]?\d{1,2}", nxt):
                field = BONUS_LABELS[low]
                if field == "bonus_number":
                    bonus_number = re.sub(r"^[Xx]", "", nxt)
                else:
                    multiplier = nxt.upper() if nxt.upper().startswith("X") else f"X{nxt}"
                i += 2
                continue

        if low in {"double play drawing", "double play"}:
            dp = []
            j = i + 1
            while j < len(payload_lines) and re.fullmatch(r"\d{1,2}", payload_lines[j]):
                dp.append(int(payload_lines[j]))
                j += 1
            if dp:
                if len(dp) == 6 and "powerball" in title_low:
                    secondary_draws = [{
                        "draw_type": "double-play",
                        "main_numbers": dp[:5],
                        "bonus_number": str(dp[5]),
                    }]
                else:
                    secondary_draws = [{
                        "draw_type": "double-play",
                        "main_numbers": dp,
                        "bonus_number": None,
                    }]
                i = j
                continue

        if re.fullmatch(r"\d{1,2}", line):
            numbers.append(int(line))

        i += 1

    if not numbers:
        numbers = [int(x) for x in re.findall(r"\b\d{1,2}\b", combined)]

    if not numbers:
        return None

    base_slug = STATE_SUFFIX_RE.sub("", game_slug.lower())
    draw_type = infer_draw_type_from_slug(game_slug)

    if title_low == "powerball double play":
        if len(numbers) >= 6:
            return {
                "draw_date": draw_date,
                "draw_type": "double-play",
                "draw_time": draw_time,
                "main_numbers": numbers[:5],
                "bonus_number": str(numbers[5]),
                "multiplier": None,
                "jackpot": jackpot,
                "cash_payout": None,
                "secondary_draws": None,
                "notes": "Scraped from Lottery Post latest state page via Playwright",
            }
        return None

    if base_slug in {"powerball", "mega-millions", "millionaire-for-life"}:
        if len(numbers) < 6:
            return None
        main_numbers = numbers[:5]
        if bonus_number is None:
            bonus_number = str(numbers[5])

    elif base_slug == "2by2":
        main_numbers = numbers[:4]

    elif any(x in base_slug for x in ["pick-2", "pega-2"]):
        main_numbers = numbers[:2]

    elif any(x in base_slug for x in ["pick-3", "play-3", "cash-3", "daily-3", "dc-3", "pega-3", "numbers"]):
        if len(numbers) >= 4:
            main_numbers = numbers[:3]
            if bonus_number is None and draw_type in {"main", "mid", "eve", "daytime", "day", "night", "dia", "noche"}:
                bonus_number = str(numbers[3])
        else:
            main_numbers = numbers[:3]

    elif any(x in base_slug for x in ["pick-4", "play-4", "cash-4", "daily-4", "win-4", "dc-4", "pega-4"]):
        if len(numbers) >= 5:
            main_numbers = numbers[:4]
            if bonus_number is None and draw_type in {"main", "mid", "eve", "daytime", "day", "night", "dia", "noche", "1pm", "4pm", "7pm", "10pm"}:
                bonus_number = str(numbers[4])
        else:
            main_numbers = numbers[:4]

    elif any(x in base_slug for x in ["pick-5", "play-5", "cash-5", "take-5", "fantasy-5", "gimme-5", "match-5", "badger-5", "north-5", "georgia-five"]):
        main_numbers = numbers[:5]

    elif "cash-pop" in base_slug:
        main_numbers = [numbers[0]]

    elif any(x in base_slug for x in ["pick-6", "lotto", "megabucks", "match-6", "lotto-47", "superlotto", "double-play", "triple-play"]):
        main_numbers = numbers[:6] if len(numbers) >= 6 else numbers

    else:
        main_numbers = numbers

    if not main_numbers:
        return None

    return {
        "draw_date": draw_date,
        "draw_type": draw_type,
        "draw_time": draw_time,
        "main_numbers": main_numbers,
        "bonus_number": bonus_number,
        "multiplier": multiplier,
        "jackpot": jackpot,
        "cash_payout": cash_payout,
        "secondary_draws": secondary_draws,
        "notes": "Scraped from Lottery Post latest state page via Playwright",
    }


def upsert_draw(game: Game, source_url: str, data: dict):
    db = SessionLocal()
    try:
        existing = db.execute(
            select(Draw).where(
                Draw.game_id == game.id,
                Draw.draw_date == data["draw_date"],
                Draw.draw_type == data["draw_type"],
            )
        ).scalar_one_or_none()

        if existing:
            print(f"{game.slug}/{data['draw_type']}: ese draw ya existe. No se insertó duplicado.")
            return "existing"

        row = Draw(
            game_id=game.id,
            draw_date=data["draw_date"],
            draw_type=data["draw_type"],
            draw_time=data["draw_time"],
            main_numbers=data["main_numbers"],
            bonus_number=data["bonus_number"],
            multiplier=data["multiplier"],
            jackpot=data["jackpot"],
            cash_payout=data["cash_payout"],
            secondary_draws=data["secondary_draws"],
            notes=data["notes"],
            source_url=source_url,
        )
        db.add(row)
        db.commit()
        print(f"{game.slug}/{data['draw_type']}: draw guardado correctamente.")
        return "created"
    finally:
        db.close()


def main():
    by_slug, source_by_game = load_games_and_sources()

    created = 0
    existing = 0
    unmatched = []

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=100)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        )

        for state_code in STATE_CODES:
            state_url = f"{BASE_URL}/{state_code}"
            print(f"\n{'=' * 90}\nSCRAPE STATE: {state_code.upper()} -> {state_url}\n{'=' * 90}")

            try:
                text_value = fetch_page_text(context, state_url)
                blocks = parse_latest_blocks(text_value)
            except Exception as e:
                print(f"{state_code.upper()}: no se pudo abrir. Detalle: {e}")
                continue

            for block in blocks:
                candidates = block_to_slug_candidates(block, state_code)
                game = None
                used_slug = None

                for slug in candidates:
                    game = by_slug.get(slug.lower())
                    if game:
                        used_slug = slug
                        break

                if not game:
                    if "jersey cash 5" in block["title"].lower():
                        game = by_slug.get("jersey-cash-5")
                        used_slug = "jersey-cash-5"

                if not game and candidates and candidates[0].startswith("powerball-double-play-"):
                    continue

                if not game:
                    unmatched.append({
                        "state": state_code,
                        "title": block["title"],
                        "slug_candidates": candidates,
                    })
                    print(f"UNMATCHED: {candidates} <- {block['title']}")
                    continue

                source_url = choose_source_url(game, source_by_game.get(game.id, []))
                data = parse_payload_to_numbers(block, game.slug)
                if not data:
                    print(f"NO DATA: {game.slug} <- {block['title']}")
                    continue

                result = upsert_draw(game, source_url, data)
                if result == "created":
                    created += 1
                elif result == "existing":
                    existing += 1

        context.close()
        browser.close()

    print("\nSUMMARY")
    print("=" * 90)
    print(f"Created: {created}")
    print(f"Existing: {existing}")
    print(f"Unmatched: {len(unmatched)}")

    if unmatched:
        print("\nUNMATCHED SAMPLE:")
        print(json.dumps(unmatched[:100], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
