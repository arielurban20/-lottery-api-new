import re
import unicodedata
from typing import Any, Dict, List, Optional


def strip_accents(text: str) -> str:
    if not text:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(c)
    )


def clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def slugify(text: str) -> str:
    text = strip_accents(text.lower())
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def normalize_for_match(text: str) -> str:
    text = strip_accents(text.lower())
    text = text.replace("+", " plus ")
    text = text.replace("&", " and ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_time_token(title: str) -> Optional[str]:
    t = normalize_for_match(title)

    m = re.search(r"\b(\d{1,2}:\d{2}\s?(?:am|pm))\b", t)
    if m:
        return m.group(1).replace(" ", "")

    m = re.search(r"\b(\d{1,2}\s?(?:am|pm))\b", t)
    if m:
        return m.group(1).replace(" ", "")

    return None


def detect_draw_type(title: str) -> str:
    t = normalize_for_match(title)

    if " dia " in f" {t} ":
        return "day"
    if " noche " in f" {t} ":
        return "night"

    if " midday " in f" {t} ":
        return "midday"
    if " daytime " in f" {t} ":
        return "daytime"
    if " day " in f" {t} ":
        return "day"
    if " evening " in f" {t} ":
        return "evening"
    if " night " in f" {t} ":
        return "night"
    if " morning " in f" {t} ":
        return "morning"
    if " afternoon " in f" {t} ":
        return "afternoon"
    if " late night " in f" {t} ":
        return "late-night"
    if " late morning " in f" {t} ":
        return "late-morning"
    if " matinee " in f" {t} ":
        return "matinee"
    if " brunch " in f" {t} ":
        return "brunch"
    if " suppertime " in f" {t} ":
        return "suppertime"
    if " early bird " in f" {t} ":
        return "early-bird"
    if " drive time " in f" {t} ":
        return "drive-time"
    if " prime time " in f" {t} ":
        return "prime-time"
    if " primetime " in f" {t} ":
        return "prime-time"
    if " coffee break " in f" {t} ":
        return "coffee-break"
    if " lunch break " in f" {t} ":
        return "lunch-break"
    if " lunch rush " in f" {t} ":
        return "lunch-rush"
    if " rush hour " in f" {t} ":
        return "rush-hour"
    if " after hours " in f" {t} ":
        return "after-hours"
    if " morning buzz " in f" {t} ":
        return "morning-buzz"
    if " clock out cash " in f" {t} ":
        return "clock-out-cash"
    if " primetime pop " in f" {t} ":
        return "primetime-pop"
    if " midnight money " in f" {t} ":
        return "midnight-money"
    if " night owl " in f" {t} ":
        return "night-owl"

    time_token = extract_time_token(title)
    if time_token:
        return time_token

    return "main"


def remove_variant_words(title: str) -> str:
    t = clean_spaces(title)

    patterns = [
        r"\bMidday\b",
        r"\bDaytime\b",
        r"\bDay\b",
        r"\bEvening\b",
        r"\bNight\b",
        r"\bMorning\b",
        r"\bAfternoon\b",
        r"\bLate Night\b",
        r"\bLate Morning\b",
        r"\bMatinee\b",
        r"\bBrunch\b",
        r"\bSuppertime\b",
        r"\bEarly Bird\b",
        r"\bDrive Time\b",
        r"\bPrime Time\b",
        r"\bPrimetime\b",
        r"\bCoffee Break\b",
        r"\bLunch Break\b",
        r"\bLunch Rush\b",
        r"\bRush Hour\b",
        r"\bAfter Hours\b",
        r"\bMorning Buzz\b",
        r"\bClock Out Cash\b",
        r"\bPrimetime Pop\b",
        r"\bMidnight Money\b",
        r"\bNight Owl\b",
        r"\bDía\b",
        r"\bNoche\b",
        r"\b1pm\b",
        r"\b4pm\b",
        r"\b7pm\b",
        r"\b10pm\b",
        r"\b9am\b",
        r"\b1:50pm\b",
        r"\b7:50pm\b",
        r"\b11:30pm\b",
    ]

    for p in patterns:
        t = re.sub(p, "", t, flags=re.IGNORECASE)

    t = re.sub(r"\s+", " ", t).strip()
    return t


def canonical_game_info(original_title: str, state_code: Optional[str] = None) -> Dict[str, Any]:
    original_title = clean_spaces(original_title)
    state_code = (state_code or "").lower().strip()

    match_title = normalize_for_match(original_title)
    draw_type = detect_draw_type(original_title)
    base_title = remove_variant_words(original_title)
    base_match = normalize_for_match(base_title)

    canonical_name = None
    canonical_slug = None
    include_state_in_slug = True
    parser_type = "standard"

    # MULTI-STATE
    if "powerball double play" in match_title:
        canonical_name = "Powerball Double Play"
        canonical_slug = "powerball-double-play"
        include_state_in_slug = False
    elif "powerball" in match_title:
        canonical_name = "Powerball"
        canonical_slug = "powerball"
        include_state_in_slug = False
    elif "mega millions" in match_title:
        canonical_name = "Mega Millions"
        canonical_slug = "mega-millions"
        include_state_in_slug = False
    elif "millionaire for life" in match_title:
        canonical_name = "Millionaire for Life"
        canonical_slug = "millionaire-for-life"
        include_state_in_slug = False
    elif "lotto america" in match_title:
        canonical_name = "Lotto America"
        canonical_slug = "lotto-america"
        include_state_in_slug = False
    elif base_match == "2by2" or "2by2" in match_title:
        canonical_name = "2by2"
        canonical_slug = "2by2"
        include_state_in_slug = False
        parser_type = "2by2"

    # CASH POP
    elif "cash pop" in match_title:
        canonical_name = "Cash Pop"
        canonical_slug = "cash-pop"
        include_state_in_slug = True

    # DC
    elif base_match.startswith("dc-3"):
        canonical_name = "DC-3"
        canonical_slug = "dc-3"
        include_state_in_slug = True
    elif base_match.startswith("dc-4"):
        canonical_name = "DC-4"
        canonical_slug = "dc-4"
        include_state_in_slug = True
    elif base_match.startswith("dc-5"):
        canonical_name = "DC-5"
        canonical_slug = "dc-5"
        include_state_in_slug = True

    # PUERTO RICO
    elif base_match.startswith("pega 2"):
        canonical_name = "Pega 2"
        canonical_slug = "pega-2"
        include_state_in_slug = True
    elif base_match.startswith("pega 3"):
        canonical_name = "Pega 3"
        canonical_slug = "pega-3"
        include_state_in_slug = True
    elif base_match.startswith("pega 4"):
        canonical_name = "Pega 4"
        canonical_slug = "pega-4"
        include_state_in_slug = True
    elif "loteria tradicional" in base_match:
        canonical_name = "Lotería Tradicional"
        canonical_slug = "loteria-tradicional"
        include_state_in_slug = True
        parser_type = "special"

    # PICKS
    elif base_match == "pick 2":
        canonical_name = "Pick 2"
        canonical_slug = "pick-2"
        include_state_in_slug = True
    elif base_match == "pick 3":
        canonical_name = "Pick 3"
        canonical_slug = "pick-3"
        include_state_in_slug = True
    elif base_match == "pick 4":
        canonical_name = "Pick 4"
        canonical_slug = "pick-4"
        include_state_in_slug = True
    elif base_match == "pick 5":
        canonical_name = "Pick 5"
        canonical_slug = "pick-5"
        include_state_in_slug = True
    elif base_match == "pick 6":
        canonical_name = "Pick 6"
        canonical_slug = "pick-6"
        include_state_in_slug = True
    elif base_match == "pick 10":
        canonical_name = "Pick 10"
        canonical_slug = "pick-10"
        include_state_in_slug = True

    # DAILY
    elif base_match == "daily 3":
        canonical_name = "Daily 3"
        canonical_slug = "daily-3"
        include_state_in_slug = True
    elif base_match == "daily 4":
        canonical_name = "Daily 4"
        canonical_slug = "daily-4"
        include_state_in_slug = True
    elif base_match == "daily 5":
        canonical_name = "Daily 5"
        canonical_slug = "daily-5"
        include_state_in_slug = True

    # PLAY
    elif base_match == "play 3":
        canonical_name = "Play 3"
        canonical_slug = "play-3"
        include_state_in_slug = True
    elif base_match == "play 4":
        canonical_name = "Play 4"
        canonical_slug = "play-4"
        include_state_in_slug = True
    elif base_match == "play 5":
        canonical_name = "Play 5"
        canonical_slug = "play-5"
        include_state_in_slug = True

    # NUMBERS / WIN / TAKE
    elif base_match == "numbers":
        canonical_name = "Numbers"
        canonical_slug = "numbers"
        include_state_in_slug = True
    elif base_match == "numbers game":
        canonical_name = "Numbers Game"
        canonical_slug = "numbers-game"
        include_state_in_slug = True
    elif base_match == "win 4":
        canonical_name = "Win 4"
        canonical_slug = "win-4"
        include_state_in_slug = True
    elif base_match == "take 5":
        canonical_name = "Take 5"
        canonical_slug = "take-5"
        include_state_in_slug = True

    # SPECIALS
    elif "all or nothing" in base_match:
        canonical_name = "All or Nothing"
        canonical_slug = "all-or-nothing"
        include_state_in_slug = True
        parser_type = "many-numbers"
    elif "quick draw" in base_match:
        canonical_name = "Quick Draw"
        canonical_slug = "quick-draw"
        include_state_in_slug = True
        parser_type = "many-numbers"
    elif "keno" in base_match:
        canonical_name = clean_spaces(base_title)
        canonical_slug = slugify(base_title)
        include_state_in_slug = True
        parser_type = "many-numbers"
    elif "myday" in base_match:
        canonical_name = "MyDaY"
        canonical_slug = "myday"
        include_state_in_slug = True
        parser_type = "special"
    elif "daily derby" in base_match:
        canonical_name = "Daily Derby"
        canonical_slug = "daily-derby"
        include_state_in_slug = True
        parser_type = "special"
    elif "poker lotto" in base_match:
        canonical_name = "Poker Lotto"
        canonical_slug = "poker-lotto"
        include_state_in_slug = True
        parser_type = "special"
    elif "colorado lotto" in base_match:
        canonical_name = "Colorado Lotto+"
        canonical_slug = "colorado-lotto-plus"
        include_state_in_slug = True
    elif "fantasy 5" in base_match:
        canonical_name = "Fantasy 5"
        canonical_slug = "fantasy-5"
        include_state_in_slug = True
    elif "mass cash" in base_match:
        canonical_name = "Mass Cash"
        canonical_slug = "mass-cash"
        include_state_in_slug = True
    elif "super kansas cash" in base_match:
        canonical_name = "Super Kansas Cash"
        canonical_slug = "super-kansas-cash"
        include_state_in_slug = True
    elif "cash ball 225" in base_match:
        canonical_name = "Cash Ball 225"
        canonical_slug = "cash-ball-225"
        include_state_in_slug = True
    elif "loto cash" in base_match:
        canonical_name = "Loto Cash"
        canonical_slug = "loto-cash"
        include_state_in_slug = True
    elif "revancha" in base_match:
        canonical_name = "Revancha"
        canonical_slug = "revancha"
        include_state_in_slug = True
    elif "big sky bonus" in base_match:
        canonical_name = "Big Sky Bonus"
        canonical_slug = "big-sky-bonus"
        include_state_in_slug = True
    elif "bank a million" in base_match:
        canonical_name = "Bank a Million"
        canonical_slug = "bank-a-million"
        include_state_in_slug = True
    elif "wild money" in base_match:
        canonical_name = "Wild Money"
        canonical_slug = "wild-money"
        include_state_in_slug = True
    elif "bonus match 5" in base_match:
        canonical_name = "Bonus Match 5"
        canonical_slug = "bonus-match-5"
        include_state_in_slug = True
    elif "tennessee cash" in base_match:
        canonical_name = "Tennessee Cash"
        canonical_slug = "tennessee-cash"
        include_state_in_slug = True
    elif "texas two step" in base_match:
        canonical_name = "Texas Two Step"
        canonical_slug = "texas-two-step"
        include_state_in_slug = True
    elif "megabucks" in base_match:
        canonical_name = "Megabucks"
        canonical_slug = "megabucks"
        include_state_in_slug = True
    elif "lucky day lotto" in base_match:
        canonical_name = "Lucky Day Lotto"
        canonical_slug = "lucky-day-lotto"
        include_state_in_slug = True

    # FALLBACK
    else:
        canonical_name = clean_spaces(base_title or original_title)
        canonical_slug = slugify(base_title or original_title)
        include_state_in_slug = True

    final_slug = canonical_slug
    if include_state_in_slug and state_code:
        final_slug = f"{canonical_slug}-{state_code}"

    return {
        "original_title": original_title,
        "base_title": base_title,
        "canonical_name": canonical_name,
        "canonical_slug": canonical_slug,
        "final_slug": final_slug,
        "draw_type": draw_type,
        "include_state_in_slug": include_state_in_slug,
        "parser_type": parser_type,
    }


GAME_RULES_BY_FINAL_SLUG: Dict[str, Dict[str, Any]] = {
    # reglas específicas por estado
    "all-or-nothing-tx": {"main": 12, "bonus": 0},
    "all-or-nothing-wi": {"main": 11, "bonus": 0},

    "megabucks-me": {"main": 5, "bonus": 1},
    "megabucks-nh": {"main": 5, "bonus": 1},
    "megabucks-vt": {"main": 5, "bonus": 1},

    "megabucks-ma": {"main": 6, "bonus": 0},
    "megabucks-or": {"main": 6, "bonus": 0},
    "megabucks-wi": {"main": 6, "bonus": 0},

    "loto-cash-pr": {"main": 5, "bonus": 1},
    "revancha-pr": {"main": 5, "bonus": 1},

    "colorado-lotto-plus-co": {"main": 6, "bonus": 1},
    "bank-a-million-va": {"main": 6, "bonus": 1},
    "wild-money-ri": {"main": 5, "bonus": 1},
    "bonus-match-5-md": {"main": 5, "bonus": 1},
    "super-kansas-cash-ks": {"main": 5, "bonus": 1},
    "tennessee-cash-tn": {"main": 5, "bonus": 1},
    "texas-two-step-tx": {"main": 4, "bonus": 1},
    "big-sky-bonus-mt": {"main": 4, "bonus": 1},

    "pick-10-ny": {"main": 20, "bonus": 0},
    "quick-draw-in": {"main": 20, "bonus": 0},
    "daily-keno-wa": {"main": 20, "bonus": 0},

    "myday-ne": {"main": 3, "bonus": 0},
    "daily-derby-ca": {"main": 3, "bonus": 0},
    "poker-lotto-mi": {"main": 5, "bonus": 0},

    "pick-2-fl": {"main": 2, "bonus": 0},
    "pick-2-pa": {"main": 2, "bonus": 0},

    "treasure-hunt-pa": {"main": 5, "bonus": 0},

    "dc-3-dc": {"main": 3, "bonus": 0},
    "dc-4-dc": {"main": 4, "bonus": 0},
    "dc-5-dc": {"main": 5, "bonus": 0},
}

GAME_RULES_BY_CANONICAL_SLUG: Dict[str, Dict[str, Any]] = {
    "powerball": {"main": 5, "bonus": 1},
    "powerball-double-play": {"main": 5, "bonus": 1},
    "mega-millions": {"main": 5, "bonus": 1},
    "millionaire-for-life": {"main": 5, "bonus": 1},
    "lotto-america": {"main": 5, "bonus": 1},

    "2by2": {"main": 4, "bonus": 0},

    "pick-2": {"main": 2, "bonus": 0},
    "pick-3": {"main": 3, "bonus": 0},
    "pick-4": {"main": 4, "bonus": 0},
    "pick-5": {"main": 5, "bonus": 0},
    "pick-6": {"main": 6, "bonus": 0},
    "pick-10": {"main": 20, "bonus": 0},

    "daily-3": {"main": 3, "bonus": 0},
    "daily-4": {"main": 4, "bonus": 0},
    "daily-5": {"main": 5, "bonus": 0},

    "play-3": {"main": 3, "bonus": 0},
    "play-4": {"main": 4, "bonus": 0},
    "play-5": {"main": 5, "bonus": 0},

    "numbers": {"main": 3, "bonus": 0},
    "numbers-game": {"main": 3, "bonus": 0},
    "win-4": {"main": 4, "bonus": 0},
    "take-5": {"main": 5, "bonus": 0},

    "cash-pop": {"main": 1, "bonus": 0},

    "pega-2": {"main": 2, "bonus": 0},
    "pega-3": {"main": 3, "bonus": 0},
    "pega-4": {"main": 4, "bonus": 0},

    "fantasy-5": {"main": 5, "bonus": 0},
    "mass-cash": {"main": 5, "bonus": 0},
    "cash-ball-225": {"main": 4, "bonus": 1},
    "gimme-5": {"main": 5, "bonus": 0},
    "super-cash": {"main": 6, "bonus": 0},
    "badger-5": {"main": 5, "bonus": 0},
    "lotto": {"main": 6, "bonus": 0},
    "lucky-day-lotto": {"main": 5, "bonus": 0},
    "quick-draw": {"main": 20, "bonus": 0},
    "daily-keno": {"main": 20, "bonus": 0},
    "treasure-hunt": {"main": 5, "bonus": 0},
    "match-6": {"main": 6, "bonus": 0},
    "cash-5": {"main": 5, "bonus": 0},
    "rolling-cash-5": {"main": 5, "bonus": 0},
    "jersey-cash-5": {"main": 5, "bonus": 1},
    "lotto-47": {"main": 6, "bonus": 0},
    "all-or-nothing": {"main": 12, "bonus": 0},
    "myday": {"main": 3, "bonus": 0},
    "daily-derby": {"main": 3, "bonus": 0},
    "poker-lotto": {"main": 5, "bonus": 0},
}


def _safe_bonus_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def _bonus_already_in_raw(nums: List[int], bonus: Optional[str]) -> bool:
    if not bonus or not bonus.isdigit():
        return False
    return nums and nums[-1] == int(bonus)


def split_main_and_bonus(
    game_slug: str,
    raw_numbers: List[int],
    bonus_number: Optional[str] = None,
    multiplier: Optional[str] = None,
) -> Dict[str, Any]:
    nums = [int(x) for x in (raw_numbers or []) if str(x).isdigit()]
    bonus = _safe_bonus_str(bonus_number)

    # 1) buscar primero por slug final exacto
    rule = GAME_RULES_BY_FINAL_SLUG.get(game_slug)

    # 2) fallback a canonical slug
    if not rule:
        canonical_guess = game_slug
        if "-" in game_slug:
            parts = game_slug.split("-")
            if len(parts) >= 2 and len(parts[-1]) == 2:
                canonical_guess = "-".join(parts[:-1])
        rule = GAME_RULES_BY_CANONICAL_SLUG.get(canonical_guess)

    # 3) si no hay regla, devolver crudo
    if not rule:
        return {
            "main_numbers": nums,
            "bonus_number": bonus,
            "multiplier": multiplier,
        }

    main_count = rule.get("main", len(nums))
    bonus_count = rule.get("bonus", 0)

    # juegos sin bonus
    if bonus_count == 0:
        return {
            "main_numbers": nums[:main_count],
            "bonus_number": None,
            "multiplier": multiplier,
        }

    # si vino bonus explícito por texto
    if bonus:
        return {
            "main_numbers": nums[:main_count],
            "bonus_number": bonus,
            "multiplier": multiplier,
        }

    # si no vino bonus explícito, derivarlo del array
    derived_bonus = None
    if len(nums) >= main_count + bonus_count:
        derived_bonus = str(nums[main_count])

    return {
        "main_numbers": nums[:main_count],
        "bonus_number": derived_bonus,
        "multiplier": multiplier,
    }