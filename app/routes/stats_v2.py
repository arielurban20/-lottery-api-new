from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Draw, Game

router = APIRouter(prefix="/stats-v2", tags=["Stats V2"])


# =========================
# CONFIG DOMINIOS FIJOS
# =========================
# main_range = (min, max)
# bonus_range = (min, max)
# include_zero_main = True solo para juegos tipo Pick 3 / Pick 4 / Pick 5 / Numbers, etc.
GAME_RULES: Dict[str, Dict[str, Any]] = {
    # Multi-state / nacionales
    "powerball": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-co": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-ct": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-fl": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-id": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-in": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-ia": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-ks": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-ky": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-me": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-md": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-mi": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-mo": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-mt": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-ne": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-nj": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-nm": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-nc": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-ok": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-pa": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-pr": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-sc": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-sd": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-tn": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-wa": {"main_range": (1, 69), "bonus_range": (1, 26)},
    "powerball-double-play-dc": {"main_range": (1, 69), "bonus_range": (1, 26)},

    "mega-millions": {"main_range": (1, 70), "bonus_range": (1, 25)},
    "millionaire-for-life": {"main_range": (1, 48), "bonus_range": (1, 10)},
    "lotto-america": {"main_range": (1, 52), "bonus_range": (1, 10), "multiplier_range": (2, 5)},
    "2by2": {"main_range": (1, 26)},  # 4 números, 2 rojos + 2 blancos

    # Pick / Daily / Numbers / Win4 / Play / DC
    "pick-2-fl": {"main_range": (0, 9), "include_zero_main": True},
    "pick-2-pa": {"main_range": (0, 9), "include_zero_main": True},
    "pega-2-pr": {"main_range": (0, 9), "include_zero_main": True},

    "pick-3-az": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-co": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-id": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-ia": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-ks": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-ky": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-la": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-me": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-md": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-mn": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-mo": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-ne": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-nh": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-nj": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-nm": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-nc": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-oh": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-ok": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-sc": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-va": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-vt": {"main_range": (0, 9), "include_zero_main": True},
    "pick-3-wi": {"main_range": (0, 9), "include_zero_main": True},
    "daily-3-ca": {"main_range": (0, 9), "include_zero_main": True},
    "daily-3-in": {"main_range": (0, 9), "include_zero_main": True},
    "daily-3-mi": {"main_range": (0, 9), "include_zero_main": True},
    "daily-3-wv": {"main_range": (0, 9), "include_zero_main": True},
    "cash-3-ar": {"main_range": (0, 9), "include_zero_main": True},
    "cash-3-ga": {"main_range": (0, 9), "include_zero_main": True},
    "cash-3-ms": {"main_range": (0, 9), "include_zero_main": True},
    "cash-3-tn": {"main_range": (0, 9), "include_zero_main": True},
    "cash-3-midday-fl": {"main_range": (0, 9), "include_zero_main": True},
    "numbers-ny": {"main_range": (0, 9), "include_zero_main": True},
    "numbers-game-ma": {"main_range": (0, 9), "include_zero_main": True},
    "numbers-game-ri": {"main_range": (0, 9), "include_zero_main": True},
    "play-3-ct": {"main_range": (0, 9), "include_zero_main": True},
    "play-3-de": {"main_range": (0, 9), "include_zero_main": True},
    "dc-3-dc": {"main_range": (0, 9), "include_zero_main": True},

    "pick-4-ca": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-co": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-id": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-ia": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-ky": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-la": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-me": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-md": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-mo": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-ne": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-nh": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-nj": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-nm": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-nc": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-oh": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-or": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-sc": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-va": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-vt": {"main_range": (0, 9), "include_zero_main": True},
    "pick-4-wi": {"main_range": (0, 9), "include_zero_main": True},
    "daily-4-ca": {"main_range": (0, 9), "include_zero_main": True},
    "daily-4-in": {"main_range": (0, 9), "include_zero_main": True},
    "daily-4-mi": {"main_range": (0, 9), "include_zero_main": True},
    "daily-4-tx": {"main_range": (0, 9), "include_zero_main": True},
    "daily-4-wv": {"main_range": (0, 9), "include_zero_main": True},
    "cash-4-ar": {"main_range": (0, 9), "include_zero_main": True},
    "cash-4-ga": {"main_range": (0, 9), "include_zero_main": True},
    "cash-4-ms": {"main_range": (0, 9), "include_zero_main": True},
    "cash-4-tn": {"main_range": (0, 9), "include_zero_main": True},
    "win-4-ny": {"main_range": (0, 9), "include_zero_main": True},
    "play-4-ct": {"main_range": (0, 9), "include_zero_main": True},
    "play-4-de": {"main_range": (0, 9), "include_zero_main": True},
    "dc-4-dc": {"main_range": (0, 9), "include_zero_main": True},

    "pick-5-fl": {"main_range": (0, 9), "include_zero_main": True},
    "pick-5-md": {"main_range": (0, 9), "include_zero_main": True},
    "pick-5-ne": {"main_range": (0, 9), "include_zero_main": True},
    "pick-5-oh": {"main_range": (0, 9), "include_zero_main": True},
    "pick-5-pa": {"main_range": (0, 9), "include_zero_main": True},
    "pick-5-va": {"main_range": (0, 9), "include_zero_main": True},
    "cash-5-in": {"main_range": (1, 45)},
    "cash-5-co": {"main_range": (1, 32)},
    "cash-5-ct": {"main_range": (1, 35)},
    "cash-5-nc": {"main_range": (1, 43)},
    "cash-5-ok": {"main_range": (1, 36)},
    "cash-5-pa": {"main_range": (1, 43)},
    "cash-5-tx": {"main_range": (1, 35)},
    "cash-5-va": {"main_range": (1, 41)},
    "play-5-de": {"main_range": (0, 9), "include_zero_main": True},
    "dc-5-dc": {"main_range": (0, 9), "include_zero_main": True},

    # Cash Pop / Pop / single-number
    "cash-pop-fl": {"main_range": (1, 15)},
    "cash-pop-ga": {"main_range": (1, 15)},
    "cash-pop-in": {"main_range": (1, 15)},
    "cash-pop-me": {"main_range": (1, 15)},
    "cash-pop-md": {"main_range": (1, 15)},
    "cash-pop-ms": {"main_range": (1, 15)},
    "cash-pop-mo": {"main_range": (1, 15)},
    "cash-pop-nc": {"main_range": (1, 15)},
    "cash-pop-pa": {"main_range": (1, 15)},
    "cash-pop-sc": {"main_range": (1, 15)},
    "cash-pop-va": {"main_range": (1, 15)},
    "cash-pop-wa": {"main_range": (1, 15)},

    # Otros conocidos
    "fantasy-5-az": {"main_range": (1, 41)},
    "fantasy-5-ca": {"main_range": (1, 39)},
    "fantasy-5-fl": {"main_range": (1, 36)},
    "fantasy-5-ga": {"main_range": (1, 42)},
    "fantasy-5-mi": {"main_range": (1, 39)},
    "gimme-5-me": {"main_range": (1, 39)},
    "gimme-5-nh": {"main_range": (1, 39)},
    "gimme-5-vt": {"main_range": (1, 39)},
    "take-5-ny": {"main_range": (1, 39)},
    "badger-5-wi": {"main_range": (1, 31)},
    "match-5-ms": {"main_range": (1, 35)},
    "bonus-match-5-md": {"main_range": (1, 39), "bonus_range": (1, 10)},
    "palmetto-cash-5-sc": {"main_range": (1, 38)},
    "north-5-mn": {"main_range": (1, 31)},
    "gopher-5-mn": {"main_range": (1, 47)},
    "show-me-cash-mo": {"main_range": (1, 39)},
    "cash-ball-225-ky": {"main_range": (1, 35), "bonus_range": (1, 25)},
    "road-runner-cash-nm": {"main_range": (1, 37)},
    "idaho-cash-id": {"main_range": (1, 45)},
    "multi-win-lotto-de": {"main_range": (1, 35)},
    "wild-money-ri": {"main_range": (1, 38), "bonus_range": (1, 38)},
    "big-sky-bonus-mt": {"main_range": (1, 31), "bonus_range": (1, 16)},
    "dakota-cash-sd": {"main_range": (1, 35)},
    "montana-cash-mt": {"main_range": (1, 45)},
    "super-kansas-cash-ks": {"main_range": (1, 32), "bonus_range": (1, 25)},
    "natural-state-jackpot-ar": {"main_range": (1, 39)},
    "daily-tennessee-jackpot-tn": {"main_range": (1, 38)},
    "daily-keno-wa": {"main_range": (1, 80)},
    "pick-10-ny": {"main_range": (1, 80)},
    "quick-draw-in": {"main_range": (1, 80)},
    "keno-mi": {"main_range": (1, 80)},
    "all-or-nothing-tx": {"main_range": (1, 24)},
    "all-or-nothing-wi": {"main_range": (1, 22)},
    "treasure-hunt-pa": {"main_range": (1, 30)},
    "kicker-oh": {"main_range": (0, 9), "include_zero_main": True},
    "poker-lotto-mi": {"main_range": (1, 13)},
    "myday-ne": {"main_range": (1, 31)},
    "numbers-game-ma": {"main_range": (0, 9), "include_zero_main": True},
    "numbers-game-ri": {"main_range": (0, 9), "include_zero_main": True},

    # Lotto
    "lotto-ar": {"main_range": (1, 40)},
    "lotto-ct": {"main_range": (1, 44)},
    "lotto-fl": {"main_range": (1, 53)},
    "lotto-il": {"main_range": (1, 50)},
    "lotto-la": {"main_range": (1, 42)},
    "lotto-ny": {"main_range": (1, 59)},
    "lotto-wa": {"main_range": (1, 49)},
    "lotto-texas-tx": {"main_range": (1, 54)},
    "hoosier-lotto-in": {"main_range": (1, 46)},
    "classic-lotto-oh": {"main_range": (1, 49)},
    "pick-6-nj": {"main_range": (1, 46)},
    "match-6-pa": {"main_range": (1, 49)},
    "superlotto-plus-ca": {"main_range": (1, 47), "bonus_range": (1, 27)},
    "megabucks-ma": {"main_range": (1, 49)},
    "megabucks-me": {"main_range": (1, 41), "bonus_range": (1, 6)},
    "megabucks-nh": {"main_range": (1, 41), "bonus_range": (1, 6)},
    "megabucks-vt": {"main_range": (1, 41), "bonus_range": (1, 6)},
    "megabucks-or": {"main_range": (1, 48)},
    "megabucks-wi": {"main_range": (1, 49)},
    "lotto-47-mi": {"main_range": (1, 47)},
    "cowboy-draw-wy": {"main_range": (1, 45)},
    "cash-25-wv": {"main_range": (1, 25)},
    "bank-a-million-va": {"main_range": (1, 40), "bonus_range": (1, 25)},
    "tennessee-cash-tn": {"main_range": (1, 35), "bonus_range": (1, 5)},
    "mo-millions-mo": {"main_range": (1, 49)},
    "win-for-life-or": {"main_range": (1, 77)},
    "texas-two-step-tx": {"main_range": (1, 35), "bonus_range": (1, 35)},
    "jackpot-triple-play-fl": {"main_range": (1, 46)},
    "revancha-pr": {"main_range": (1, 39)},
    "loto-cash-pr": {"main_range": (1, 39), "bonus_range": (1, 10)},
}


router = APIRouter(prefix="/stats-v2", tags=["Stats V2"])


def get_db() -> Session:
    return SessionLocal()


def normalize_int_list(values: Any) -> List[int]:
    if not values:
        return []

    if isinstance(values, list):
        out = []
        for v in values:
            try:
                out.append(int(v))
            except Exception:
                pass
        return out

    return []


def get_game_by_slug(db: Session, game_slug: str) -> Optional[Game]:
    return db.execute(
        select(Game).where(Game.slug == game_slug.lower())
    ).scalar_one_or_none()


def get_draws_for_game(db: Session, game_id: int, limit: int = 3000) -> List[Draw]:
    stmt = (
        select(Draw)
        .where(Draw.game_id == game_id)
        .order_by(desc(Draw.draw_date), desc(Draw.id))
        .limit(limit)
    )
    return db.execute(stmt).scalars().all()


def parse_bonus_number(draw: Draw) -> Optional[int]:
    if draw.bonus_number is None:
        return None
    try:
        return int(str(draw.bonus_number))
    except Exception:
        return None


def get_game_rules(game_slug: str, draws: List[Draw]) -> Dict[str, Any]:
    if game_slug in GAME_RULES:
        return GAME_RULES[game_slug]

    # fallback inteligente
    all_numbers = []
    bonus_numbers = []

    for d in draws:
        all_numbers.extend(normalize_int_list(d.main_numbers))
        b = parse_bonus_number(d)
        if b is not None:
            bonus_numbers.append(b)

    mx_main = max(all_numbers) if all_numbers else 0
    mx_bonus = max(bonus_numbers) if bonus_numbers else 0
    mn_main = min(all_numbers) if all_numbers else 1

    include_zero = mn_main == 0
    main_min = 0 if include_zero else 1

    # redondeo básico
    if mx_main <= 9:
        main_max = 9
    elif mx_main <= 10:
        main_max = 10
    elif mx_main <= 15:
        main_max = 15
    elif mx_main <= 20:
        main_max = 20
    elif mx_main <= 24:
        main_max = 24
    elif mx_main <= 25:
        main_max = 25
    elif mx_main <= 31:
        main_max = 31
    elif mx_main <= 35:
        main_max = 35
    elif mx_main <= 39:
        main_max = 39
    elif mx_main <= 40:
        main_max = 40
    elif mx_main <= 45:
        main_max = 45
    elif mx_main <= 46:
        main_max = 46
    elif mx_main <= 47:
        main_max = 47
    elif mx_main <= 48:
        main_max = 48
    elif mx_main <= 49:
        main_max = 49
    elif mx_main <= 52:
        main_max = 52
    elif mx_main <= 53:
        main_max = 53
    elif mx_main <= 54:
        main_max = 54
    elif mx_main <= 59:
        main_max = 59
    elif mx_main <= 69:
        main_max = 69
    elif mx_main <= 70:
        main_max = 70
    elif mx_main <= 80:
        main_max = 80
    else:
        main_max = mx_main

    rules = {
        "main_range": (main_min, main_max),
        "include_zero_main": include_zero,
    }

    if mx_bonus > 0:
        rules["bonus_range"] = (1 if mx_bonus > 0 else 0, mx_bonus)

    return rules


def build_domain(range_tuple: Tuple[int, int]) -> List[int]:
    start, end = range_tuple
    return list(range(start, end + 1))


def split_numbers_using_rules(draw: Draw, rules: Dict[str, Any]) -> Dict[str, Any]:
    raw_main = normalize_int_list(draw.main_numbers)
    raw_bonus = parse_bonus_number(draw)

    clean_main: List[int] = []
    clean_bonus: Optional[int] = None

    main_range = rules.get("main_range")
    bonus_range = rules.get("bonus_range")

    if main_range:
        mn, mx = main_range
        clean_main = [n for n in raw_main if mn <= n <= mx]
    else:
        clean_main = raw_main[:]

    if raw_bonus is not None and bonus_range:
        bmn, bmx = bonus_range
        if bmn <= raw_bonus <= bmx:
            clean_bonus = raw_bonus

    return {
        "main_numbers": clean_main,
        "bonus_number": clean_bonus,
    }


def get_last_seen_map(draws: List[Draw], rules: Dict[str, Any], mode: str = "main") -> Dict[int, Dict[str, Any]]:
    last_seen: Dict[int, Dict[str, Any]] = {}

    for idx, draw in enumerate(draws):
        parsed = split_numbers_using_rules(draw, rules)

        if mode == "main":
            values = parsed["main_numbers"]
        else:
            values = [parsed["bonus_number"]] if parsed["bonus_number"] is not None else []

        for n in values:
            if n not in last_seen:
                last_seen[n] = {
                    "number": n,
                    "draw_id": draw.id,
                    "draw_date": str(draw.draw_date) if draw.draw_date else None,
                    "draw_type": draw.draw_type,
                    "draws_ago": idx,
                }

    return last_seen


def get_frequency(values: List[int]) -> List[Dict[str, Any]]:
    c = Counter(values)
    return [{"number": n, "frequency": f} for n, f in c.most_common()]


def build_hot_cold_overdue(
    draws: List[Draw],
    domain: List[int],
    rules: Dict[str, Any],
    mode: str = "main",
    top: int = 10,
) -> Dict[str, Any]:
    values: List[int] = []

    for draw in draws:
        parsed = split_numbers_using_rules(draw, rules)
        if mode == "main":
            values.extend(parsed["main_numbers"])
        else:
            if parsed["bonus_number"] is not None:
                values.append(parsed["bonus_number"])

    frequency = get_frequency(values)
    freq_map = {row["number"]: row["frequency"] for row in frequency}
    last_seen_map = get_last_seen_map(draws, rules, mode=mode)

    hot = frequency[:top]

    cold_pool = []
    overdue_pool = []

    for n in domain:
        freq = freq_map.get(n, 0)
        last = last_seen_map.get(n)

        cold_pool.append({
            "number": n,
            "frequency": freq,
            "last_seen_date": last["draw_date"] if last else None,
            "draws_ago": last["draws_ago"] if last else None,
            "never_seen": last is None,
        })

        overdue_pool.append({
            "number": n,
            "draws_ago": last["draws_ago"] if last else None,
            "last_seen_date": last["draw_date"] if last else None,
        })

    cold_pool.sort(key=lambda x: (x["frequency"], -(x["draws_ago"] or 999999), x["number"]))
    overdue_pool.sort(key=lambda x: (-(x["draws_ago"] or 999999), x["number"]))

    return {
        "hot": hot,
        "cold": cold_pool[:top],
        "overdue": overdue_pool[:top],
        "frequency": frequency,
        "total_values_counted": len(values),
        "unique_values_seen": len(set(values)),
    }


@router.get("/game/{game_slug}")
def stats_game_v2(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=100),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para: {game_slug}")

        rules = get_game_rules(game.slug, draws)

        main_domain = build_domain(rules["main_range"])
        main_stats = build_hot_cold_overdue(draws, main_domain, rules, mode="main", top=top)

        has_bonus = "bonus_range" in rules
        bonus_stats = None
        bonus_domain = []

        if has_bonus:
            bonus_domain = build_domain(rules["bonus_range"])
            bonus_stats = build_hot_cold_overdue(draws, bonus_domain, rules, mode="bonus", top=top)

        latest = draws[0]

        return {
            "game_slug": game.slug,
            "game_name": game.name,
            "draws_analyzed": len(draws),
            "latest_draw_date": str(latest.draw_date) if latest.draw_date else None,
            "rules": {
                "main_range": rules.get("main_range"),
                "bonus_range": rules.get("bonus_range"),
                "include_zero_main": rules.get("include_zero_main", False),
            },
            "main_stats": {
                "total_draws": len(draws),
                "total_numbers_counted": main_stats["total_values_counted"],
                "unique_numbers_seen": main_stats["unique_values_seen"],
                "hot_numbers": main_stats["hot"],
                "cold_numbers": main_stats["cold"],
                "most_overdue": main_stats["overdue"],
                "frequency": main_stats["frequency"],
            },
            "bonus_stats": {
                "has_bonus": has_bonus,
                "bonus_domain": bonus_domain,
                "total_bonus_counted": bonus_stats["total_values_counted"] if bonus_stats else 0,
                "hot_bonus_numbers": bonus_stats["hot"] if bonus_stats else [],
                "cold_bonus_numbers": bonus_stats["cold"] if bonus_stats else [],
                "most_overdue_bonus": bonus_stats["overdue"] if bonus_stats else [],
                "bonus_frequency": bonus_stats["frequency"] if bonus_stats else [],
            },
        }
    finally:
        db.close()


@router.get("/hot/{game_slug}")
def stats_hot_v2(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=100),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para: {game_slug}")

        rules = get_game_rules(game.slug, draws)
        main_domain = build_domain(rules["main_range"])
        main_stats = build_hot_cold_overdue(draws, main_domain, rules, mode="main", top=top)

        bonus_hot = []
        if "bonus_range" in rules:
            bonus_domain = build_domain(rules["bonus_range"])
            bonus_stats = build_hot_cold_overdue(draws, bonus_domain, rules, mode="bonus", top=top)
            bonus_hot = bonus_stats["hot"]

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "rules": rules,
            "hot_numbers": main_stats["hot"],
            "hot_bonus_numbers": bonus_hot,
        }
    finally:
        db.close()


@router.get("/cold/{game_slug}")
def stats_cold_v2(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=100),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para: {game_slug}")

        rules = get_game_rules(game.slug, draws)
        main_domain = build_domain(rules["main_range"])
        main_stats = build_hot_cold_overdue(draws, main_domain, rules, mode="main", top=top)

        bonus_cold = []
        if "bonus_range" in rules:
            bonus_domain = build_domain(rules["bonus_range"])
            bonus_stats = build_hot_cold_overdue(draws, bonus_domain, rules, mode="bonus", top=top)
            bonus_cold = bonus_stats["cold"]

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "rules": rules,
            "cold_numbers": main_stats["cold"],
            "cold_bonus_numbers": bonus_cold,
        }
    finally:
        db.close()


@router.get("/frequency/{game_slug}")
def stats_frequency_v2(
    game_slug: str,
    limit_draws: int = Query(default=500, ge=10, le=5000),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para: {game_slug}")

        rules = get_game_rules(game.slug, draws)
        main_domain = build_domain(rules["main_range"])
        main_stats = build_hot_cold_overdue(draws, main_domain, rules, mode="main", top=10)

        bonus_frequency = []
        if "bonus_range" in rules:
            bonus_domain = build_domain(rules["bonus_range"])
            bonus_stats = build_hot_cold_overdue(draws, bonus_domain, rules, mode="bonus", top=10)
            bonus_frequency = bonus_stats["frequency"]

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "rules": rules,
            "frequency": main_stats["frequency"],
            "bonus_frequency": bonus_frequency,
        }
    finally:
        db.close()


@router.get("/overdue/{game_slug}")
def stats_overdue_v2(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=100),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para: {game_slug}")

        rules = get_game_rules(game.slug, draws)
        main_domain = build_domain(rules["main_range"])
        main_stats = build_hot_cold_overdue(draws, main_domain, rules, mode="main", top=top)

        bonus_overdue = []
        if "bonus_range" in rules:
            bonus_domain = build_domain(rules["bonus_range"])
            bonus_stats = build_hot_cold_overdue(draws, bonus_domain, rules, mode="bonus", top=top)
            bonus_overdue = bonus_stats["overdue"]

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "rules": rules,
            "most_overdue": main_stats["overdue"],
            "most_overdue_bonus": bonus_overdue,
        }
    finally:
        db.close()