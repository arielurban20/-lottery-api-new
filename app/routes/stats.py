from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Draw, Game

router = APIRouter(prefix="/stats", tags=["Stats"])


def get_db() -> Session:
    db = SessionLocal()
    return db


def close_db(db: Session):
    try:
        db.close()
    except Exception:
        pass


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


def split_main_and_bonus(draw: Draw) -> Dict[str, Any]:
    main_numbers = normalize_int_list(draw.main_numbers)

    bonus_number: Optional[int] = None
    if draw.bonus_number is not None:
        try:
            bonus_number = int(str(draw.bonus_number))
        except Exception:
            bonus_number = None

    return {
        "main_numbers": main_numbers,
        "bonus_number": bonus_number,
    }


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


def build_frequency_table(numbers: List[int]) -> List[Dict[str, Any]]:
    counter = Counter(numbers)

    rows = []
    for number, freq in counter.most_common():
        rows.append({
            "number": number,
            "frequency": freq,
        })
    return rows


def build_last_seen_map(draws: List[Draw], include_bonus: bool = False) -> Dict[int, Dict[str, Any]]:
    last_seen = {}

    # draws vienen en orden DESC por fecha
    for idx, draw in enumerate(draws):
        parsed = split_main_and_bonus(draw)
        current_numbers = list(parsed["main_numbers"])

        if include_bonus and parsed["bonus_number"] is not None:
            current_numbers.append(parsed["bonus_number"])

        for n in current_numbers:
            if n not in last_seen:
                last_seen[n] = {
                    "number": n,
                    "draw_id": draw.id,
                    "draw_date": str(draw.draw_date) if draw.draw_date else None,
                    "draw_type": draw.draw_type,
                    "draws_ago": idx,
                }

    return last_seen


def infer_number_domain(draws: List[Draw], include_bonus: bool = False) -> List[int]:
    seen = set()

    for draw in draws:
        parsed = split_main_and_bonus(draw)
        for n in parsed["main_numbers"]:
            seen.add(n)
        if include_bonus and parsed["bonus_number"] is not None:
            seen.add(parsed["bonus_number"])

    if not seen:
        return []

    mx = max(seen)

    # Dominio simple y práctico
    if mx <= 10:
        return list(range(0, 10))
    if mx <= 20:
        return list(range(1, 21))
    if mx <= 35:
        return list(range(1, 36))
    if mx <= 40:
        return list(range(1, 41))
    if mx <= 50:
        return list(range(1, 51))
    if mx <= 60:
        return list(range(1, 61))
    if mx <= 70:
        return list(range(1, 71))
    if mx <= 80:
        return list(range(1, 81))

    return list(range(1, mx + 1))


def compute_main_stats(draws: List[Draw], top: int = 10) -> Dict[str, Any]:
    all_main_numbers: List[int] = []

    for draw in draws:
        parsed = split_main_and_bonus(draw)
        all_main_numbers.extend(parsed["main_numbers"])

    frequency_rows = build_frequency_table(all_main_numbers)
    last_seen_map = build_last_seen_map(draws, include_bonus=False)
    domain = infer_number_domain(draws, include_bonus=False)

    hot = frequency_rows[:top]

    cold_pool = []
    for n in domain:
        freq = 0
        last_seen = None

        for row in frequency_rows:
            if row["number"] == n:
                freq = row["frequency"]
                break

        if n in last_seen_map:
            last_seen = last_seen_map[n]

        cold_pool.append({
            "number": n,
            "frequency": freq,
            "last_seen_date": last_seen["draw_date"] if last_seen else None,
            "draws_ago": last_seen["draws_ago"] if last_seen else None,
            "never_seen": last_seen is None,
        })

    cold_pool.sort(
        key=lambda x: (
            x["frequency"],
            -999999 if x["draws_ago"] is None else -x["draws_ago"],
            x["number"],
        )
    )

    cold = cold_pool[:top]

    overdue_pool = []
    for n in domain:
        if n in last_seen_map:
            overdue_pool.append({
                "number": n,
                "draws_ago": last_seen_map[n]["draws_ago"],
                "last_seen_date": last_seen_map[n]["draw_date"],
            })
        else:
            overdue_pool.append({
                "number": n,
                "draws_ago": None,
                "last_seen_date": None,
            })

    overdue_pool.sort(
        key=lambda x: (
            -999999 if x["draws_ago"] is None else -x["draws_ago"],
            x["number"],
        )
    )

    return {
        "total_draws": len(draws),
        "total_numbers_counted": len(all_main_numbers),
        "unique_numbers_seen": len(set(all_main_numbers)),
        "hot_numbers": hot,
        "cold_numbers": cold,
        "most_overdue": overdue_pool[:top],
        "frequency": frequency_rows,
    }


def compute_bonus_stats(draws: List[Draw], top: int = 10) -> Dict[str, Any]:
    bonus_numbers: List[int] = []

    for draw in draws:
        parsed = split_main_and_bonus(draw)
        if parsed["bonus_number"] is not None:
            bonus_numbers.append(parsed["bonus_number"])

    if not bonus_numbers:
        return {
            "has_bonus": False,
            "total_bonus_counted": 0,
            "hot_bonus_numbers": [],
            "cold_bonus_numbers": [],
            "most_overdue_bonus": [],
            "bonus_frequency": [],
        }

    frequency_rows = build_frequency_table(bonus_numbers)
    last_seen_map = build_last_seen_map(draws, include_bonus=True)

    # para bonus usamos dominio basado solo en bonus vistos
    bonus_draw_like = []
    for draw in draws:
        parsed = split_main_and_bonus(draw)
        if parsed["bonus_number"] is not None:
            class Temp:
                pass
            temp = Temp()
            temp.main_numbers = []
            temp.bonus_number = parsed["bonus_number"]
            temp.id = draw.id
            temp.draw_date = draw.draw_date
            temp.draw_type = draw.draw_type
            bonus_draw_like.append(temp)

    domain = infer_number_domain(bonus_draw_like, include_bonus=True)

    hot = frequency_rows[:top]

    cold_pool = []
    for n in domain:
        freq = 0
        last_seen = None

        for row in frequency_rows:
            if row["number"] == n:
                freq = row["frequency"]
                break

        if n in last_seen_map:
            last_seen = last_seen_map[n]

        cold_pool.append({
            "number": n,
            "frequency": freq,
            "last_seen_date": last_seen["draw_date"] if last_seen else None,
            "draws_ago": last_seen["draws_ago"] if last_seen else None,
            "never_seen": last_seen is None,
        })

    cold_pool.sort(
        key=lambda x: (
            x["frequency"],
            -999999 if x["draws_ago"] is None else -x["draws_ago"],
            x["number"],
        )
    )

    overdue_pool = []
    for n in domain:
        if n in last_seen_map:
            overdue_pool.append({
                "number": n,
                "draws_ago": last_seen_map[n]["draws_ago"],
                "last_seen_date": last_seen_map[n]["draw_date"],
            })
        else:
            overdue_pool.append({
                "number": n,
                "draws_ago": None,
                "last_seen_date": None,
            })

    overdue_pool.sort(
        key=lambda x: (
            -999999 if x["draws_ago"] is None else -x["draws_ago"],
            x["number"],
        )
    )

    return {
        "has_bonus": True,
        "total_bonus_counted": len(bonus_numbers),
        "hot_bonus_numbers": hot,
        "cold_bonus_numbers": cold_pool[:top],
        "most_overdue_bonus": overdue_pool[:top],
        "bonus_frequency": frequency_rows,
    }


@router.get("/game/{game_slug}")
def get_stats_by_game(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=50),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para el juego: {game_slug}")

        main_stats = compute_main_stats(draws, top=top)
        bonus_stats = compute_bonus_stats(draws, top=top)

        latest_draw = draws[0]

        return {
            "game_slug": game.slug,
            "game_name": game.name,
            "draws_analyzed": len(draws),
            "latest_draw_date": str(latest_draw.draw_date) if latest_draw.draw_date else None,
            "main_stats": main_stats,
            "bonus_stats": bonus_stats,
        }
    finally:
        close_db(db)


@router.get("/hot/{game_slug}")
def get_hot_numbers(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=50),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para el juego: {game_slug}")

        stats = compute_main_stats(draws, top=top)
        bonus_stats = compute_bonus_stats(draws, top=top)

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "hot_numbers": stats["hot_numbers"],
            "hot_bonus_numbers": bonus_stats["hot_bonus_numbers"] if bonus_stats["has_bonus"] else [],
        }
    finally:
        close_db(db)


@router.get("/cold/{game_slug}")
def get_cold_numbers(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=50),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para el juego: {game_slug}")

        stats = compute_main_stats(draws, top=top)
        bonus_stats = compute_bonus_stats(draws, top=top)

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "cold_numbers": stats["cold_numbers"],
            "cold_bonus_numbers": bonus_stats["cold_bonus_numbers"] if bonus_stats["has_bonus"] else [],
        }
    finally:
        close_db(db)


@router.get("/frequency/{game_slug}")
def get_frequency_table(
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
            raise HTTPException(status_code=404, detail=f"No hay draws para el juego: {game_slug}")

        stats = compute_main_stats(draws, top=10)
        bonus_stats = compute_bonus_stats(draws, top=10)

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "frequency": stats["frequency"],
            "bonus_frequency": bonus_stats["bonus_frequency"] if bonus_stats["has_bonus"] else [],
        }
    finally:
        close_db(db)


@router.get("/overdue/{game_slug}")
def get_overdue_numbers(
    game_slug: str,
    limit_draws: int = Query(default=300, ge=10, le=5000),
    top: int = Query(default=10, ge=1, le=50),
):
    db = get_db()
    try:
        game = get_game_by_slug(db, game_slug)
        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        draws = get_draws_for_game(db, game.id, limit=limit_draws)
        if not draws:
            raise HTTPException(status_code=404, detail=f"No hay draws para el juego: {game_slug}")

        stats = compute_main_stats(draws, top=top)
        bonus_stats = compute_bonus_stats(draws, top=top)

        return {
            "game_slug": game.slug,
            "draws_analyzed": len(draws),
            "most_overdue": stats["most_overdue"][:top],
            "most_overdue_bonus": bonus_stats["most_overdue_bonus"][:top] if bonus_stats["has_bonus"] else [],
        }
    finally:
        close_db(db)