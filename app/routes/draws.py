from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Draw, Game

router = APIRouter(prefix="/draws", tags=["Draws"])


def get_db() -> Session:
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.close()
        raise


def close_db(db: Session):
    try:
        db.close()
    except Exception:
        pass


def draw_to_dict(draw: Draw, game_slug: Optional[str] = None) -> dict:
    return {
        "id": draw.id,
        "game_id": draw.game_id,
        "game_slug": game_slug,
        "draw_date": str(draw.draw_date) if draw.draw_date else None,
        "draw_type": draw.draw_type,
        "draw_time": str(draw.draw_time) if draw.draw_time else None,
        "main_numbers": draw.main_numbers,
        "bonus_number": draw.bonus_number,
        "multiplier": draw.multiplier,
        "jackpot": draw.jackpot,
        "cash_payout": draw.cash_payout,
        "secondary_draws": draw.secondary_draws,
        "notes": draw.notes,
        "source_url": draw.source_url,
        "created_at": str(draw.created_at) if getattr(draw, "created_at", None) else None,
        "updated_at": str(draw.updated_at) if getattr(draw, "updated_at", None) else None,
        "verification_status": getattr(draw, "verification_status", None),
        "confidence_score": getattr(draw, "confidence_score", None),
        "source_provider": getattr(draw, "source_provider", None),
        "needs_review": getattr(draw, "needs_review", None),
    }


@router.get("/latest")
def get_latest_draws(
    state: Optional[str] = Query(default=None, description="Slug del estado, por ejemplo: nj"),
    limit: int = Query(default=100, ge=1, le=500),
):
    db = get_db()
    try:
        games = db.execute(select(Game).where(Game.is_active == True)).scalars().all()
        results = []

        for game in games:
            if state:
                if game.slug in {"powerball", "mega-millions", "millionaire-for-life", "lotto-america", "2by2"}:
                    pass
                elif not game.slug.endswith(f"-{state.lower()}"):
                    continue

            stmt = (
                select(Draw)
                .where(Draw.game_id == game.id)
                .order_by(desc(Draw.draw_date), desc(Draw.id))
                .limit(1)
            )
            draw = db.execute(stmt).scalar_one_or_none()

            if draw:
                results.append(draw_to_dict(draw, game.slug))

        results.sort(
            key=lambda x: (
                x["draw_date"] or "",
                x["game_slug"] or "",
                x["draw_type"] or "",
            ),
            reverse=True,
        )

        return {
            "count": min(len(results), limit),
            "items": results[:limit],
        }
    finally:
        close_db(db)


@router.get("/game/{game_slug}")
def get_draws_by_game(
    game_slug: str,
    limit: int = Query(default=50, ge=1, le=500),
    draw_type: Optional[str] = Query(default=None),
    draw_date: Optional[date] = Query(default=None),
):
    db = get_db()
    try:
        game = db.execute(
            select(Game).where(Game.slug == game_slug.lower())
        ).scalar_one_or_none()

        if not game:
            raise HTTPException(status_code=404, detail=f"Juego no encontrado: {game_slug}")

        stmt = select(Draw).where(Draw.game_id == game.id)

        if draw_type:
            stmt = stmt.where(Draw.draw_type == draw_type)

        if draw_date:
            stmt = stmt.where(Draw.draw_date == draw_date)

        stmt = stmt.order_by(desc(Draw.draw_date), desc(Draw.id)).limit(limit)

        draws = db.execute(stmt).scalars().all()

        return {
            "game_slug": game.slug,
            "count": len(draws),
            "items": [draw_to_dict(d, game.slug) for d in draws],
        }
    finally:
        close_db(db)


@router.get("/state/{state_slug}")
def get_draws_by_state(
    state_slug: str,
    limit: int = Query(default=100, ge=1, le=500),
):
    db = get_db()
    try:
        state_slug = state_slug.lower()

        games = db.execute(
            select(Game).where(Game.is_active == True)
        ).scalars().all()

        matched_games = []
        for game in games:
            if game.slug.endswith(f"-{state_slug}"):
                matched_games.append(game)

        if not matched_games:
            raise HTTPException(status_code=404, detail=f"No se encontraron juegos para el estado: {state_slug}")

        items = []
        for game in matched_games:
            stmt = (
                select(Draw)
                .where(Draw.game_id == game.id)
                .order_by(desc(Draw.draw_date), desc(Draw.id))
                .limit(1)
            )
            draw = db.execute(stmt).scalar_one_or_none()
            if draw:
                items.append(draw_to_dict(draw, game.slug))

        items.sort(
            key=lambda x: (
                x["draw_date"] or "",
                x["game_slug"] or "",
                x["draw_type"] or "",
            ),
            reverse=True,
        )

        return {
            "state_slug": state_slug,
            "count": min(len(items), limit),
            "items": items[:limit],
        }
    finally:
        close_db(db)


@router.get("/date/{draw_date}")
def get_draws_by_date(
    draw_date: date,
    limit: int = Query(default=500, ge=1, le=2000),
):
    db = get_db()
    try:
        stmt = (
            select(Draw, Game.slug)
            .join(Game, Draw.game_id == Game.id)
            .where(Draw.draw_date == draw_date)
            .order_by(Game.slug, Draw.draw_type, desc(Draw.id))
            .limit(limit)
        )

        rows = db.execute(stmt).all()

        items = []
        for draw, game_slug in rows:
            items.append(draw_to_dict(draw, game_slug))

        return {
            "draw_date": str(draw_date),
            "count": len(items),
            "items": items,
        }
    finally:
        close_db(db)


@router.get("/search")
def search_draws(
    game_slug: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    draw_type: Optional[str] = Query(default=None),
    draw_date: Optional[date] = Query(default=None),
    needs_review: Optional[bool] = Query(default=None),
    verification_status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
):
    db = get_db()
    try:
        stmt = select(Draw, Game.slug).join(Game, Draw.game_id == Game.id)

        if game_slug:
            stmt = stmt.where(Game.slug == game_slug.lower())

        if state:
            stmt = stmt.where(Game.slug.like(f"%-{state.lower()}"))

        if draw_type:
            stmt = stmt.where(Draw.draw_type == draw_type)

        if draw_date:
            stmt = stmt.where(Draw.draw_date == draw_date)

        if needs_review is not None and hasattr(Draw, "needs_review"):
            stmt = stmt.where(Draw.needs_review == needs_review)

        if verification_status and hasattr(Draw, "verification_status"):
            stmt = stmt.where(Draw.verification_status == verification_status)

        stmt = stmt.order_by(desc(Draw.draw_date), desc(Draw.id)).limit(limit)

        rows = db.execute(stmt).all()

        items = []
        for draw, slug in rows:
            items.append(draw_to_dict(draw, slug))

        return {
            "count": len(items),
            "items": items,
        }
    finally:
        close_db(db)


@router.get("/{draw_id}")
def get_draw_by_id(draw_id: int):
    db = get_db()
    try:
        stmt = (
            select(Draw, Game.slug)
            .join(Game, Draw.game_id == Game.id)
            .where(Draw.id == draw_id)
        )
        row = db.execute(stmt).first()

        if not row:
            raise HTTPException(status_code=404, detail=f"Draw no encontrado: {draw_id}")

        draw, game_slug = row
        return draw_to_dict(draw, game_slug)
    finally:
        close_db(db)