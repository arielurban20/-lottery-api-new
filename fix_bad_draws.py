from sqlalchemy import select
from app.database import SessionLocal
from app.models import Draw, Game

BAD_RULES = {
    "mass-cash-ma":      5,
    "lucky-lotto-il":    5,
    "dc-5-dc":           5,
    "treasure-hunt-pa":  5,
    "all-or-nothing-tx": 12,
    "all-or-nothing-wi": 12,
    "2by2":              4,
    "megabucks-me":      6,
    "megabucks-nh":      6,
    "megabucks-vt":      6,
    "pick-10-ny":        20,
    "quick-draw-in":     20,
    "keno-mi":           22,
    "pega-2-pr":         2,
    "pick-2-fl":         2,
    "pick-2-pa":         2,
    "loto-cash-pr":      6,
    "revancha-pr":       6,
    "myday-ne":          3,
}

def main():
    db = SessionLocal()
    deleted = 0

    try:
        for slug, expected in BAD_RULES.items():
            game = db.execute(
                select(Game).where(Game.slug == slug)
            ).scalar_one_or_none()

            if not game:
                print(f"SKIP (no existe en DB): {slug}")
                continue

            draws = db.execute(
                select(Draw).where(Draw.game_id == game.id)
            ).scalars().all()

            for draw in draws:
                nums = draw.main_numbers or []
                if isinstance(nums, list) and len(nums) != expected:
                    print(
                        f"DELETE: {slug} | id={draw.id} | "
                        f"fecha={draw.draw_date} | "
                        f"tenia={len(nums)} esperaba={expected} | "
                        f"numeros={nums}"
                    )
                    db.delete(draw)
                    deleted += 1

        db.commit()
        print(f"\nLISTO. Se borraron {deleted} draws malos.")
        print("Ahora corre scrape_all_states_dom_v2.py para repoblar.")

    except Exception as e:
        db.rollback()
        print(f"ERROR: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()