from sqlalchemy import select
from app.database import SessionLocal
from app.models import Draw, Game

db = SessionLocal()

try:
    rows = db.execute(
        select(Draw, Game)
        .join(Game, Game.id == Draw.game_id)
        .where(Game.slug == "powerball")
        .order_by(Draw.draw_date.desc(), Draw.id.desc())
        .limit(5)
    ).all()

    for draw, game in rows:
        print("=" * 60)
        print("GAME:", game.slug)
        print("DATE:", draw.draw_date)
        print("TYPE:", draw.draw_type)
        print("NUMBERS:", draw.main_numbers)
        print("BONUS:", draw.bonus_number)
        print("MULTIPLIER:", draw.multiplier)
        print("JACKPOT:", draw.jackpot)
        print("SOURCE:", draw.source_url)

finally:
    db.close()