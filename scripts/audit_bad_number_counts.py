import json
from collections import Counter, defaultdict

from sqlalchemy import select

from app.database import SessionLocal
from app.models import Draw, Game


GAME_RULES = {
    "all-or-nothing-wi": {"main_count": 11, "bonus_count": 0},

    "loto-cash-pr": {"main_count": 5, "bonus_count": 1},
    "revancha-pr": {"main_count": 5, "bonus_count": 1},

    "megabucks-me": {"main_count": 5, "bonus_count": 1},
    "megabucks-nh": {"main_count": 5, "bonus_count": 1},
    "megabucks-vt": {"main_count": 5, "bonus_count": 1},

    "pick-10-ny": {"main_count": 20, "bonus_count": 0},
    "quick-draw-in": {"main_count": 20, "bonus_count": 0},
    "keno-mi": {"main_count": 22, "bonus_count": 0},
}


def get_expected_actual_counts(draw, game_slug: str):
    rule = GAME_RULES.get(game_slug)
    if not rule:
        return None, None

    main_numbers = draw.main_numbers or []
    bonus_number = getattr(draw, "bonus_number", None)

    expected = rule["main_count"] + rule.get("bonus_count", 0)

    actual = len(main_numbers)
    if rule.get("bonus_count", 0) > 0 and bonus_number is not None:
        actual += 1

    return expected, actual


def main():
    db = SessionLocal()
    bad_rows = []

    try:
        games = db.execute(
            select(Game).where(Game.slug.in_(list(GAME_RULES.keys())))
        ).scalars().all()

        games_by_id = {g.id: g for g in games}

        if not games_by_id:
            print("=" * 90)
            print("AUDIT BAD NUMBER COUNTS COMPLETED")
            print("=" * 90)
            print("No matching games found in DB for current GAME_RULES.")
            return

        draws = db.execute(
            select(Draw).where(Draw.game_id.in_(list(games_by_id.keys())))
        ).scalars().all()

        for draw in draws:
            game = games_by_id.get(draw.game_id)
            if not game:
                continue

            expected, actual = get_expected_actual_counts(draw, game.slug)
            if expected is None:
                continue

            if expected != actual:
                bad_rows.append({
                    "game_slug": game.slug,
                    "draw_date": str(draw.draw_date),
                    "draw_type": draw.draw_type,
                    "expected_count": expected,
                    "actual_count": actual,
                    "main_numbers": draw.main_numbers,
                    "bonus_number": getattr(draw, "bonus_number", None),
                    "source_url": getattr(draw, "source_url", None),
                })

        grouped = defaultdict(lambda: {
            "count": 0,
            "examples": [],
        })

        for row in bad_rows:
            key = f"{row['game_slug']}|expected={row['expected_count']}|actual={row['actual_count']}"
            grouped[key]["count"] += 1
            if len(grouped[key]["examples"]) < 3:
                grouped[key]["examples"].append({
                    "draw_date": row["draw_date"],
                    "draw_type": row["draw_type"],
                    "main_numbers": row["main_numbers"],
                    "bonus_number": row["bonus_number"],
                })

        grouped_sorted = sorted(
            [{"key": k, **v} for k, v in grouped.items()],
            key=lambda x: x["count"],
            reverse=True,
        )

        with open("audit_bad_number_counts_report.json", "w", encoding="utf-8") as f:
            json.dump({
                "total_bad_rows": len(bad_rows),
                "total_grouped_issues": len(grouped_sorted),
                "bad_rows": bad_rows,
                "grouped_issues": grouped_sorted,
            }, f, ensure_ascii=False, indent=2)

        with open("audit_bad_number_counts_report.txt", "w", encoding="utf-8") as f:
            f.write("=" * 90 + "\n")
            f.write("AUDIT BAD NUMBER COUNTS\n")
            f.write("=" * 90 + "\n")
            f.write(f"Total bad rows: {len(bad_rows)}\n")
            f.write(f"Total grouped issues: {len(grouped_sorted)}\n")
            f.write("JSON report: audit_bad_number_counts_report.json\n")
            f.write("TXT report: audit_bad_number_counts_report.txt\n\n")

            f.write("TOP 20 GROUPED ISSUES\n")
            f.write("-" * 90 + "\n")
            for item in grouped_sorted[:20]:
                f.write(f"{item['key']} | count={item['count']}\n")

            f.write("\nDETAILED ROWS\n")
            f.write("-" * 90 + "\n")
            for row in bad_rows:
                f.write(
                    f"{row['game_slug']} | {row['draw_date']} | {row['draw_type']} | "
                    f"expected={row['expected_count']} | actual={row['actual_count']} | "
                    f"main={row['main_numbers']} | bonus={row['bonus_number']}\n"
                )

        print("=" * 90)
        print("AUDIT BAD NUMBER COUNTS COMPLETED")
        print("=" * 90)
        print(f"Total bad rows: {len(bad_rows)}")
        print(f"Total grouped issues: {len(grouped_sorted)}")
        print("JSON report: audit_bad_number_counts_report.json")
        print("TXT report: audit_bad_number_counts_report.txt")
        print("\nTOP 20 GROUPED ISSUES")
        print("-" * 90)
        for item in grouped_sorted[:20]:
            print(f"{item['key']} | count={item['count']}")

    finally:
        db.close()


if __name__ == "__main__":
    main()