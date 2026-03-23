from collections import defaultdict

from sqlalchemy import select

from app.database import SessionLocal
from app.models import Draw, Game, GameSource
from app.utils.game_normalizer import canonical_game_info


def pick_better_value(current, incoming):
    if current in (None, "", [], {}):
        return incoming
    return current


def merge_draw_data(survivor_draw: Draw, duplicate_draw: Draw):
    survivor_draw.main_numbers = pick_better_value(survivor_draw.main_numbers, duplicate_draw.main_numbers)
    survivor_draw.bonus_number = pick_better_value(survivor_draw.bonus_number, duplicate_draw.bonus_number)
    survivor_draw.multiplier = pick_better_value(survivor_draw.multiplier, duplicate_draw.multiplier)
    survivor_draw.draw_time = pick_better_value(survivor_draw.draw_time, duplicate_draw.draw_time)
    survivor_draw.jackpot = pick_better_value(survivor_draw.jackpot, duplicate_draw.jackpot)
    survivor_draw.cash_payout = pick_better_value(survivor_draw.cash_payout, duplicate_draw.cash_payout)
    survivor_draw.secondary_draws = pick_better_value(survivor_draw.secondary_draws, duplicate_draw.secondary_draws)
    survivor_draw.notes = pick_better_value(survivor_draw.notes, duplicate_draw.notes)
    survivor_draw.source_url = pick_better_value(survivor_draw.source_url, duplicate_draw.source_url)

    if hasattr(survivor_draw, "raw_payload") and hasattr(duplicate_draw, "raw_payload"):
        survivor_draw.raw_payload = pick_better_value(
            getattr(survivor_draw, "raw_payload", None),
            getattr(duplicate_draw, "raw_payload", None),
        )

    if hasattr(survivor_draw, "source_provider") and hasattr(duplicate_draw, "source_provider"):
        survivor_draw.source_provider = pick_better_value(
            getattr(survivor_draw, "source_provider", None),
            getattr(duplicate_draw, "source_provider", None),
        )

    if hasattr(survivor_draw, "verification_status") and hasattr(duplicate_draw, "verification_status"):
        survivor_draw.verification_status = pick_better_value(
            getattr(survivor_draw, "verification_status", None),
            getattr(duplicate_draw, "verification_status", None),
        )

    if hasattr(survivor_draw, "confidence_score") and hasattr(duplicate_draw, "confidence_score"):
        survivor_draw.confidence_score = pick_better_value(
            getattr(survivor_draw, "confidence_score", None),
            getattr(duplicate_draw, "confidence_score", None),
        )

    if hasattr(survivor_draw, "needs_review") and hasattr(duplicate_draw, "needs_review"):
        survivor_draw.needs_review = pick_better_value(
            getattr(survivor_draw, "needs_review", None),
            getattr(duplicate_draw, "needs_review", None),
        )


def merge_game_sources(db, survivor_game_id: int, duplicate_game_id: int):
    survivor_sources = db.execute(
        select(GameSource).where(GameSource.game_id == survivor_game_id)
    ).scalars().all()

    survivor_urls = set()
    for s in survivor_sources:
        if s.source_url:
            survivor_urls.add(s.source_url.strip())

    duplicate_sources = db.execute(
        select(GameSource).where(GameSource.game_id == duplicate_game_id)
    ).scalars().all()

    moved = 0
    deleted = 0

    for src in duplicate_sources:
        src_url = (src.source_url or "").strip()

        if src_url and src_url in survivor_urls:
            db.delete(src)
            deleted += 1
            continue

        src.game_id = survivor_game_id
        if src_url:
            survivor_urls.add(src_url)
        moved += 1

    return moved, deleted


def choose_survivor(items, final_slug: str):
    """
    Prioridad:
    1) juego que YA tenga el slug final
    2) si no, el de menor ID
    """
    exact_slug_matches = []
    for game, info in items:
        if (game.slug or "").lower() == final_slug.lower():
            exact_slug_matches.append((game, info))

    if exact_slug_matches:
        exact_slug_matches.sort(key=lambda x: x[0].id)
        return exact_slug_matches[0]

    items = sorted(items, key=lambda x: x[0].id)
    return items[0]


def main():
    db = SessionLocal()

    try:
        games = db.execute(
            select(Game).order_by(Game.id.asc())
        ).scalars().all()

        grouped = defaultdict(list)

        print("=" * 90)
        print("ANALYZING CURRENT GAMES")
        print("=" * 90)

        for game in games:
            state_code = ""
            if game.slug and "-" in game.slug:
                maybe_state = game.slug.split("-")[-1]
                if len(maybe_state) in (2, 3):
                    state_code = maybe_state.lower()

            info = canonical_game_info(game.name, state_code=state_code)
            final_slug = info["final_slug"]

            grouped[final_slug].append((game, info))

            print(
                f"GAME ID {game.id} | old_name={game.name} | old_slug={game.slug} "
                f"-> canonical_name={info['canonical_name']} | canonical_slug={final_slug}"
            )

        print()
        print("=" * 90)
        print("MERGING DUPLICATES")
        print("=" * 90)

        merged_games_count = 0
        renamed_count = 0
        moved_draws_count = 0
        merged_draw_collisions_count = 0
        moved_sources_count = 0
        deleted_sources_count = 0

        for final_slug, items in grouped.items():
            survivor_game, survivor_info = choose_survivor(items, final_slug)

            # duplicates = todos menos survivor
            duplicates = [(g, i) for g, i in items if g.id != survivor_game.id]

            # solo renombrar survivor si NO existe otro con ese slug dentro del grupo
            # como choose_survivor prioriza el exact match, aquí ya no debe chocar
            changed = False
            if survivor_game.name != survivor_info["canonical_name"]:
                survivor_game.name = survivor_info["canonical_name"]
                changed = True

            if (survivor_game.slug or "").lower() != final_slug.lower():
                survivor_game.slug = final_slug.lower()
                changed = True

            if changed:
                db.flush()
                renamed_count += 1
                print(
                    f"RENAMED survivor ID {survivor_game.id} -> "
                    f"{survivor_game.name} | {survivor_game.slug}"
                )

            if not duplicates:
                continue

            print(f"\nMERGE GROUP: {final_slug}")
            print(f"Survivor: ID {survivor_game.id}")

            for duplicate_game, duplicate_info in duplicates:
                print(
                    f"  DUPLICATE -> ID {duplicate_game.id} | "
                    f"{duplicate_game.name} | {duplicate_game.slug}"
                )

                duplicate_draws = db.execute(
                    select(Draw).where(Draw.game_id == duplicate_game.id)
                ).scalars().all()

                for d in duplicate_draws:
                    existing_survivor_draw = db.execute(
                        select(Draw).where(
                            Draw.game_id == survivor_game.id,
                            Draw.draw_date == d.draw_date,
                            Draw.draw_type == d.draw_type,
                        )
                    ).scalar_one_or_none()

                    if existing_survivor_draw:
                        merge_draw_data(existing_survivor_draw, d)
                        db.delete(d)
                        merged_draw_collisions_count += 1
                    else:
                        d.game_id = survivor_game.id
                        moved_draws_count += 1

                moved, deleted = merge_game_sources(
                    db=db,
                    survivor_game_id=survivor_game.id,
                    duplicate_game_id=duplicate_game.id,
                )
                moved_sources_count += moved
                deleted_sources_count += deleted

                db.flush()
                db.delete(duplicate_game)
                merged_games_count += 1

        db.commit()

        print()
        print("=" * 90)
        print("DONE")
        print("=" * 90)
        print(f"Renamed survivors: {renamed_count}")
        print(f"Merged duplicate games: {merged_games_count}")
        print(f"Moved draws: {moved_draws_count}")
        print(f"Merged colliding draws: {merged_draw_collisions_count}")
        print(f"Moved sources: {moved_sources_count}")
        print(f"Deleted duplicate sources: {deleted_sources_count}")

    except Exception as e:
        db.rollback()
        print("ERROR:", str(e))
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()