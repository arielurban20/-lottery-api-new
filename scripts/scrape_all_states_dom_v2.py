import json
import re
from datetime import datetime

from playwright.sync_api import sync_playwright
from sqlalchemy import select, text

from app.database import SessionLocal
from app.models import Draw, Game
from app.utils.game_normalizer import (
    canonical_game_info,
    split_main_and_bonus,
    GAME_RULES_BY_FINAL_SLUG,
    GAME_RULES_BY_CANONICAL_SLUG,
)


MULTI_STATE_FINAL_SLUGS = {
    "powerball",
    "powerball-double-play",
    "mega-millions",
    "millionaire-for-life",
    "lotto-america",
    "2by2",
}


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_date(text: str):
    try:
        return datetime.strptime(clean(text), "%A, %B %d, %Y").date()
    except Exception:
        return None


def get_states():
    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id, name, slug, source_url
                FROM states
                WHERE is_active = true
                ORDER BY name
            """)
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def get_games():
    db = SessionLocal()
    try:
        games = db.execute(select(Game).where(Game.is_active == True)).scalars().all()
        return {g.slug.lower(): g for g in games}
    finally:
        db.close()


def _extract_numbers_from_node(node) -> list[int]:
    nums = []

    li_items = node.locator("li")
    if li_items.count() > 0:
        for j in range(li_items.count()):
            try:
                txt = clean(li_items.nth(j).inner_text())
                if re.fullmatch(r"\d+", txt):
                    nums.append(int(txt))
            except Exception:
                pass
        return nums

    try:
        text_block = clean(node.inner_text())
        found = re.findall(r"\b\d+\b", text_block)
        nums = [int(x) for x in found]
    except Exception:
        nums = []

    return nums


def extract_primary_number_list(section, parser_type: str = "standard", final_slug: str = "") -> list[int]:
    """
    standard:
      toma el primer bloque real

    many-numbers / 2by2:
      combina todos los ul.resultsnums del section
    """
    if parser_type in {"many-numbers", "2by2"} or final_slug in {
        "2by2",
        "pick-10-ny",
        "quick-draw-in",
        "daily-keno-wa",
        "keno-mi",
        "all-or-nothing-tx",
        "all-or-nothing-wi",
    }:
        all_nums = []
        loc = section.locator("ul.resultsnums")
        for i in range(loc.count()):
            nums = _extract_numbers_from_node(loc.nth(i))
            if nums:
                all_nums.extend(nums)

        if all_nums:
            return all_nums

    candidates = [
        "ul.resultsnums",
        "div.resultsnumsrow ul.resultsnums",
        "div.resultsnumsrow",
    ]

    for selector in candidates:
        loc = section.locator(selector)
        if loc.count() == 0:
            continue

        for i in range(loc.count()):
            nums = _extract_numbers_from_node(loc.nth(i))
            if nums:
                return nums

    return []


def extract_text_extras(section) -> dict:
    def safe_text(locator) -> str:
        try:
            if locator.count() > 0:
                texts = []
                for i in range(locator.count()):
                    try:
                        txt = clean(locator.nth(i).inner_text())
                        if txt:
                            texts.append(txt)
                    except Exception:
                        pass
                return " | ".join(texts)
        except Exception:
            pass
        return ""

    full_text = clean(section.inner_text())

    extra_chunks = [
        safe_text(section.locator(".jackpot")),
        safe_text(section.locator(".nextdraw")),
        safe_text(section.locator(".nextdrawing")),
        safe_text(section.locator(".resultsnext")),
        safe_text(section.locator(".drawinfo")),
        safe_text(section.locator(".gameinfo")),
        safe_text(section.locator(".prize")),
        safe_text(section.locator(".panel")),
        safe_text(section.locator("p")),
        safe_text(section.locator("small")),
        safe_text(section.locator("strong")),
        safe_text(section.locator("span")),
    ]

    combined_text = " | ".join([full_text] + [x for x in extra_chunks if x])
    combined_text = clean(combined_text)

    data = {
        "bonus_number": None,
        "multiplier": None,
        "jackpot": None,
        "jackpot_change": None,
        "next_draw_text": None,
        "next_draw_timezone": None,
        "next_draw_relative": None,
        "full_text": combined_text,
    }

    bonus_patterns = [
        r"Cash Ball[:\s]+(\d{1,2})",
        r"Star Ball[:\s]+(\d{1,2})",
        r"Powerball[:\s]+(\d{1,2})",
        r"Mega Ball[:\s]+(\d{1,2})",
        r"Mega Number[:\s]+(\d{1,2})",
        r"Millionaire Ball[:\s]+(\d{1,2})",
        r"Bullseye[:\s]+(\d{1,2})",
        r"Fireball[:\s]+(\d{1,2})",
        r"Wild Money[:\s]+(\d{1,2})",
        r"Kicker[:\s]+(\d+)",
        r"Cash Ball 225[:\s]+(\d{1,2})",
        r"Bonus Ball[:\s]+(\d{1,2})",
        r"Bonus Number[:\s]+(\d{1,2})",
        r"Bolo Cash[:\s]+(\d{1,2})",
        r"Megaball[:\s]+(\d{1,2})",
    ]

    mult_patterns = [
        r"All Star Bonus[:\s]+([Xx]?\d+)",
        r"Power Play[:\s]+([Xx]?\d+)",
        r"Megaplier[:\s]+([Xx]?\d+)",
        r"Multiplier[:\s]+([Xx]?\d+)",
        r"Plus[:\s]+([Xx]?\d+)",
        r"Multiplicador[:\s]+([Xx]?\d+)",
    ]

    jackpot_patterns = [
        r"Next Jackpot[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Estimated Jackpot[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Estimated Grand Prize[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Grand Prize[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Top Prize[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Cash Value[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Annuity[:\s]+(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    jackpot_change_patterns = [
        r"Change from last[:\s]+([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot Change[:\s]+([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Change[:\s]+([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    next_draw_patterns = [
        r"Next Drawing[:\s]+([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing[:\s]+([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing[:\s]+([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4})",
        r"Next Draw[:\s]+([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Draw[:\s]+([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
    ]

    timezone_patterns = [
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+Time\s*\(GMT[^\)]*\))",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+Time\s*\(UTC[^\)]*\))",
        r"\b(Eastern Time|Central Time|Mountain Time|Pacific Time)\b",
        r"\b(ET|CT|MT|PT)\b",
    ]

    relative_patterns = [
        r"(\d+\s+(?:minute|minutes|hour|hours|day|days)\s+from\s+now)",
        r"(in\s+\d+\s*(?:m|h|d))",
        r"(in\s+\d+\s+(?:minutes|hours|days))",
    ]

    for pattern in bonus_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["bonus_number"] = m.group(1).strip()
            break

    for pattern in mult_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            value = m.group(1).strip().upper()
            data["multiplier"] = value if value.startswith("X") else f"X{value}"
            break

    for pattern in jackpot_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["jackpot"] = m.group(1).strip()
            break

    for pattern in jackpot_change_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["jackpot_change"] = m.group(1).strip()
            break

    for pattern in next_draw_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["next_draw_text"] = clean(m.group(1))
            break

    for pattern in timezone_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["next_draw_timezone"] = clean(m.group(1))
            break

    for pattern in relative_patterns:
        m = re.search(pattern, combined_text, re.IGNORECASE)
        if m:
            data["next_draw_relative"] = clean(m.group(1))
            break

    return data


def extract_page_level_extras(full_page_text: str, title: str) -> dict:
    page_text = clean(full_page_text)
    title_clean = clean(title)

    data = {
        "jackpot": None,
        "jackpot_change": None,
        "next_draw_text": None,
        "next_draw_timezone": None,
        "next_draw_relative": None,
    }

    # títulos conocidos para cortar el bloque antes del siguiente juego
    known_titles = [
        "Powerball Double Play",
        "Powerball",
        "Mega Millions",
        "Millionaire for Life",
        "2by2",
        "Lotto America",
        "Cowboy Draw",
        "Cash Pop",
        "Fantasy 5",
        "Pick 2",
        "Pick 3",
        "Pick 4",
        "Pick 5",
        "Take 5",
        "Numbers",
        "Win 4",
        "Lotto",
        "Daily 3",
        "Daily 4",
        "Daily Derby",
        "SuperLotto Plus",
        "Jackpot Triple Play",
        "DC-3",
        "DC-4",
        "DC-5",
        "Pega 2",
        "Pega 3",
        "Pega 4",
        "Revancha",
        "Loto Cash",
        "Loteria Tradicional",
    ]

    # Buscar todas las ocurrencias del título y usar la que realmente pertenece
    # al bloque del juego, no al encabezado descriptivo de la página.
    matches = list(re.finditer(re.escape(title_clean), page_text, re.IGNORECASE))
    if not matches:
        return data

    best_chunk = None

    for match in matches:
        start = match.start()
        tail = page_text[start:start + 1800]

        # Este filtro obliga a que sea un bloque real del juego:
        # debe contener fecha, Prizes/Odds o Speak, Past Results o Next Drawing.
        looks_like_game_block = (
            re.search(r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}", tail, re.IGNORECASE)
            or "Prizes/Odds" in tail
            or "Speak" in tail
            or "Past Results" in tail
            or "Next Drawing:" in tail
        )

        if not looks_like_game_block:
            continue

        next_positions = []
        for other_title in known_titles:
            other_title_clean = clean(other_title)
            if other_title_clean.lower() == title_clean.lower():
                continue

            other_match = re.search(re.escape(other_title_clean), tail, re.IGNORECASE)
            if other_match and other_match.start() > 0:
                next_positions.append(other_match.start())

        end = min(next_positions) if next_positions else min(len(tail), 1800)
        chunk = clean(tail[:end])

        if "Next Drawing:" in chunk or "Next Jackpot:" in chunk or "Past Results" in chunk:
            best_chunk = chunk
            break

        if best_chunk is None:
            best_chunk = chunk

    if not best_chunk:
        return data

    chunk = best_chunk

    next_draw_patterns = [
        r"Next Drawing:\s*([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing:\s*([A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s*(?:am|pm))",
        r"Next Drawing:\s*([A-Za-z]{3},\s*[A-Za-z]{3}\s+\d{1,2},\s+\d{4})",
    ]

    jackpot_patterns = [
        r"Next Jackpot:\s*(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Estimated Jackpot:\s*(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot:\s*(\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    jackpot_change_patterns = [
        r"Change from last:\s*([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
        r"Jackpot Change:\s*([+\-]?\$[0-9,\.]+(?:\s*(?:Million|Billion|Thousand))?)",
    ]

    timezone_patterns = [
        r"(Eastern Time\s*\(GMT[^\)]*\))",
        r"(Central Time\s*\(GMT[^\)]*\))",
        r"(Mountain Time\s*\(GMT[^\)]*\))",
        r"(Pacific Time\s*\(GMT[^\)]*\))",
        r"\b(Eastern Time|Central Time|Mountain Time|Pacific Time)\b",
    ]

    relative_patterns = [
        r"(\d+\s+(?:minute|minutes|hour|hours|day|days)\s+from\s+now)",
        r"(in\s+\d+\s+(?:minutes|hours|days))",
    ]

    for pattern in next_draw_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["next_draw_text"] = clean(m.group(1))
            break

    for pattern in jackpot_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["jackpot"] = clean(m.group(1))
            break

    for pattern in jackpot_change_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["jackpot_change"] = clean(m.group(1))
            break

    for pattern in timezone_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["next_draw_timezone"] = clean(m.group(1))
            break

    for pattern in relative_patterns:
        m = re.search(pattern, chunk, re.IGNORECASE)
        if m:
            data["next_draw_relative"] = clean(m.group(1))
            break

    return data


def validate_entry(
    final_slug: str,
    canonical_slug: str,
    main_numbers: list[int],
    bonus_number=None,
) -> bool:
    if not main_numbers:
        return False

    rule = GAME_RULES_BY_FINAL_SLUG.get(final_slug)

    if not rule:
        rule = GAME_RULES_BY_CANONICAL_SLUG.get(canonical_slug)

    if not rule:
        return len(main_numbers) > 0

    expected_main = rule.get("main", 0)
    expected_bonus = rule.get("bonus", 0)

    if len(main_numbers) != expected_main:
        return False

    has_bonus = bonus_number is not None and str(bonus_number).strip() != ""

    if expected_bonus > 0 and not has_bonus:
        return False

    if expected_bonus == 0 and has_bonus:
        return False

    if final_slug == "myday-ne":
        if len(main_numbers) != 3:
            return False

        month, day, year = main_numbers

        if not (1 <= month <= 12):
            return False

        if not (0 <= year <= 99):
            return False

        days_in_month = {
            1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
        }

        if not (1 <= day <= days_in_month[month]):
            return False

    return True


def save_draw(
    game: Game,
    draw_date,
    draw_type: str,
    main_numbers: list[int],
    bonus_number,
    multiplier,
    jackpot,
    jackpot_change,
    next_draw_text,
    next_draw_timezone,
    next_draw_relative,
    source_url: str,
    raw_payload: dict,
    notes: str,
):
    db = SessionLocal()
    try:
        existing = db.execute(
            select(Draw).where(
                Draw.game_id == game.id,
                Draw.draw_date == draw_date,
                Draw.draw_type == draw_type,
            )
        ).scalar_one_or_none()

        if existing:
            existing.main_numbers = main_numbers
            existing.bonus_number = bonus_number
            existing.multiplier = multiplier
            existing.jackpot = jackpot
            existing.source_url = source_url
            existing.notes = notes

            if hasattr(existing, "jackpot_change"):
                existing.jackpot_change = jackpot_change
            if hasattr(existing, "next_draw_text"):
                existing.next_draw_text = next_draw_text
            if hasattr(existing, "next_draw_timezone"):
                existing.next_draw_timezone = next_draw_timezone
            if hasattr(existing, "next_draw_relative"):
                existing.next_draw_relative = next_draw_relative

            if hasattr(existing, "raw_payload"):
                existing.raw_payload = raw_payload
            if hasattr(existing, "source_provider"):
                existing.source_provider = "Lottery Post"
            if hasattr(existing, "verification_status"):
                existing.verification_status = "verified"
            if hasattr(existing, "confidence_score"):
                existing.confidence_score = 90
            if hasattr(existing, "needs_review"):
                existing.needs_review = False

            db.commit()
            return "updated"

        row = Draw(
            game_id=game.id,
            draw_date=draw_date,
            draw_type=draw_type,
            draw_time=None,
            main_numbers=main_numbers,
            bonus_number=bonus_number,
            multiplier=multiplier,
            jackpot=jackpot,
            cash_payout=None,
            secondary_draws=None,
            notes=notes,
            source_url=source_url,
        )

        if hasattr(row, "jackpot_change"):
            row.jackpot_change = jackpot_change
        if hasattr(row, "next_draw_text"):
            row.next_draw_text = next_draw_text
        if hasattr(row, "next_draw_timezone"):
            row.next_draw_timezone = next_draw_timezone
        if hasattr(row, "next_draw_relative"):
            row.next_draw_relative = next_draw_relative

        if hasattr(row, "raw_payload"):
            row.raw_payload = raw_payload
        if hasattr(row, "source_provider"):
            row.source_provider = "Lottery Post"
        if hasattr(row, "verification_status"):
            row.verification_status = "verified"
        if hasattr(row, "confidence_score"):
            row.confidence_score = 90
        if hasattr(row, "needs_review"):
            row.needs_review = False

        db.add(row)
        db.commit()
        return "created"
    finally:
        db.close()


def get_or_create_game_in_db(
    final_slug: str,
    canonical_name: str,
    games_by_slug: dict[str, Game],
):
    game = games_by_slug.get(final_slug.lower())
    if game:
        return game, False

    db = SessionLocal()
    try:
        game = db.execute(
            select(Game).where(Game.slug == final_slug.lower())
        ).scalar_one_or_none()

        if game:
            changed = False
            if game.name != canonical_name:
                game.name = canonical_name
                changed = True

            if changed:
                db.commit()
                db.refresh(game)

            games_by_slug[game.slug.lower()] = game
            return game, False

        game = Game(
            name=canonical_name,
            slug=final_slug.lower(),
            is_active=True,
        )
        db.add(game)
        db.commit()
        db.refresh(game)

        games_by_slug[game.slug.lower()] = game
        return game, True
    finally:
        db.close()


def scrape_state(page, state: dict, games_by_slug: dict[str, Game]):
    page.goto(state["source_url"], wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(5000)
    full_page_text = clean(page.locator("body").inner_text())

    sections = page.locator("section")
    results = []

    for i in range(sections.count()):
        section = sections.nth(i)

        if section.locator("h2").count() == 0:
            continue

        try:
            title = clean(section.locator("h2").first.inner_text())
        except Exception:
            continue

        if not title:
            continue

        draw_date = None
        if section.locator("time").count() > 0:
            try:
                draw_date = parse_date(section.locator("time").first.inner_text())
            except Exception:
                pass

        if not draw_date:
            continue

        info = canonical_game_info(title, state_code=state["slug"])

        canonical_name = info["canonical_name"]
        canonical_slug = info["canonical_slug"]
        final_slug = info["final_slug"]
        draw_type = info["draw_type"]
        parser_type = info["parser_type"]

        raw_numbers = extract_primary_number_list(
            section,
            parser_type=parser_type,
            final_slug=final_slug,
        )

        extras = extract_text_extras(section)
        page_extras = extract_page_level_extras(full_page_text, title)
       
        if final_slug in {"mega-millions", "powerball", "powerball-double-play"}:
         print("\nDEBUG EXTRAS")
         print("STATE:", state["slug"])
         print("TITLE:", title)
         print("FINAL SLUG:", final_slug)
         print("PAGE EXTRAS:", page_extras)

        bonus_number = extras["bonus_number"]
        multiplier = extras["multiplier"]
        debug_text = extras["full_text"]

        jackpot = extras["jackpot"] or page_extras["jackpot"]
        jackpot_change = extras["jackpot_change"] or page_extras["jackpot_change"]
        next_draw_text = extras["next_draw_text"] or page_extras["next_draw_text"]
        next_draw_timezone = extras["next_draw_timezone"] or page_extras["next_draw_timezone"]
        next_draw_relative = extras["next_draw_relative"] or page_extras["next_draw_relative"]

        parts = split_main_and_bonus(
            game_slug=final_slug,
            raw_numbers=raw_numbers,
            bonus_number=bonus_number,
            multiplier=multiplier,
        )

        main_numbers = parts["main_numbers"]
        final_bonus_number = parts["bonus_number"]
        final_multiplier = parts["multiplier"]

        payload = {
            "title": title,
            "state_slug": state["slug"],
            "canonical_name": canonical_name,
            "canonical_slug": canonical_slug,
            "final_slug": final_slug,
            "draw_type": draw_type,
            "parser_type": parser_type,
            "raw_numbers": raw_numbers,
            "main_numbers": main_numbers,
            "bonus_number": final_bonus_number,
            "multiplier": final_multiplier,
            "jackpot": jackpot,
            "jackpot_change": jackpot_change,
            "next_draw_text": next_draw_text,
            "next_draw_timezone": next_draw_timezone,
            "next_draw_relative": next_draw_relative,
            "debug_text": debug_text[:2000],
        }

        if not validate_entry(
            final_slug=final_slug,
            canonical_slug=canonical_slug,
            main_numbers=main_numbers,
            bonus_number=final_bonus_number,
        ):
            results.append({
                "status": "invalid",
                "title": title,
                "resolved_slug": final_slug,
                "draw_date": str(draw_date),
                "payload": payload,
            })
            continue

        try:
            game, created_game = get_or_create_game_in_db(
                final_slug=final_slug,
                canonical_name=canonical_name,
                games_by_slug=games_by_slug,
            )
        except Exception as e:
            results.append({
                "status": "unmatched",
                "title": title,
                "resolved_slug": final_slug,
                "draw_date": str(draw_date),
                "error": str(e),
                "payload": payload,
            })
            continue

        notes_parts = [
            "Scraped from Lottery Post DOM sections v2 normalized",
            f"original_title={title}",
            f"parser_type={parser_type}",
            f"canonical_slug={canonical_slug}",
        ]

        if parser_type in {"special", "many-numbers", "2by2"}:
            notes_parts.append(f"raw_numbers={raw_numbers}")

        final_notes = " | ".join(notes_parts)

        action = save_draw(
            game=game,
            draw_date=draw_date,
            draw_type=draw_type,
            main_numbers=main_numbers,
            bonus_number=final_bonus_number,
            multiplier=final_multiplier,
            jackpot=jackpot,
            jackpot_change=jackpot_change,
            next_draw_text=next_draw_text,
            next_draw_timezone=next_draw_timezone,
            next_draw_relative=next_draw_relative,
            source_url=state["source_url"],
            raw_payload=payload,
            notes=final_notes,
        )

        row = {
            "status": action,
            "title": title,
            "resolved_slug": game.slug,
            "draw_date": str(draw_date),
            "main_numbers": main_numbers,
            "bonus_number": final_bonus_number,
            "multiplier": final_multiplier,
            "jackpot": jackpot,
            "jackpot_change": jackpot_change,
            "next_draw_text": next_draw_text,
            "next_draw_timezone": next_draw_timezone,
            "next_draw_relative": next_draw_relative,
        }

        if created_game:
            row["created_game"] = True

        results.append(row)

    return results


def main():
    states = get_states()
    games_by_slug = get_games()

    created = 0
    updated = 0
    unmatched = 0
    invalid = 0
    created_games = 0
    report = []

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False, slow_mo=20)
        page = browser.new_page()

        for state in states:
            print("\n" + "=" * 90)
            print(f"STATE: {state['name']} ({state['slug']})")
            print(f"URL: {state['source_url']}")
            print("=" * 90)

            try:
                rows = scrape_state(page, state, games_by_slug)
            except Exception as e:
                print(f"ERROR: {e}")
                report.append({
                    "state": state["slug"],
                    "state_name": state["name"],
                    "error": str(e),
                    "rows": [],
                })
                continue

            for row in rows:
                status = row["status"]

                if row.get("created_game"):
                    created_games += 1
                    print(f"CREATED GAME ON THE FLY: {row['resolved_slug']}")

                if status == "created":
                    created += 1
                    print(f"CREATED: {row['title']} -> {row['resolved_slug']} -> {row['main_numbers']}")
                elif status == "updated":
                    updated += 1
                    print(f"UPDATED: {row['title']} -> {row['resolved_slug']} -> {row['main_numbers']}")
                elif status == "unmatched":
                    unmatched += 1
                    print(f"UNMATCHED: {row['title']} -> {row['resolved_slug']}")
                elif status == "invalid":
                    invalid += 1
                    print(f"INVALID: {row['title']} -> {row['resolved_slug']}")

            report.append({
                "state": state["slug"],
                "state_name": state["name"],
                "rows": rows,
            })

        browser.close()

    with open("all_states_dom_report_v2.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\nSUMMARY")
    print("=" * 90)
    print(f"Created games: {created_games}")
    print(f"Created draws: {created}")
    print(f"Updated draws: {updated}")
    print(f"Unmatched: {unmatched}")
    print(f"Invalid: {invalid}")
    print("Report saved: all_states_dom_report_v2.json")


if __name__ == "__main__":
    main()