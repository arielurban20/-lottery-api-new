import json
from collections import defaultdict, Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent

REPORT_FILES = [
    BASE_DIR / "all_states_dom_report.json",
    BASE_DIR / "all_states_dom_report_v2.json",
    BASE_DIR / "strict_multistate_state_pages_report.json",
    BASE_DIR / "state_pages_preview_report.json",
    BASE_DIR / "compare_all_live_report.json",
]

OUTPUT_JSON = BASE_DIR / "audit_unmatched_games_report.json"
OUTPUT_TXT = BASE_DIR / "audit_unmatched_games_report.txt"


def safe_load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"__load_error__": str(e), "__file__": str(path)}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def make_bucket() -> Dict[str, Any]:
    return {
        "count": 0,
        "states": set(),
        "report_files": set(),
        "titles": set(),
        "slugs": set(),
        "statuses": Counter(),
        "samples": [],
    }


def add_sample(bucket: Dict[str, Any], sample: Dict[str, Any], max_samples: int = 5):
    if len(bucket["samples"]) < max_samples:
        bucket["samples"].append(sample)


def process_list_items(
    items: List[Dict[str, Any]],
    source_file: str,
    buckets_by_slug: Dict[str, Dict[str, Any]],
    raw_entries: List[Dict[str, Any]],
):
    for item in items:
        if not isinstance(item, dict):
            continue

        status = normalize_text(item.get("status") or item.get("result") or item.get("type")).upper()
        title = normalize_text(item.get("title") or item.get("game_name") or item.get("name"))
        slug = normalize_text(item.get("slug") or item.get("game_slug") or item.get("normalized_slug"))
        state_name = normalize_text(item.get("state_name") or item.get("state"))
        state_code = normalize_text(item.get("state_code") or item.get("code"))
        draw_date = normalize_text(item.get("date") or item.get("draw_date"))
        notes = normalize_text(item.get("notes") or item.get("message") or item.get("reason"))
        url = normalize_text(item.get("url") or item.get("state_url") or item.get("source_url"))

        interesting = status in {"UNMATCHED", "INVALID", "NOT_ENOUGH_NUMBERS", "NO_PARSE", "ERROR", "NEED_REVIEW"}

        if not interesting:
            continue

        key = slug or title or f"unknown-{len(raw_entries)+1}"
        bucket = buckets_by_slug.setdefault(key, make_bucket())

        bucket["count"] += 1
        if state_name:
            bucket["states"].add(f"{state_name} ({state_code})" if state_code else state_name)
        elif state_code:
            bucket["states"].add(state_code)

        bucket["report_files"].add(source_file)
        if title:
            bucket["titles"].add(title)
        if slug:
            bucket["slugs"].add(slug)
        if status:
            bucket["statuses"][status] += 1

        sample = {
            "status": status,
            "title": title,
            "slug": slug,
            "state_name": state_name,
            "state_code": state_code,
            "draw_date": draw_date,
            "notes": notes,
            "url": url,
            "source_file": source_file,
        }
        add_sample(bucket, sample)

        raw_entries.append(sample)


def scan_any_json(
    data: Any,
    source_file: str,
    buckets_by_slug: Dict[str, Dict[str, Any]],
    raw_entries: List[Dict[str, Any]],
):
    if isinstance(data, list):
        if all(isinstance(x, dict) for x in data):
            process_list_items(data, source_file, buckets_by_slug, raw_entries)
        for item in data:
            scan_any_json(item, source_file, buckets_by_slug, raw_entries)

    elif isinstance(data, dict):
        # caso directo: dict con keys tipo unmatched / invalid / results / items
        for key, value in data.items():
            lower_key = str(key).lower()

            if lower_key in {
                "unmatched",
                "invalid",
                "items",
                "results",
                "entries",
                "games",
                "details",
                "review",
                "problems",
                "issues",
            } and isinstance(value, list):
                process_list_items(value, source_file, buckets_by_slug, raw_entries)

            scan_any_json(value, source_file, buckets_by_slug, raw_entries)


def convert_sets(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "count": obj["count"],
        "states": sorted(obj["states"]),
        "report_files": sorted(obj["report_files"]),
        "titles": sorted(obj["titles"]),
        "slugs": sorted(obj["slugs"]),
        "statuses": dict(obj["statuses"]),
        "samples": obj["samples"],
    }


def build_state_summary(raw_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_state = defaultdict(lambda: {"count": 0, "statuses": Counter(), "games": set()})

    for row in raw_entries:
        state_label = ""
        if row["state_name"] and row["state_code"]:
            state_label = f'{row["state_name"]} ({row["state_code"]})'
        elif row["state_name"]:
            state_label = row["state_name"]
        elif row["state_code"]:
            state_label = row["state_code"]
        else:
            state_label = "UNKNOWN"

        by_state[state_label]["count"] += 1
        by_state[state_label]["statuses"][row["status"]] += 1
        if row["slug"]:
            by_state[state_label]["games"].add(row["slug"])
        elif row["title"]:
            by_state[state_label]["games"].add(row["title"])

    out = {}
    for state, info in sorted(by_state.items(), key=lambda x: (-x[1]["count"], x[0])):
        out[state] = {
            "count": info["count"],
            "statuses": dict(info["statuses"]),
            "games": sorted(info["games"]),
        }
    return out


def build_suggested_actions(grouped: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    suggestions = []

    for key, info in grouped.items():
        titles = " | ".join(info["titles"]).lower()
        slugs = " | ".join(info["slugs"]).lower()
        joined = f"{titles} {slugs}"

        action = "review_manually"
        reason = "Juego especial o nombre no normalizado"

        if "cash pop" in joined:
            action = "merge_by_base_slug"
            reason = "Cash Pop suele compartir slug base y variar por horario"
        elif "pick 3" in joined or "pick-3" in joined:
            action = "normalize_pick3_variants"
            reason = "Separar o consolidar por draw_type/midday/evening/night"
        elif "pick 4" in joined or "pick-4" in joined:
            action = "normalize_pick4_variants"
            reason = "Separar o consolidar por draw_type/midday/evening/night"
        elif "pick 5" in joined or "pick-5" in joined:
            action = "normalize_pick5_variants"
            reason = "Separar o consolidar por draw_type/midday/evening/night"
        elif "dc-3" in joined or "dc-4" in joined or "dc-5" in joined:
            action = "normalize_dc_variants"
            reason = "Juegos DC vienen con hora incrustada en el título"
        elif "all or nothing" in joined:
            action = "normalize_all_or_nothing_variants"
            reason = "All or Nothing suele venir por franja horaria"
        elif "quick draw" in joined or "keno" in joined:
            action = "special_parser_large_set"
            reason = "Juego de muchos números; revisar parser especializado"
        elif "daily derby" in joined:
            action = "special_parser_daily_derby"
            reason = "Daily Derby tiene estructura distinta"
        elif "myday" in joined:
            action = "special_parser_myday"
            reason = "MyDaY requiere lógica propia"
        elif "poker lotto" in joined:
            action = "special_parser_poker_lotto"
            reason = "Poker Lotto no usa la misma lógica de números simples"
        elif "2by2" in joined:
            action = "special_parser_2by2"
            reason = "2by2 usa formato 2+2"
        elif "lotería tradicional" in joined or "loteria tradicional" in joined:
            action = "exclude_or_custom_parser"
            reason = "Juego fuera del patrón estándar"

        suggestions.append({
            "group_key": key,
            "count": info["count"],
            "titles": info["titles"],
            "slugs": info["slugs"],
            "states": info["states"],
            "statuses": info["statuses"],
            "suggested_action": action,
            "reason": reason,
        })

    suggestions.sort(key=lambda x: (-x["count"], x["suggested_action"], x["group_key"]))
    return suggestions


def write_text_report(
    grouped: Dict[str, Dict[str, Any]],
    state_summary: Dict[str, Any],
    suggestions: List[Dict[str, Any]],
    scanned_files: List[str],
):
    lines = []
    lines.append("AUDIT UNMATCHED / INVALID GAMES")
    lines.append("=" * 90)
    lines.append("")

    lines.append("FILES SCANNED")
    lines.append("-" * 90)
    for f in scanned_files:
        lines.append(f"- {f}")
    lines.append("")

    lines.append("TOP STATES WITH ISSUES")
    lines.append("-" * 90)
    for state, info in list(state_summary.items())[:20]:
        lines.append(f"{state}: {info['count']} | statuses={info['statuses']}")
    lines.append("")

    lines.append("TOP GROUPED ISSUES")
    lines.append("-" * 90)
    top_groups = sorted(grouped.items(), key=lambda x: (-x[1]["count"], x[0]))
    for key, info in top_groups[:50]:
        lines.append(f"KEY: {key}")
        lines.append(f"COUNT: {info['count']}")
        lines.append(f"STATUSES: {info['statuses']}")
        lines.append(f"TITLES: {info['titles']}")
        lines.append(f"SLUGS: {info['slugs']}")
        lines.append(f"STATES: {info['states']}")
        if info["samples"]:
            lines.append("SAMPLES:")
            for s in info["samples"]:
                lines.append(
                    f"  - [{s['status']}] {s['title']} | slug={s['slug']} | "
                    f"state={s['state_name']} ({s['state_code']}) | date={s['draw_date']} | notes={s['notes']}"
                )
        lines.append("-" * 90)

    lines.append("")
    lines.append("SUGGESTED ACTIONS")
    lines.append("-" * 90)
    for s in suggestions[:50]:
        lines.append(
            f"{s['group_key']} | count={s['count']} | action={s['suggested_action']} | reason={s['reason']}"
        )

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    buckets_by_slug: Dict[str, Dict[str, Any]] = {}
    raw_entries: List[Dict[str, Any]] = []
    scanned_files: List[str] = []

    for report_file in REPORT_FILES:
        data = safe_load_json(report_file)
        if data is None:
            continue

        scanned_files.append(str(report_file.name))
        scan_any_json(data, report_file.name, buckets_by_slug, raw_entries)

    grouped = {k: convert_sets(v) for k, v in buckets_by_slug.items()}
    state_summary = build_state_summary(raw_entries)
    suggestions = build_suggested_actions(grouped)

    final_report = {
        "summary": {
            "files_scanned": scanned_files,
            "raw_issue_entries_found": len(raw_entries),
            "unique_grouped_issue_keys": len(grouped),
            "output_json": str(OUTPUT_JSON.name),
            "output_txt": str(OUTPUT_TXT.name),
        },
        "state_summary": state_summary,
        "grouped_issues": dict(sorted(grouped.items(), key=lambda x: (-x[1]["count"], x[0]))),
        "suggested_actions": suggestions,
        "raw_entries": raw_entries,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(final_report, f, ensure_ascii=False, indent=2)

    write_text_report(grouped, state_summary, suggestions, scanned_files)

    print("=" * 90)
    print("AUDIT COMPLETED")
    print("=" * 90)
    print("Files scanned:", len(scanned_files))
    print("Raw issue entries found:", len(raw_entries))
    print("Unique grouped issue keys:", len(grouped))
    print("JSON report:", OUTPUT_JSON.name)
    print("TXT report:", OUTPUT_TXT.name)
    print()

    print("TOP 20 GROUPED ISSUES")
    print("-" * 90)
    for key, info in list(sorted(grouped.items(), key=lambda x: (-x[1]["count"], x[0])))[:20]:
        print(f"{key} -> count={info['count']} | statuses={info['statuses']} | states={len(info['states'])}")

    print()
    print("TOP 20 STATES WITH ISSUES")
    print("-" * 90)
    for state, info in list(state_summary.items())[:20]:
        print(f"{state} -> count={info['count']} | statuses={info['statuses']}")


if __name__ == "__main__":
    main()