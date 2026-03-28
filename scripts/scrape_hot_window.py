import json
import traceback
from datetime import datetime, timezone

from scripts.scrape_all_states_dom_v6 import main as run_general_scraper


def main():
    started_at = datetime.now(timezone.utc)

    report = {
        "job": "hot-window",
        "status": "started",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "message": None,
        "source_runner": "scripts.scrape_all_states_dom_v6",
        "generated_files": [
            "all_states_dom_report_v6.json",
            "hot_window_report.json",
            "hot_window_report.txt",
        ],
    }

    try:
        run_general_scraper()

        finished_at = datetime.now(timezone.utc)
        report["status"] = "success"
        report["finished_at"] = finished_at.isoformat()
        report["message"] = "Hot window wrapper executed successfully using scrape_all_states_dom_v6."

    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        report["status"] = "error"
        report["finished_at"] = finished_at.isoformat()
        report["message"] = str(e)
        report["traceback"] = traceback.format_exc()

    with open("hot_window_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open("hot_window_report.txt", "w", encoding="utf-8") as f:
        f.write("HOT WINDOW REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"job: {report['job']}\n")
        f.write(f"status: {report['status']}\n")
        f.write(f"started_at: {report['started_at']}\n")
        f.write(f"finished_at: {report['finished_at']}\n")
        f.write(f"source_runner: {report['source_runner']}\n")
        f.write(f"message: {report['message']}\n")
        if report.get("traceback"):
            f.write("\nTRACEBACK\n")
            f.write(report["traceback"])

    if report["status"] == "error":
        raise RuntimeError(report["message"])


if __name__ == "__main__":
    main()