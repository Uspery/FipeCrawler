from pathlib import Path
from typing import Dict
import json
from datetime import datetime

STATE_DIR = Path(".state")
STATE_FILE = STATE_DIR / "full_scan.json"


def today_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def load_state() -> Dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_state(state: Dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def clear_state() -> None:
    try:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
    except Exception:
        pass


def append_csv_row(csv_path: Path, row: Dict, headers: list) -> None:
    import csv

    new_file = not csv_path.exists()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if new_file:
            writer.writeheader()
        writer.writerow(row)
