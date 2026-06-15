from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from src.utils.config import BRONZE_DIR


def write_bronze(asset_class: str, source: str, symbol: str, payload: dict[str, Any], run_date: date) -> Path:
    safe_symbol = symbol.replace("/", "_")
    output_dir = BRONZE_DIR / asset_class / f"source={source}" / f"date={run_date.isoformat()}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{safe_symbol}.json"
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, default=str)
    return output_path

