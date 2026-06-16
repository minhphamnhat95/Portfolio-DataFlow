import json

from src.utils.config import BRONZE_DIR


def write_bronze(asset_class, source, symbol, payload, run_date):
    safe_symbol = symbol.replace("/", "_")
    date_partition = f"date={run_date.isoformat()}"
    source_partition = f"source={source}"

    output_dir = BRONZE_DIR / asset_class / source_partition / date_partition
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file_name = f"{safe_symbol}.json"
    output_path = output_dir / output_file_name

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, default=str)

    return output_path
