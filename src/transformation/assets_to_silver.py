import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.schemas import asset_schema
from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.utils.config import CRYPTO_SYMBOLS, EQUITY_SYMBOLS, FX_SYMBOL, SILVER_DIR


def build_asset_rows():
    rows = []

    for symbol in EQUITY_SYMBOLS:
        row = {
            "symbol": symbol,
            "asset_class": "equity",
            "source": "yahoo",
            "currency": "AUD",
            "description": symbol,
            "is_active": True,
        }
        rows.append(row)

    for symbol in CRYPTO_SYMBOLS:
        row = {
            "symbol": symbol,
            "asset_class": "crypto",
            "source": "binance",
            "currency": "USDT",
            "description": symbol,
            "is_active": True,
        }
        rows.append(row)

    fx_row = {
        "symbol": FX_SYMBOL,
        "asset_class": "fx",
        "source": "yahoo",
        "currency": "USD",
        "description": "AUD/USD exchange rate",
        "is_active": True,
    }
    rows.append(fx_row)

    return rows


def create_assets_dataframe(spark):
    rows = build_asset_rows()
    schema = asset_schema()
    assets_df = spark.createDataFrame(rows, schema)

    return assets_df


def write_assets(assets_df, output_path=None, mode="overwrite"):
    if output_path is None:
        output_path = SILVER_DIR / "assets"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer = assets_df.write
    writer = writer.mode(mode)
    writer.parquet(str(output_path))

    return output_path


def transform_assets_to_silver(spark, output_path=None, mode="overwrite"):
    assets_df = create_assets_dataframe(spark)
    output_path = write_assets(assets_df, output_path, mode)
    row_count = assets_df.count()

    result = {
        "table": "silver.assets",
        "row_count": row_count,
        "output_path": str(output_path),
    }

    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Create Silver assets Parquet from configured asset lists.")
    parser.add_argument("--master", default="local[*]", help="Spark master value.")
    parser.add_argument("--output-path", default=None, help="Optional output path for Silver assets Parquet.")
    parser.add_argument("--mode", default="overwrite", help="Spark write mode. Default is overwrite.")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    spark = None

    try:
        spark = build_spark_session("finance-market-data-assets-to-silver", args.master)
        result = transform_assets_to_silver(spark, args.output_path, args.mode)
        print(f"Wrote {result['row_count']} rows to {result['output_path']}")
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
