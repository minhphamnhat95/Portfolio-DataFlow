import argparse
import sys
from datetime import date
from pathlib import Path

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.binance_crypto_to_silver import (
    read_binance_crypto_bronze,
    transform_binance_crypto_bronze_to_asset_prices,
)
from src.transformation.assets_to_silver import transform_assets_to_silver
from src.transformation.silver_asset_prices import write_asset_prices
from src.transformation.silver_fx_rates import write_fx_rates
from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.transformation.yahoo_equities_to_silver import (
    read_yahoo_equity_bronze,
    transform_yahoo_equity_bronze_to_asset_prices,
)
from src.transformation.yahoo_fx_to_silver import read_yahoo_fx_bronze, transform_yahoo_fx_bronze_to_fx_rates


def transform_bronze_to_silver_for_run_date(spark, run_date, mode="overwrite"):
    assets_result = transform_assets_to_silver(spark, mode=mode)

    yahoo_equity_bronze_df = read_yahoo_equity_bronze(spark, run_date)
    yahoo_asset_prices_df = transform_yahoo_equity_bronze_to_asset_prices(yahoo_equity_bronze_df, run_date)

    binance_crypto_bronze_df = read_binance_crypto_bronze(spark, run_date)
    binance_asset_prices_df = transform_binance_crypto_bronze_to_asset_prices(binance_crypto_bronze_df, run_date)

    asset_prices_df = yahoo_asset_prices_df.unionByName(binance_asset_prices_df)
    asset_prices_output_path = write_asset_prices(asset_prices_df, mode=mode)
    asset_prices_row_count = asset_prices_df.count()

    yahoo_fx_bronze_df = read_yahoo_fx_bronze(spark, run_date)
    fx_rates_df = transform_yahoo_fx_bronze_to_fx_rates(yahoo_fx_bronze_df, run_date)
    fx_rates_output_path = write_fx_rates(fx_rates_df, mode=mode)
    fx_rates_row_count = fx_rates_df.count()

    result = {
        "run_date": run_date.isoformat(),
        "assets_row_count": assets_result["row_count"],
        "assets_output_path": assets_result["output_path"],
        "asset_prices_row_count": asset_prices_row_count,
        "asset_prices_output_path": str(asset_prices_output_path),
        "fx_rates_row_count": fx_rates_row_count,
        "fx_rates_output_path": str(fx_rates_output_path),
    }

    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Bronze JSON files into Silver Parquet tables.")
    parser.add_argument("--run-date", required=True, help="Bronze run date partition to transform, YYYY-MM-DD.")
    parser.add_argument("--master", default="local[*]", help="Spark master value.")
    parser.add_argument("--mode", default="overwrite", help="Spark write mode. Default is overwrite.")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.run_date)
    spark = None

    try:
        spark = build_spark_session("finance-market-data-bronze-to-silver", args.master)
        result = transform_bronze_to_silver_for_run_date(spark, run_date, args.mode)
        print(f"Wrote {result['assets_row_count']} rows to {result['assets_output_path']}")
        print(f"Wrote {result['asset_prices_row_count']} rows to {result['asset_prices_output_path']}")
        print(f"Wrote {result['fx_rates_row_count']} rows to {result['fx_rates_output_path']}")
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
