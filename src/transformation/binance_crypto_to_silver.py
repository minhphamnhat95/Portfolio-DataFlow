import argparse
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import functions as spark_functions

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.schemas import binance_crypto_bronze_schema
from src.transformation.silver_asset_prices import (
    apply_asset_price_schema,
    filter_clean_asset_price_rows,
    write_asset_prices,
)
from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.utils.config import BRONZE_DIR


def locate_binance_crypto_bronze_paths(run_date):
    date_partition = f"date={run_date.isoformat()}"
    bronze_partition_path = BRONZE_DIR / "crypto" / "source=binance" / date_partition
    paths = []

    if not bronze_partition_path.exists():
        return paths

    for path in bronze_partition_path.glob("*.json"):
        paths.append(str(path))

    paths.sort()

    return paths


def read_binance_crypto_bronze(spark, run_date):
    paths = locate_binance_crypto_bronze_paths(run_date)

    if len(paths) == 0:
        raise ValueError(f"No Binance crypto Bronze JSON files found for {run_date.isoformat()}")

    reader = spark.read
    reader = reader.option("multiLine", "true")
    reader = reader.schema(binance_crypto_bronze_schema())
    bronze_df = reader.json(paths)

    return bronze_df


def transform_binance_crypto_bronze_to_asset_prices(bronze_df, run_date):
    exploded_df = bronze_df.select(
        spark_functions.col("symbol").alias("symbol"),
        spark_functions.col("source").alias("source"),
        spark_functions.explode(spark_functions.col("records")).alias("record"),
        spark_functions.input_file_name().alias("bronze_file_path"),
    )

    open_time_seconds = (spark_functions.col("record").getItem(0).cast("double") / spark_functions.lit(1000)).cast("long")

    selected_df = exploded_df.select(
        spark_functions.col("symbol").alias("symbol"),
        spark_functions.lit("crypto").alias("asset_class"),
        spark_functions.col("source").alias("source"),
        spark_functions.to_date(spark_functions.from_unixtime(open_time_seconds)).alias("price_date"),
        spark_functions.col("record").getItem(1).cast("double").alias("open_price"),
        spark_functions.col("record").getItem(2).cast("double").alias("high_price"),
        spark_functions.col("record").getItem(3).cast("double").alias("low_price"),
        spark_functions.col("record").getItem(4).cast("double").alias("close_price"),
        spark_functions.col("record").getItem(5).cast("double").alias("volume"),
        spark_functions.lit("USDT").alias("currency"),
        spark_functions.lit(run_date.isoformat()).cast("date").alias("ingestion_run_date"),
        spark_functions.col("bronze_file_path").alias("bronze_file_path"),
    )

    typed_df = apply_asset_price_schema(selected_df)
    clean_df = filter_clean_asset_price_rows(typed_df)

    return clean_df


def transform_binance_crypto_for_run_date(spark, run_date, output_path=None, mode="overwrite"):
    bronze_df = read_binance_crypto_bronze(spark, run_date)
    asset_prices_df = transform_binance_crypto_bronze_to_asset_prices(bronze_df, run_date)
    output_path = write_asset_prices(asset_prices_df, output_path, mode)

    row_count = asset_prices_df.count()

    result = {
        "table": "silver.asset_prices",
        "source": "binance",
        "asset_class": "crypto",
        "run_date": run_date.isoformat(),
        "row_count": row_count,
        "output_path": str(output_path),
    }

    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Binance crypto Bronze JSON into Silver asset_prices Parquet.")
    parser.add_argument("--run-date", required=True, help="Bronze run date partition to transform, YYYY-MM-DD.")
    parser.add_argument("--master", default="local[*]", help="Spark master value.")
    parser.add_argument("--output-path", default=None, help="Optional output path for Silver asset_prices Parquet.")
    parser.add_argument("--mode", default="overwrite", help="Spark write mode. Default is overwrite.")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.run_date)
    spark = None

    try:
        spark = build_spark_session("finance-market-data-binance-crypto-to-silver", args.master)
        result = transform_binance_crypto_for_run_date(spark, run_date, args.output_path, args.mode)
        print(f"Wrote {result['row_count']} rows to {result['output_path']}")
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
