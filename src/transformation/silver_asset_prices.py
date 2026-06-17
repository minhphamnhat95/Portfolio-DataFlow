from pathlib import Path

from pyspark.sql import functions as spark_functions

from src.transformation.schemas import asset_price_schema
from src.utils.config import SILVER_DIR


def apply_asset_price_schema(asset_prices_df):
    schema = asset_price_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = asset_prices_df.select(selected_columns)

    return typed_df


def filter_clean_asset_price_rows(asset_prices_df):
    clean_df = asset_prices_df
    clean_df = clean_df.filter(spark_functions.col("symbol").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("asset_class").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("source").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("price_date").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("open_price").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("high_price").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("low_price").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("close_price").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("volume").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("currency").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("ingestion_run_date").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("open_price") >= 0)
    clean_df = clean_df.filter(spark_functions.col("high_price") >= 0)
    clean_df = clean_df.filter(spark_functions.col("low_price") >= 0)
    clean_df = clean_df.filter(spark_functions.col("close_price") >= 0)
    clean_df = clean_df.filter(spark_functions.col("volume") >= 0)

    return clean_df


def write_asset_prices(asset_prices_df, output_path=None, mode="overwrite"):
    if output_path is None:
        output_path = SILVER_DIR / "asset_prices"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer = asset_prices_df.write
    writer = writer.mode(mode)
    writer.parquet(str(output_path))

    return output_path
