from pathlib import Path

from pyspark.sql import functions as spark_functions

from src.transformation.schemas import fx_rate_schema
from src.utils.config import SILVER_DIR


def apply_fx_rate_schema(fx_rates_df):
    schema = fx_rate_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = fx_rates_df.select(selected_columns)

    return typed_df


def filter_clean_fx_rate_rows(fx_rates_df):
    clean_df = fx_rates_df
    clean_df = clean_df.filter(spark_functions.col("symbol").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("source").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("rate_date").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("base_currency").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("quote_currency").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("rate").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("ingestion_run_date").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("rate") > 0)

    return clean_df


def write_fx_rates(fx_rates_df, output_path=None, mode="overwrite"):
    if output_path is None:
        output_path = SILVER_DIR / "fx_rates"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer = fx_rates_df.write
    writer = writer.mode(mode)
    writer.parquet(str(output_path))

    return output_path
