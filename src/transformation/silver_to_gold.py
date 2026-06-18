import argparse
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as spark_functions

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.schemas import (
    gold_asset_return_schema,
    gold_portfolio_return_schema,
    gold_portfolio_summary_schema,
    portfolio_weight_schema,
)
from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.utils.config import (
    FIXED_PORTFOLIO_WEIGHTS,
    GOLD_DIR,
    RISK_FREE_RATE,
    SILVER_DIR,
    TRADING_DAYS_PER_YEAR,
)


def read_silver_asset_prices(spark):
    path = SILVER_DIR / "asset_prices"
    asset_prices_df = spark.read.parquet(str(path))

    return asset_prices_df


def read_silver_fx_rates(spark):
    path = SILVER_DIR / "fx_rates"
    fx_rates_df = spark.read.parquet(str(path))

    return fx_rates_df


def build_asset_returns(asset_prices_df, fx_rates_df):
    fx_lookup_df = fx_rates_df.select(
        spark_functions.col("rate_date").alias("fx_date"),
        spark_functions.col("rate").alias("aud_usd_rate"),
    )

    joined_df = asset_prices_df.join(
        fx_lookup_df,
        asset_prices_df.price_date == fx_lookup_df.fx_date,
        "left",
    )

    selected_df = joined_df.select(
        spark_functions.col("symbol").alias("symbol"),
        spark_functions.col("asset_class").alias("asset_class"),
        spark_functions.col("source").alias("source"),
        spark_functions.col("price_date").alias("price_date"),
        spark_functions.col("close_price").alias("close_price_native"),
        spark_functions.when(
            spark_functions.col("currency") == "AUD",
            spark_functions.col("close_price"),
        )
        .when(
            spark_functions.col("currency") == "USDT",
            spark_functions.col("close_price") / spark_functions.col("aud_usd_rate"),
        )
        .otherwise(None)
        .alias("close_price_aud"),
        spark_functions.col("currency").alias("currency"),
    )

    clean_df = selected_df.filter(spark_functions.col("close_price_aud").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("close_price_aud") > 0)

    price_window = Window.partitionBy("symbol").orderBy("price_date")
    return_df = clean_df.withColumn(
        "previous_close_price_aud",
        spark_functions.lag(spark_functions.col("close_price_aud")).over(price_window),
    )
    return_df = return_df.withColumn(
        "daily_return",
        spark_functions.col("close_price_aud") / spark_functions.col("previous_close_price_aud") - spark_functions.lit(1.0),
    )

    typed_df = apply_gold_asset_return_schema(return_df)

    return typed_df


def apply_gold_asset_return_schema(asset_returns_df):
    schema = gold_asset_return_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = asset_returns_df.select(selected_columns)

    return typed_df


def build_portfolio_weights_dataframe(spark, portfolio_name="fixed_demo", weights=None):
    if weights is None:
        weights = FIXED_PORTFOLIO_WEIGHTS

    rows = []

    for weight in weights:
        row = {
            "portfolio_name": portfolio_name,
            "symbol": weight["symbol"],
            "target_weight": float(weight["weight"]),
        }
        rows.append(row)

    weights_df = spark.createDataFrame(rows, portfolio_weight_schema())

    return weights_df


def build_portfolio_returns(asset_returns_df, portfolio_weights_df):
    weighted_returns_df = asset_returns_df.join(portfolio_weights_df, "symbol", "inner")
    weighted_returns_df = weighted_returns_df.filter(spark_functions.col("daily_return").isNotNull())
    weighted_returns_df = weighted_returns_df.withColumn(
        "weighted_return",
        spark_functions.col("daily_return") * spark_functions.col("target_weight"),
    )

    grouped_df = weighted_returns_df.groupBy("portfolio_name", "price_date").agg(
        spark_functions.sum("weighted_return").alias("daily_return"),
        spark_functions.sum("target_weight").alias("available_weight"),
    )

    complete_df = grouped_df.filter(spark_functions.col("available_weight") >= spark_functions.lit(0.999999))

    portfolio_window = Window.partitionBy("portfolio_name").orderBy("price_date")
    cumulative_log_return = spark_functions.sum(
        spark_functions.log(spark_functions.col("daily_return") + spark_functions.lit(1.0))
    ).over(portfolio_window)

    portfolio_returns_df = complete_df.withColumn(
        "cumulative_return",
        spark_functions.exp(cumulative_log_return) - spark_functions.lit(1.0),
    )

    typed_df = apply_gold_portfolio_return_schema(portfolio_returns_df)

    return typed_df


def apply_gold_portfolio_return_schema(portfolio_returns_df):
    schema = gold_portfolio_return_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = portfolio_returns_df.select(selected_columns)

    return typed_df


def build_portfolio_summary(portfolio_returns_df, risk_free_rate=RISK_FREE_RATE):
    portfolio_value_df = portfolio_returns_df.withColumn(
        "portfolio_value",
        spark_functions.col("cumulative_return") + spark_functions.lit(1.0),
    )

    drawdown_window = Window.partitionBy("portfolio_name").orderBy("price_date").rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    drawdown_df = portfolio_value_df.withColumn(
        "running_peak_value",
        spark_functions.max("portfolio_value").over(drawdown_window),
    )
    drawdown_df = drawdown_df.withColumn(
        "drawdown",
        spark_functions.col("portfolio_value") / spark_functions.col("running_peak_value") - spark_functions.lit(1.0),
    )

    return_stats_df = portfolio_returns_df.groupBy("portfolio_name").agg(
        spark_functions.min("price_date").alias("start_date"),
        spark_functions.max("price_date").alias("end_date"),
        spark_functions.count("daily_return").alias("observation_count"),
        spark_functions.avg("daily_return").alias("average_daily_return"),
        spark_functions.stddev_samp("daily_return").alias("daily_volatility"),
    )

    drawdown_stats_df = drawdown_df.groupBy("portfolio_name").agg(
        spark_functions.min("drawdown").alias("max_drawdown")
    )

    summary_df = return_stats_df.join(drawdown_stats_df, "portfolio_name", "inner")
    summary_df = summary_df.withColumn(
        "annual_return",
        spark_functions.col("average_daily_return") * spark_functions.lit(TRADING_DAYS_PER_YEAR),
    )
    summary_df = summary_df.withColumn(
        "annual_volatility",
        spark_functions.col("daily_volatility") * spark_functions.sqrt(spark_functions.lit(TRADING_DAYS_PER_YEAR)),
    )
    summary_df = summary_df.withColumn("risk_free_rate", spark_functions.lit(risk_free_rate))
    summary_df = summary_df.withColumn(
        "sharpe_ratio",
        (spark_functions.col("annual_return") - spark_functions.col("risk_free_rate"))
        / spark_functions.col("annual_volatility"),
    )

    typed_df = apply_gold_portfolio_summary_schema(summary_df)

    return typed_df


def apply_gold_portfolio_summary_schema(portfolio_summary_df):
    schema = gold_portfolio_summary_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = portfolio_summary_df.select(selected_columns)

    return typed_df


def write_gold_table(dataframe, table_name, mode="overwrite"):
    output_path = GOLD_DIR / table_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer = dataframe.write
    writer = writer.mode(mode)
    writer.parquet(str(output_path))

    return output_path


def transform_silver_to_gold(spark, mode="overwrite"):
    asset_prices_df = read_silver_asset_prices(spark)
    fx_rates_df = read_silver_fx_rates(spark)

    asset_returns_df = build_asset_returns(asset_prices_df, fx_rates_df)
    asset_returns_output_path = write_gold_table(asset_returns_df, "asset_returns", mode)
    asset_returns_row_count = asset_returns_df.count()

    weights_df = build_portfolio_weights_dataframe(spark)
    portfolio_returns_df = build_portfolio_returns(asset_returns_df, weights_df)
    portfolio_returns_output_path = write_gold_table(portfolio_returns_df, "portfolio_returns", mode)
    portfolio_returns_row_count = portfolio_returns_df.count()

    portfolio_summary_df = build_portfolio_summary(portfolio_returns_df)
    portfolio_summary_output_path = write_gold_table(portfolio_summary_df, "portfolio_summary", mode)
    portfolio_summary_row_count = portfolio_summary_df.count()

    result = {
        "asset_returns_row_count": asset_returns_row_count,
        "asset_returns_output_path": str(asset_returns_output_path),
        "portfolio_returns_row_count": portfolio_returns_row_count,
        "portfolio_returns_output_path": str(portfolio_returns_output_path),
        "portfolio_summary_row_count": portfolio_summary_row_count,
        "portfolio_summary_output_path": str(portfolio_summary_output_path),
    }

    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Silver Parquet tables into Gold portfolio metrics.")
    parser.add_argument("--master", default="local[*]", help="Spark master value.")
    parser.add_argument("--mode", default="overwrite", help="Spark write mode. Default is overwrite.")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    spark = None

    try:
        spark = build_spark_session("finance-market-data-silver-to-gold", args.master)
        result = transform_silver_to_gold(spark, args.mode)
        print(f"Wrote {result['asset_returns_row_count']} rows to {result['asset_returns_output_path']}")
        print(f"Wrote {result['portfolio_returns_row_count']} rows to {result['portfolio_returns_output_path']}")
        print(f"Wrote {result['portfolio_summary_row_count']} rows to {result['portfolio_summary_output_path']}")
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
