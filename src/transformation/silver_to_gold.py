import argparse
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as spark_functions

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.schemas import (
    gold_asset_return_calendar_schema,
    gold_asset_return_schema,
    gold_date_spine_schema,
    gold_portfolio_return_calendar_schema,
    gold_portfolio_return_schema,
    gold_portfolio_summary_schema,
    portfolio_weight_schema,
)
from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.utils.config import (
    CALENDAR_DAYS_PER_YEAR,
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


def build_date_spine(asset_prices_df):
    bounds_df = asset_prices_df.agg(
        spark_functions.min("price_date").alias("start_date"),
        spark_functions.max("price_date").alias("end_date"),
    )

    date_spine_df = bounds_df.select(
        spark_functions.explode(
            spark_functions.sequence(
                spark_functions.col("start_date"),
                spark_functions.col("end_date"),
                spark_functions.expr("interval 1 day"),
            )
        ).alias("calendar_date")
    )

    typed_df = apply_gold_date_spine_schema(date_spine_df)

    return typed_df


def apply_gold_date_spine_schema(date_spine_df):
    schema = gold_date_spine_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = date_spine_df.select(selected_columns)

    return typed_df


def build_forward_filled_fx_rates(fx_rates_df, date_spine_df):
    observed_fx_df = fx_rates_df.select(
        spark_functions.col("rate_date").alias("observed_fx_date"),
        spark_functions.col("rate").alias("observed_aud_usd_rate"),
    )

    joined_df = date_spine_df.join(
        observed_fx_df,
        date_spine_df.calendar_date == observed_fx_df.observed_fx_date,
        "left",
    )
    joined_df = joined_df.withColumn("fx_partition", spark_functions.lit("AUDUSD=X"))

    fx_window = Window.partitionBy("fx_partition").orderBy("calendar_date").rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )

    filled_df = joined_df.withColumn(
        "source_fx_date",
        spark_functions.last(spark_functions.col("observed_fx_date"), True).over(fx_window),
    )
    filled_df = filled_df.withColumn(
        "aud_usd_rate",
        spark_functions.last(spark_functions.col("observed_aud_usd_rate"), True).over(fx_window),
    )
    filled_df = filled_df.withColumn(
        "is_fx_observed",
        spark_functions.col("observed_fx_date").isNotNull(),
    )
    filled_df = filled_df.withColumn(
        "is_fx_forward_filled",
        spark_functions.col("source_fx_date").isNotNull() & spark_functions.col("observed_fx_date").isNull(),
    )

    selected_df = filled_df.select(
        spark_functions.col("calendar_date"),
        spark_functions.col("aud_usd_rate"),
        spark_functions.col("source_fx_date"),
        spark_functions.col("is_fx_observed"),
        spark_functions.col("is_fx_forward_filled"),
    )

    return selected_df


def build_asset_returns_calendar(asset_prices_df, fx_rates_df, date_spine_df):
    asset_metadata_df = asset_prices_df.select(
        "symbol",
        "asset_class",
        "source",
        "currency",
    ).distinct()

    asset_calendar_df = asset_metadata_df.crossJoin(
        date_spine_df.select(spark_functions.col("calendar_date").alias("price_date"))
    )

    observed_prices_df = asset_prices_df.select(
        spark_functions.col("symbol").alias("observed_symbol"),
        spark_functions.col("price_date").alias("observed_price_date"),
        spark_functions.col("close_price").alias("observed_close_price_native"),
    )

    joined_prices_df = asset_calendar_df.join(
        observed_prices_df,
        (asset_calendar_df.symbol == observed_prices_df.observed_symbol)
        & (asset_calendar_df.price_date == observed_prices_df.observed_price_date),
        "left",
    )

    price_window = Window.partitionBy("symbol").orderBy("price_date").rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )

    filled_prices_df = joined_prices_df.withColumn(
        "source_price_date",
        spark_functions.last(spark_functions.col("observed_price_date"), True).over(price_window),
    )
    filled_prices_df = filled_prices_df.withColumn(
        "close_price_native",
        spark_functions.last(spark_functions.col("observed_close_price_native"), True).over(price_window),
    )
    filled_prices_df = filled_prices_df.withColumn(
        "is_price_observed",
        spark_functions.col("observed_price_date").isNotNull(),
    )
    filled_prices_df = filled_prices_df.withColumn(
        "is_price_forward_filled",
        spark_functions.col("source_price_date").isNotNull() & spark_functions.col("observed_price_date").isNull(),
    )
    filled_prices_df = filled_prices_df.filter(spark_functions.col("source_price_date").isNotNull())

    fx_filled_df = build_forward_filled_fx_rates(fx_rates_df, date_spine_df)
    joined_fx_df = filled_prices_df.join(
        fx_filled_df,
        filled_prices_df.price_date == fx_filled_df.calendar_date,
        "left",
    )

    selected_df = joined_fx_df.select(
        spark_functions.col("symbol"),
        spark_functions.col("asset_class"),
        spark_functions.col("source"),
        spark_functions.col("price_date"),
        spark_functions.col("close_price_native"),
        spark_functions.when(
            spark_functions.col("currency") == "AUD",
            spark_functions.col("close_price_native"),
        )
        .when(
            spark_functions.col("currency") == "USDT",
            spark_functions.col("close_price_native") / spark_functions.col("aud_usd_rate"),
        )
        .otherwise(None)
        .alias("close_price_aud"),
        spark_functions.col("currency"),
        spark_functions.col("is_price_observed"),
        spark_functions.col("is_price_forward_filled"),
        spark_functions.when(
            spark_functions.col("currency") == "USDT",
            spark_functions.col("is_fx_observed"),
        )
        .otherwise(spark_functions.lit(False))
        .alias("is_fx_observed"),
        spark_functions.when(
            spark_functions.col("currency") == "USDT",
            spark_functions.col("is_fx_forward_filled"),
        )
        .otherwise(spark_functions.lit(False))
        .alias("is_fx_forward_filled"),
        spark_functions.col("source_price_date"),
        spark_functions.when(
            spark_functions.col("currency") == "USDT",
            spark_functions.col("source_fx_date"),
        )
        .otherwise(None)
        .alias("source_fx_date"),
    )

    clean_df = selected_df.filter(spark_functions.col("close_price_aud").isNotNull())
    clean_df = clean_df.filter(spark_functions.col("close_price_aud") > 0)

    return_window = Window.partitionBy("symbol").orderBy("price_date")
    return_df = clean_df.withColumn(
        "previous_close_price_aud",
        spark_functions.lag(spark_functions.col("close_price_aud")).over(return_window),
    )
    return_df = return_df.withColumn(
        "daily_return",
        spark_functions.col("close_price_aud") / spark_functions.col("previous_close_price_aud") - spark_functions.lit(1.0),
    )

    typed_df = apply_gold_asset_return_calendar_schema(return_df)

    return typed_df


def apply_gold_asset_return_calendar_schema(asset_returns_calendar_df):
    schema = gold_asset_return_calendar_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = asset_returns_calendar_df.select(selected_columns)

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


def build_portfolio_returns_calendar(asset_returns_calendar_df, portfolio_weights_df):
    weighted_returns_df = asset_returns_calendar_df.join(portfolio_weights_df, "symbol", "inner")
    weighted_returns_df = weighted_returns_df.filter(spark_functions.col("daily_return").isNotNull())
    weighted_returns_df = weighted_returns_df.withColumn(
        "weighted_return",
        spark_functions.col("daily_return") * spark_functions.col("target_weight"),
    )
    weighted_returns_df = weighted_returns_df.withColumn(
        "observed_asset_flag",
        spark_functions.when(spark_functions.col("is_price_observed"), spark_functions.lit(1)).otherwise(spark_functions.lit(0)),
    )
    weighted_returns_df = weighted_returns_df.withColumn(
        "forward_filled_price_flag",
        spark_functions.when(spark_functions.col("is_price_forward_filled"), spark_functions.lit(1)).otherwise(spark_functions.lit(0)),
    )
    weighted_returns_df = weighted_returns_df.withColumn(
        "forward_filled_fx_flag",
        spark_functions.when(spark_functions.col("is_fx_forward_filled"), spark_functions.lit(1)).otherwise(spark_functions.lit(0)),
    )

    grouped_df = weighted_returns_df.groupBy("portfolio_name", "price_date").agg(
        spark_functions.sum("weighted_return").alias("daily_return"),
        spark_functions.sum("target_weight").alias("available_weight"),
        spark_functions.sum("observed_asset_flag").alias("observed_asset_count"),
        spark_functions.sum("forward_filled_price_flag").alias("forward_filled_price_count"),
        spark_functions.sum("forward_filled_fx_flag").alias("forward_filled_fx_count"),
    )

    complete_df = grouped_df.filter(spark_functions.col("available_weight") >= spark_functions.lit(0.999999))
    complete_df = complete_df.withColumn(
        "has_forward_filled_price",
        spark_functions.col("forward_filled_price_count") > spark_functions.lit(0),
    )
    complete_df = complete_df.withColumn(
        "has_forward_filled_fx",
        spark_functions.col("forward_filled_fx_count") > spark_functions.lit(0),
    )

    portfolio_window = Window.partitionBy("portfolio_name").orderBy("price_date")
    cumulative_log_return = spark_functions.sum(
        spark_functions.log(spark_functions.col("daily_return") + spark_functions.lit(1.0))
    ).over(portfolio_window)

    portfolio_returns_df = complete_df.withColumn(
        "cumulative_return",
        spark_functions.exp(cumulative_log_return) - spark_functions.lit(1.0),
    )

    typed_df = apply_gold_portfolio_return_calendar_schema(portfolio_returns_df)

    return typed_df


def apply_gold_portfolio_return_calendar_schema(portfolio_returns_calendar_df):
    schema = gold_portfolio_return_calendar_schema()
    selected_columns = []

    for field in schema.fields:
        selected_column = spark_functions.col(field.name).cast(field.dataType).alias(field.name)
        selected_columns.append(selected_column)

    typed_df = portfolio_returns_calendar_df.select(selected_columns)

    return typed_df


def build_portfolio_summary(portfolio_returns_df, risk_free_rate=RISK_FREE_RATE, annualization_days=TRADING_DAYS_PER_YEAR):
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
        spark_functions.col("average_daily_return") * spark_functions.lit(annualization_days),
    )
    summary_df = summary_df.withColumn(
        "annual_volatility",
        spark_functions.col("daily_volatility") * spark_functions.sqrt(spark_functions.lit(annualization_days)),
    )
    summary_df = summary_df.withColumn("risk_free_rate", spark_functions.lit(risk_free_rate))
    summary_df = summary_df.withColumn(
        "sharpe_ratio",
        (spark_functions.col("annual_return") - spark_functions.col("risk_free_rate"))
        / spark_functions.col("annual_volatility"),
    )

    typed_df = apply_gold_portfolio_summary_schema(summary_df)

    return typed_df


def build_portfolio_summary_calendar(portfolio_returns_calendar_df, risk_free_rate=RISK_FREE_RATE):
    portfolio_summary_df = build_portfolio_summary(
        portfolio_returns_calendar_df,
        risk_free_rate,
        CALENDAR_DAYS_PER_YEAR,
    )

    return portfolio_summary_df


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

    if mode.lower() == "overwrite":
        remove_existing_output_path(output_path)

    writer = dataframe.write
    writer = writer.mode(mode)
    writer.parquet(str(output_path))

    return output_path


def remove_existing_output_path(output_path):
    if not output_path.exists():
        return

    max_attempts = 5

    for attempt_number in range(max_attempts):
        try:
            if output_path.is_dir():
                shutil.rmtree(output_path, onexc=make_path_writable_and_retry)
            else:
                os.chmod(output_path, stat.S_IWRITE)
                output_path.unlink()

            return
        except OSError as error:
            is_last_attempt = attempt_number == max_attempts - 1

            if is_last_attempt:
                powershell_result = remove_existing_output_path_with_powershell(output_path)

                if powershell_result["removed"]:
                    return

                message = (
                    "Could not remove existing Gold output folder: "
                    + str(output_path)
                    + ". Close Spark, Power BI, Explorer preview, or any process reading this folder, then rerun. "
                    + "Original error: "
                    + str(error)
                )
                if powershell_result["error_message"] is not None:
                    message = message + " PowerShell fallback error: " + powershell_result["error_message"]

                raise OSError(message) from error

            time.sleep(0.5)


def make_path_writable_and_retry(function, path, excinfo):
    os.chmod(path, stat.S_IWRITE)
    function(path)


def remove_existing_output_path_with_powershell(output_path):
    result = {
        "removed": False,
        "error_message": None,
    }

    if not sys.platform.startswith("win"):
        return result

    quoted_output_path = quote_powershell_literal_path(str(output_path))
    command = "Remove-Item -LiteralPath " + quoted_output_path + " -Recurse -Force"

    process = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            command,
        ],
        capture_output=True,
        text=True,
    )

    if process.returncode == 0 and not output_path.exists():
        result["removed"] = True
        return result

    error_message = process.stderr.strip()
    if error_message == "":
        error_message = process.stdout.strip()

    if error_message == "":
        error_message = "PowerShell returned exit code " + str(process.returncode)

    result["error_message"] = error_message

    return result


def quote_powershell_literal_path(path):
    escaped_path = path.replace("'", "''")
    quoted_path = "'" + escaped_path + "'"

    return quoted_path

 
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

    date_spine_df = build_date_spine(asset_prices_df)
    date_spine_output_path = write_gold_table(date_spine_df, "date_spine", mode)
    date_spine_row_count = date_spine_df.count()

    asset_returns_calendar_df = build_asset_returns_calendar(asset_prices_df, fx_rates_df, date_spine_df)
    asset_returns_calendar_output_path = write_gold_table(asset_returns_calendar_df, "asset_returns_calendar", mode)
    asset_returns_calendar_row_count = asset_returns_calendar_df.count()

    portfolio_returns_calendar_df = build_portfolio_returns_calendar(asset_returns_calendar_df, weights_df)
    portfolio_returns_calendar_output_path = write_gold_table(
        portfolio_returns_calendar_df,
        "portfolio_returns_calendar",
        mode,
    )
    portfolio_returns_calendar_row_count = portfolio_returns_calendar_df.count()

    portfolio_summary_calendar_df = build_portfolio_summary_calendar(portfolio_returns_calendar_df)
    portfolio_summary_calendar_output_path = write_gold_table(
        portfolio_summary_calendar_df,
        "portfolio_summary_calendar",
        mode,
    )
    portfolio_summary_calendar_row_count = portfolio_summary_calendar_df.count()

    result = {
        "asset_returns_row_count": asset_returns_row_count,
        "asset_returns_output_path": str(asset_returns_output_path),
        "portfolio_returns_row_count": portfolio_returns_row_count,
        "portfolio_returns_output_path": str(portfolio_returns_output_path),
        "portfolio_summary_row_count": portfolio_summary_row_count,
        "portfolio_summary_output_path": str(portfolio_summary_output_path),
        "date_spine_row_count": date_spine_row_count,
        "date_spine_output_path": str(date_spine_output_path),
        "asset_returns_calendar_row_count": asset_returns_calendar_row_count,
        "asset_returns_calendar_output_path": str(asset_returns_calendar_output_path),
        "portfolio_returns_calendar_row_count": portfolio_returns_calendar_row_count,
        "portfolio_returns_calendar_output_path": str(portfolio_returns_calendar_output_path),
        "portfolio_summary_calendar_row_count": portfolio_summary_calendar_row_count,
        "portfolio_summary_calendar_output_path": str(portfolio_summary_calendar_output_path),
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
        print(f"Wrote {result['date_spine_row_count']} rows to {result['date_spine_output_path']}")
        print(f"Wrote {result['asset_returns_calendar_row_count']} rows to {result['asset_returns_calendar_output_path']}")
        print(f"Wrote {result['portfolio_returns_calendar_row_count']} rows to {result['portfolio_returns_calendar_output_path']}")
        print(f"Wrote {result['portfolio_summary_calendar_row_count']} rows to {result['portfolio_summary_calendar_output_path']}")
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
