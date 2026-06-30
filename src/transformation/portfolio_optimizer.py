import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as spark_functions

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.utils.config import FIXED_PORTFOLIO_WEIGHTS, GOLD_DIR


def read_gold_asset_returns_calendar(spark):
    path = GOLD_DIR / "asset_returns_calendar"
    asset_returns_calendar_df = spark.read.parquet(str(path))

    return asset_returns_calendar_df


def get_default_optimizer_symbols():
    symbols = []

    for weight in FIXED_PORTFOLIO_WEIGHTS:
        symbol = weight["symbol"]
        symbols.append(symbol)

    return symbols


def build_symbol_filter(symbols):
    symbol_filter = None

    for symbol in symbols:
        current_symbol_filter = spark_functions.col("symbol") == spark_functions.lit(symbol)

        if symbol_filter is None:
            symbol_filter = current_symbol_filter
        else:
            symbol_filter = symbol_filter | current_symbol_filter

    return symbol_filter


def build_symbol_column(symbol):
    column_name = "`" + symbol + "`"
    column = spark_functions.col(column_name)

    return column


def validate_return_matrix_symbols(symbols):
    if symbols is None:
        raise ValueError("At least one symbol is required to build the return matrix.")

    if len(symbols) == 0:
        raise ValueError("At least one symbol is required to build the return matrix.")


def build_return_matrix(asset_returns_calendar_df, symbols, start_date=None, end_date=None):
    validate_return_matrix_symbols(symbols)

    selected_df = asset_returns_calendar_df.select(
        spark_functions.col("price_date"),
        spark_functions.col("symbol"),
        spark_functions.col("daily_return"),
    )

    symbol_filter = build_symbol_filter(symbols)
    selected_df = selected_df.filter(symbol_filter)

    if start_date is not None:
        selected_df = selected_df.filter(
            spark_functions.col("price_date") >= spark_functions.lit(start_date).cast("date")
        )

    if end_date is not None:
        selected_df = selected_df.filter(
            spark_functions.col("price_date") <= spark_functions.lit(end_date).cast("date")
        )

    selected_df = selected_df.filter(spark_functions.col("daily_return").isNotNull())

    matrix_df = selected_df.groupBy("price_date").pivot("symbol", symbols).agg(
        spark_functions.first("daily_return", True)
    )

    complete_matrix_df = matrix_df

    for symbol in symbols:
        symbol_column = build_symbol_column(symbol)
        complete_matrix_df = complete_matrix_df.filter(symbol_column.isNotNull())

    selected_columns = []
    selected_columns.append(spark_functions.col("price_date"))

    for symbol in symbols:
        symbol_column = build_symbol_column(symbol)
        selected_column = symbol_column.cast("double").alias(symbol)
        selected_columns.append(selected_column)

    return_matrix_df = complete_matrix_df.select(selected_columns)
    return_matrix_df = return_matrix_df.orderBy("price_date")

    return return_matrix_df


def parse_args():
    parser = argparse.ArgumentParser(description="Build a wide daily return matrix for portfolio optimization.")
    parser.add_argument(
        "--master",
        default="local[1]",
        help="Spark master URL.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help="Symbol to include. Repeat this option for multiple symbols. Defaults to fixed portfolio symbols.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional first return date to include, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional last return date to include, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--show-rows",
        type=int,
        default=20,
        help="Number of matrix rows to print for inspection.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    spark = build_spark_session("portfolio-return-matrix", args.master)

    try:
        asset_returns_calendar_df = read_gold_asset_returns_calendar(spark)

        symbols = args.symbols
        if symbols is None:
            symbols = get_default_optimizer_symbols()

        return_matrix_df = build_return_matrix(
            asset_returns_calendar_df,
            symbols,
            args.start_date,
            args.end_date,
        )

        row_count = return_matrix_df.count()

        print("Return matrix symbols: " + ", ".join(symbols))
        print("Return matrix rows: " + str(row_count))
        return_matrix_df.show(args.show_rows, truncate=False)
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
