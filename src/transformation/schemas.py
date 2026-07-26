from pyspark.sql.types import ArrayType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


def yahoo_equity_bronze_schema():
    record_schema = StructType(
        [
            StructField("Date", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Volume", DoubleType(), True),
        ]
    )

    schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("records", ArrayType(record_schema), True),
        ]
    )

    return schema


def binance_crypto_bronze_schema():
    schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("records", ArrayType(ArrayType(StringType())), True),
        ]
    )

    return schema


def yahoo_fx_bronze_schema():
    record_schema = StructType(
        [
            StructField("Date", StringType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Volume", DoubleType(), True),
        ]
    )

    schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("records", ArrayType(record_schema), True),
        ]
    )

    return schema


def asset_price_schema():
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("asset_class", StringType(), False),
            StructField("source", StringType(), False),
            StructField("price_date", DateType(), False),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("currency", StringType(), False),
            StructField("ingestion_run_date", DateType(), False),
            StructField("bronze_file_path", StringType(), True),
        ]
    )

    return schema


def asset_schema():
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("asset_class", StringType(), False),
            StructField("source", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("description", StringType(), True),
            StructField("is_active", BooleanType(), False),
        ]
    )

    return schema


def fx_rate_schema():
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("source", StringType(), False),
            StructField("rate_date", DateType(), False),
            StructField("base_currency", StringType(), False),
            StructField("quote_currency", StringType(), False),
            StructField("rate", DoubleType(), False),
            StructField("ingestion_run_date", DateType(), False),
            StructField("bronze_file_path", StringType(), True),
        ]
    )

    return schema


def gold_dim_date_schema():
    schema = StructType(
        [
            StructField("date_key", DateType(), False),
            StructField("calendar_year", LongType(), False),
            StructField("calendar_quarter", LongType(), False),
            StructField("calendar_month", LongType(), False),
            StructField("month_name", StringType(), False),
            StructField("day_of_month", LongType(), False),
            StructField("day_of_week", LongType(), False),
            StructField("day_name", StringType(), False),
            StructField("is_weekend", BooleanType(), False),
        ]
    )

    return schema


def gold_dim_asset_schema():
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("asset_class", StringType(), False),
            StructField("source", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("description", StringType(), True),
            StructField("is_active", BooleanType(), False),
        ]
    )

    return schema


def gold_dim_portfolio_schema():
    schema = StructType(
        [
            StructField("portfolio_name", StringType(), False),
            StructField("portfolio_type", StringType(), False),
            StructField("description", StringType(), True),
            StructField("is_active", BooleanType(), False),
        ]
    )

    return schema


def gold_fact_asset_daily_schema():
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("date_key", DateType(), False),
            StructField("close_price_native", DoubleType(), False),
            StructField("close_price_aud", DoubleType(), False),
            StructField("daily_return", DoubleType(), True),
            StructField("is_price_observed", BooleanType(), False),
            StructField("is_price_forward_filled", BooleanType(), False),
            StructField("is_fx_observed", BooleanType(), False),
            StructField("is_fx_forward_filled", BooleanType(), False),
            StructField("source_price_date", DateType(), False),
            StructField("source_fx_date", DateType(), True),
        ]
    )

    return schema


def portfolio_weight_schema():
    schema = StructType(
        [
            StructField("portfolio_name", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("target_weight", DoubleType(), False),
        ]
    )

    return schema


def gold_fact_portfolio_daily_schema():
    schema = StructType(
        [
            StructField("portfolio_name", StringType(), False),
            StructField("date_key", DateType(), False),
            StructField("daily_return", DoubleType(), False),
            StructField("cumulative_return", DoubleType(), False),
            StructField("has_forward_filled_price", BooleanType(), False),
            StructField("has_forward_filled_fx", BooleanType(), False),
            StructField("observed_asset_count", LongType(), False),
            StructField("forward_filled_price_count", LongType(), False),
            StructField("forward_filled_fx_count", LongType(), False),
        ]
    )

    return schema


def gold_fact_portfolio_summary_daily_schema():
    schema = StructType(
        [
            StructField("portfolio_name", StringType(), False),
            StructField("date_key", DateType(), False),
            StructField("observation_count", LongType(), False),
            StructField("ytd_return", DoubleType(), True),
            StructField("annual_return", DoubleType(), True),
            StructField("annual_volatility", DoubleType(), True),
            StructField("risk_free_rate", DoubleType(), False),
            StructField("sharpe_ratio", DoubleType(), True),
            StructField("max_drawdown", DoubleType(), True),
        ]
    )

    return schema


def gold_fact_optimizer_summary_schema():
    schema = StructType(
        [
            StructField("portfolio_name", StringType(), False),
            StructField("as_of_date", DateType(), False),
            StructField("lookback_start_date", DateType(), False),
            StructField("lookback_end_date", DateType(), False),
            StructField("observation_count", LongType(), False),
            StructField("risk_free_rate", DoubleType(), False),
            StructField("annual_return", DoubleType(), True),
            StructField("annual_volatility", DoubleType(), True),
            StructField("sharpe_ratio", DoubleType(), True),
            StructField("optimizer_method", StringType(), False),
            StructField("constraint_set_name", StringType(), False),
            StructField("optimization_success", BooleanType(), False),
            StructField("optimization_message", StringType(), True),
        ]
    )

    return schema


def gold_fact_optimizer_weight_schema():
    schema = StructType(
        [
            StructField("portfolio_name", StringType(), False),
            StructField("as_of_date", DateType(), False),
            StructField("symbol", StringType(), False),
            StructField("current_weight", DoubleType(), False),
            StructField("optimized_weight", DoubleType(), False),
            StructField("weight_difference", DoubleType(), False),
        ]
    )

    return schema
