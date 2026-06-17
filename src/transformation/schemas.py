from pyspark.sql.types import ArrayType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType
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
