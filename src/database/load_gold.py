import argparse
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path
from uuid import uuid4

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.database.connection import (
    close_postgres_connection,
    describe_postgres_connection,
    open_postgres_connection,
)
from src.database.schema import create_database_objects
from src.utils.config import GOLD_DIR


GOLD_TABLE_CONFIGS = [
    {
        "gold_folder_name": "dim_date",
        "schema_name": "gold",
        "table_name": "dim_date",
        "qualified_table_name": "gold.dim_date",
        "columns": [
            "date_key",
            "calendar_year",
            "calendar_quarter",
            "calendar_month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend",
        ],
    },
    {
        "gold_folder_name": "dim_asset",
        "schema_name": "gold",
        "table_name": "dim_asset",
        "qualified_table_name": "gold.dim_asset",
        "columns": [
            "symbol",
            "asset_class",
            "source",
            "currency",
            "description",
            "is_active",
        ],
    },
    {
        "gold_folder_name": "dim_portfolio",
        "schema_name": "gold",
        "table_name": "dim_portfolio",
        "qualified_table_name": "gold.dim_portfolio",
        "columns": [
            "portfolio_name",
            "portfolio_type",
            "description",
            "is_active",
        ],
    },
    {
        "gold_folder_name": "fact_asset_daily",
        "schema_name": "gold",
        "table_name": "fact_asset_daily",
        "qualified_table_name": "gold.fact_asset_daily",
        "columns": [
            "symbol",
            "date_key",
            "close_price_native",
            "close_price_aud",
            "daily_return",
            "is_price_observed",
            "is_price_forward_filled",
            "is_fx_observed",
            "is_fx_forward_filled",
            "source_price_date",
            "source_fx_date",
        ],
    },
    {
        "gold_folder_name": "fact_portfolio_daily",
        "schema_name": "gold",
        "table_name": "fact_portfolio_daily",
        "qualified_table_name": "gold.fact_portfolio_daily",
        "columns": [
            "portfolio_name",
            "date_key",
            "daily_return",
            "cumulative_return",
            "has_forward_filled_price",
            "has_forward_filled_fx",
            "observed_asset_count",
            "forward_filled_price_count",
            "forward_filled_fx_count",
        ],
    },
    {
        "gold_folder_name": "fact_portfolio_summary_daily",
        "schema_name": "gold",
        "table_name": "fact_portfolio_summary_daily",
        "qualified_table_name": "gold.fact_portfolio_summary_daily",
        "columns": [
            "portfolio_name",
            "date_key",
            "observation_count",
            "ytd_return",
            "annual_return",
            "annual_volatility",
            "risk_free_rate",
            "sharpe_ratio",
            "max_drawdown",
        ],
    },
    {
        "gold_folder_name": "fact_optimizer_summary",
        "schema_name": "gold",
        "table_name": "fact_optimizer_summary",
        "qualified_table_name": "gold.fact_optimizer_summary",
        "columns": [
            "portfolio_name",
            "as_of_date",
            "lookback_start_date",
            "lookback_end_date",
            "observation_count",
            "risk_free_rate",
            "annual_return",
            "annual_volatility",
            "sharpe_ratio",
            "optimizer_method",
            "constraint_set_name",
            "optimization_success",
            "optimization_message",
        ],
    },
    {
        "gold_folder_name": "fact_optimizer_weights",
        "schema_name": "gold",
        "table_name": "fact_optimizer_weights",
        "qualified_table_name": "gold.fact_optimizer_weights",
        "columns": [
            "portfolio_name",
            "as_of_date",
            "symbol",
            "current_weight",
            "optimized_weight",
            "weight_difference",
        ],
    },
]


def get_current_utc_timestamp():
    current_timestamp = datetime.now(timezone.utc)
    current_timestamp = current_timestamp.replace(tzinfo=None)

    return current_timestamp


def read_gold_parquet(gold_folder_name):
    import pandas

    parquet_path = GOLD_DIR / gold_folder_name

    if not parquet_path.exists():
        message = "Gold Parquet folder does not exist: " + str(parquet_path)
        raise FileNotFoundError(message)

    dataframe = pandas.read_parquet(str(parquet_path))

    return dataframe


def validate_dataframe_columns(dataframe, expected_columns, table_name):
    actual_columns = list(dataframe.columns)
    missing_columns = []

    for expected_column in expected_columns:
        if expected_column not in actual_columns:
            missing_columns.append(expected_column)

    if len(missing_columns) > 0:
        message = "Missing columns for " + table_name + ": " + ", ".join(missing_columns)
        raise ValueError(message)


def normalize_database_value(value):
    import pandas

    if value is None:
        return None

    if pandas.isna(value):
        return None

    if hasattr(value, "to_pydatetime"):
        converted_value = value.to_pydatetime()
        return converted_value

    if hasattr(value, "item"):
        converted_value = value.item()
        return converted_value

    return value


def build_insert_rows(dataframe, columns):
    records = dataframe.to_dict("records")
    rows = []

    for record in records:
        row_values = []

        for column_name in columns:
            value = record[column_name]
            value = normalize_database_value(value)
            row_values.append(value)

        row = tuple(row_values)
        rows.append(row)

    return rows


def truncate_table(connection, schema_name, table_name):
    from psycopg2 import sql

    cursor = connection.cursor()

    try:
        statement = sql.SQL("TRUNCATE TABLE {}.{};").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        cursor.execute(statement)
    finally:
        cursor.close()


def insert_rows(connection, schema_name, table_name, columns, rows):
    if len(rows) == 0:
        return

    from psycopg2 import sql
    from psycopg2.extras import execute_values

    cursor = connection.cursor()

    try:
        column_identifiers = []

        for column_name in columns:
            column_identifier = sql.Identifier(column_name)
            column_identifiers.append(column_identifier)

        statement = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(column_identifiers),
        )

        query = statement.as_string(connection)
        execute_values(cursor, query, rows)
    finally:
        cursor.close()


def insert_load_log(connection, run_id, table_name, started_at, finished_at, status, row_count, error_message):
    cursor = connection.cursor()

    try:
        statement = """
        INSERT INTO audit.load_logs (
            run_id,
            table_name,
            started_at,
            finished_at,
            status,
            row_count,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        values = (
            run_id,
            table_name,
            started_at,
            finished_at,
            status,
            row_count,
            error_message,
        )

        cursor.execute(statement, values)
    finally:
        cursor.close()


def load_one_gold_table(connection, table_config, run_id):
    table_name = table_config["qualified_table_name"]
    started_at = get_current_utc_timestamp()
    row_count = 0

    try:
        dataframe = read_gold_parquet(table_config["gold_folder_name"])
        validate_dataframe_columns(dataframe, table_config["columns"], table_name)

        row_count = len(dataframe)
        rows = build_insert_rows(dataframe, table_config["columns"])

        truncate_table(connection, table_config["schema_name"], table_config["table_name"])
        insert_rows(connection, table_config["schema_name"], table_config["table_name"], table_config["columns"], rows)

        finished_at = get_current_utc_timestamp()
        insert_load_log(connection, run_id, table_name, started_at, finished_at, "success", row_count, None)
        connection.commit()

        result = {
            "table_name": table_name,
            "status": "success",
            "row_count": row_count,
            "error_message": None,
        }

        return result
    except Exception as error:
        connection.rollback()

        finished_at = get_current_utc_timestamp()
        error_message = str(error)
        insert_load_log(connection, run_id, table_name, started_at, finished_at, "failed", row_count, error_message)
        connection.commit()

        raise


def preview_gold_tables():
    results = []

    for table_config in GOLD_TABLE_CONFIGS:
        dataframe = read_gold_parquet(table_config["gold_folder_name"])
        validate_dataframe_columns(dataframe, table_config["columns"], table_config["qualified_table_name"])

        result = {
            "table_name": table_config["qualified_table_name"],
            "gold_folder_name": table_config["gold_folder_name"],
            "row_count": len(dataframe),
        }
        results.append(result)

    return results


def load_gold_tables():
    run_id = "load_" + uuid4().hex[:12]
    connection = None
    results = []

    try:
        connection = open_postgres_connection()
        create_database_objects(connection)

        for table_config in GOLD_TABLE_CONFIGS:
            result = load_one_gold_table(connection, table_config, run_id)
            results.append(result)
    finally:
        close_postgres_connection(connection)

    load_result = {
        "run_id": run_id,
        "results": results,
    }

    return load_result


def print_preview_results(results):
    print("Gold Parquet tables ready to load:")

    for result in results:
        line = (
            result["table_name"]
            + " <- data/gold/"
            + result["gold_folder_name"]
            + " ("
            + str(result["row_count"])
            + " rows)"
        )
        print(line)


def print_load_results(load_result):
    print("PostgreSQL load run: " + load_result["run_id"])

    for result in load_result["results"]:
        line = (
            result["table_name"]
            + " -> "
            + result["status"]
            + " ("
            + str(result["row_count"])
            + " rows)"
        )
        print(line)


def parse_args():
    parser = argparse.ArgumentParser(description="Load Gold Parquet tables into PostgreSQL.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read Gold Parquet and print row counts without connecting to PostgreSQL.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    if args.dry_run:
        results = preview_gold_tables()
        print_preview_results(results)
        return 0

    connection_description = describe_postgres_connection()
    print("Loading Gold Parquet tables into PostgreSQL on " + connection_description)

    load_result = load_gold_tables()
    print_load_results(load_result)

    return 0


if __name__ == "__main__":
    sys.exit(main())
