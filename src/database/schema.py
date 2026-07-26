import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.database.connection import (
    close_postgres_connection,
    describe_postgres_connection,
    open_postgres_connection,
)


CREATE_GOLD_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS gold;
"""

CREATE_AUDIT_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS audit;
"""

DROP_LEGACY_GOLD_TABLES_SQL = """
DROP TABLE IF EXISTS gold.asset_returns;
DROP TABLE IF EXISTS gold.date_spine;
DROP TABLE IF EXISTS gold.asset_returns_calendar;
DROP TABLE IF EXISTS gold.portfolio_returns;
DROP TABLE IF EXISTS gold.portfolio_returns_calendar;
DROP TABLE IF EXISTS gold.portfolio_summary;
DROP TABLE IF EXISTS gold.portfolio_summary_calendar;
DROP TABLE IF EXISTS gold.optimized_portfolio_summary;
DROP TABLE IF EXISTS gold.optimized_portfolio_weights;
DROP TABLE IF EXISTS gold.fact_portfolio_summary_daily;
"""

CREATE_GOLD_DIM_DATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key DATE NOT NULL,
    calendar_year BIGINT NOT NULL,
    calendar_quarter BIGINT NOT NULL,
    calendar_month BIGINT NOT NULL,
    month_name TEXT NOT NULL,
    day_of_month BIGINT NOT NULL,
    day_of_week BIGINT NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    PRIMARY KEY (date_key)
);
"""

CREATE_GOLD_DIM_ASSET_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.dim_asset (
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    source TEXT NOT NULL,
    currency TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL,
    PRIMARY KEY (symbol)
);
"""

CREATE_GOLD_DIM_PORTFOLIO_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.dim_portfolio (
    portfolio_name TEXT NOT NULL,
    portfolio_type TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL,
    PRIMARY KEY (portfolio_name)
);
"""

CREATE_GOLD_FACT_ASSET_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.fact_asset_daily (
    symbol TEXT NOT NULL,
    date_key DATE NOT NULL,
    close_price_native DOUBLE PRECISION NOT NULL,
    close_price_aud DOUBLE PRECISION NOT NULL,
    daily_return DOUBLE PRECISION,
    is_price_observed BOOLEAN NOT NULL,
    is_price_forward_filled BOOLEAN NOT NULL,
    is_fx_observed BOOLEAN NOT NULL,
    is_fx_forward_filled BOOLEAN NOT NULL,
    source_price_date DATE NOT NULL,
    source_fx_date DATE,
    PRIMARY KEY (symbol, date_key)
);
"""

CREATE_GOLD_FACT_PORTFOLIO_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.fact_portfolio_daily (
    portfolio_name TEXT NOT NULL,
    date_key DATE NOT NULL,
    daily_return DOUBLE PRECISION NOT NULL,
    cumulative_return DOUBLE PRECISION NOT NULL,
    has_forward_filled_price BOOLEAN NOT NULL,
    has_forward_filled_fx BOOLEAN NOT NULL,
    observed_asset_count BIGINT NOT NULL,
    forward_filled_price_count BIGINT NOT NULL,
    forward_filled_fx_count BIGINT NOT NULL,
    PRIMARY KEY (portfolio_name, date_key)
);
"""

CREATE_GOLD_FACT_PORTFOLIO_SUMMARY_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.fact_portfolio_summary_daily (
    portfolio_name TEXT NOT NULL,
    date_key DATE NOT NULL,
    observation_count BIGINT NOT NULL,
    ytd_return DOUBLE PRECISION,
    annual_return DOUBLE PRECISION,
    annual_volatility DOUBLE PRECISION,
    risk_free_rate DOUBLE PRECISION NOT NULL,
    sharpe_ratio DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    PRIMARY KEY (portfolio_name, date_key)
);
"""

CREATE_GOLD_FACT_OPTIMIZER_SUMMARY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.fact_optimizer_summary (
    portfolio_name TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    lookback_start_date DATE NOT NULL,
    lookback_end_date DATE NOT NULL,
    observation_count BIGINT NOT NULL,
    risk_free_rate DOUBLE PRECISION NOT NULL,
    annual_return DOUBLE PRECISION,
    annual_volatility DOUBLE PRECISION,
    sharpe_ratio DOUBLE PRECISION,
    optimizer_method TEXT NOT NULL,
    constraint_set_name TEXT NOT NULL,
    optimization_success BOOLEAN NOT NULL,
    optimization_message TEXT,
    PRIMARY KEY (portfolio_name, as_of_date)
);
"""

CREATE_GOLD_FACT_OPTIMIZER_WEIGHTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.fact_optimizer_weights (
    portfolio_name TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    current_weight DOUBLE PRECISION NOT NULL,
    optimized_weight DOUBLE PRECISION NOT NULL,
    weight_difference DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (portfolio_name, as_of_date, symbol)
);
"""

CREATE_AUDIT_LOAD_LOGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit.load_logs (
    run_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    row_count BIGINT,
    error_message TEXT
);
"""


def get_schema_statements():
    statements = []

    statements.append(CREATE_GOLD_SCHEMA_SQL)
    statements.append(CREATE_AUDIT_SCHEMA_SQL)

    return statements


def get_table_statements():
    statements = []

    statements.append(DROP_LEGACY_GOLD_TABLES_SQL)
    statements.append(CREATE_GOLD_DIM_DATE_TABLE_SQL)
    statements.append(CREATE_GOLD_DIM_ASSET_TABLE_SQL)
    statements.append(CREATE_GOLD_DIM_PORTFOLIO_TABLE_SQL)
    statements.append(CREATE_GOLD_FACT_ASSET_DAILY_TABLE_SQL)
    statements.append(CREATE_GOLD_FACT_PORTFOLIO_DAILY_TABLE_SQL)
    statements.append(CREATE_GOLD_FACT_PORTFOLIO_SUMMARY_DAILY_TABLE_SQL)
    statements.append(CREATE_GOLD_FACT_OPTIMIZER_SUMMARY_TABLE_SQL)
    statements.append(CREATE_GOLD_FACT_OPTIMIZER_WEIGHTS_TABLE_SQL)
    statements.append(CREATE_AUDIT_LOAD_LOGS_TABLE_SQL)

    return statements


def execute_sql_statements(connection, statements):
    cursor = connection.cursor()

    try:
        for statement in statements:
            cursor.execute(statement)
    finally:
        cursor.close()


def create_schemas(connection):
    statements = get_schema_statements()
    execute_sql_statements(connection, statements)


def create_tables(connection):
    statements = get_table_statements()
    execute_sql_statements(connection, statements)


def create_database_objects(connection):
    try:
        create_schemas(connection)
        create_tables(connection)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def initialize_database():
    connection = None

    try:
        connection = open_postgres_connection()
        create_database_objects(connection)
    finally:
        close_postgres_connection(connection)


def print_sql_plan():
    schema_statements = get_schema_statements()
    table_statements = get_table_statements()

    print("Schema statements:")

    for statement in schema_statements:
        print(statement.strip())
        print()

    print("Table statements:")

    for statement in table_statements:
        print(statement.strip())
        print()


def parse_args():
    parser = argparse.ArgumentParser(description="Create PostgreSQL schemas and tables for serving Gold data.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL statements without connecting to PostgreSQL.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    if args.dry_run:
        print_sql_plan()
        return 0

    connection_description = describe_postgres_connection()
    print("Creating PostgreSQL objects on " + connection_description)

    initialize_database()

    print("PostgreSQL schemas and tables are ready.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
