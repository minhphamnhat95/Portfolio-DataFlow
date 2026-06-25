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

CREATE_GOLD_ASSET_RETURNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.asset_returns (
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    source TEXT NOT NULL,
    price_date DATE NOT NULL,
    close_price_native DOUBLE PRECISION NOT NULL,
    close_price_aud DOUBLE PRECISION NOT NULL,
    currency TEXT NOT NULL,
    daily_return DOUBLE PRECISION,
    PRIMARY KEY (symbol, price_date)
);
"""

CREATE_GOLD_PORTFOLIO_RETURNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.portfolio_returns (
    portfolio_name TEXT NOT NULL,
    price_date DATE NOT NULL,
    daily_return DOUBLE PRECISION NOT NULL,
    cumulative_return DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (portfolio_name, price_date)
);
"""

CREATE_GOLD_PORTFOLIO_SUMMARY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gold.portfolio_summary (
    portfolio_name TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    observation_count BIGINT NOT NULL,
    annual_return DOUBLE PRECISION,
    annual_volatility DOUBLE PRECISION,
    risk_free_rate DOUBLE PRECISION NOT NULL,
    sharpe_ratio DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    PRIMARY KEY (portfolio_name)
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

    statements.append(CREATE_GOLD_ASSET_RETURNS_TABLE_SQL)
    statements.append(CREATE_GOLD_PORTFOLIO_RETURNS_TABLE_SQL)
    statements.append(CREATE_GOLD_PORTFOLIO_SUMMARY_TABLE_SQL)
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
