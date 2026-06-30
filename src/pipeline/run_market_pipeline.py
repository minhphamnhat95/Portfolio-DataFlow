import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.database.load_gold import load_gold_tables
from src.pipeline.run_daily import run_daily 
from src.transformation.bronze_to_silver import transform_bronze_to_silver_for_run_date
from src.transformation.silver_to_gold import transform_silver_to_gold
from src.transformation.spark_session import build_spark_session
from src.transformation.spark_session import stop_spark_session
from src.utils.config import DEFAULT_START_DATE
from src.utils.config import VALIDATION_FAILURE_THRESHOLD


def run_market_pipeline(
    start_date,
    end_date,
    retries,
    spark_master,
    silver_mode,
    gold_mode,
    quarantine_invalid=False,
    validation_failure_threshold=VALIDATION_FAILURE_THRESHOLD,
    skip_ingestion=False,
    skip_silver=False,
    skip_gold=False,
    skip_postgres=False,
):
    result = {
        "ingestion": None,
        "silver": None,
        "gold": None,
        "postgres": None,
    }

    ingestion_result = None

    if skip_ingestion:
        print("Skipping Bronze ingestion and validation.")
    else:
        print("Step 1/4: Ingesting Bronze JSON and validating Bronze files.")
        ingestion_result = run_daily(
            start_date=start_date,
            end_date=end_date,
            retries=retries,
            validate_after_ingestion=True,
            quarantine_invalid=quarantine_invalid,
            validation_failure_threshold=validation_failure_threshold,
        )
        result["ingestion"] = ingestion_result

        validation_summary = ingestion_result["validation_summary"]

        if validation_summary is not None:
            if validation_summary.should_fail_run:
                raise RuntimeError("Stopping pipeline because Bronze validation failed.")

    run_date = determine_run_date(ingestion_result)
    spark = None

    try:
        if skip_silver:
            print("Skipping Bronze to Silver transformation.")
        else:
            print("Step 2/4: Transforming Bronze JSON to Silver Parquet.")
            spark = build_spark_session("finance-market-data-local-pipeline", spark_master)
            silver_result = transform_bronze_to_silver_for_run_date(spark, run_date, silver_mode)
            result["silver"] = silver_result
            print_silver_result(silver_result)

        if skip_gold:
            print("Skipping Silver to Gold transformation.")
        else:
            print("Step 3/4: Transforming Silver Parquet to Gold metrics.")

            if spark is None:
                spark = build_spark_session("finance-market-data-local-pipeline", spark_master)

            gold_result = transform_silver_to_gold(spark, gold_mode)
            result["gold"] = gold_result
            print_gold_result(gold_result)
    finally:
        stop_spark_session(spark)

    if skip_postgres:
        print("Skipping Gold to PostgreSQL load.")
    else:
        print("Step 4/4: Loading Gold Parquet to PostgreSQL.")
        postgres_result = load_gold_tables()
        result["postgres"] = postgres_result
        print_postgres_result(postgres_result)

    print("Market pipeline finished.")

    return result


def determine_run_date(ingestion_result):
    if ingestion_result is None:
        from datetime import date

        run_date = date.today()
    else:
        from datetime import date

        run_date = date.fromisoformat(ingestion_result["run_date"])

    return run_date


def print_silver_result(result):
    print("Silver assets rows: " + str(result["assets_row_count"]))
    print("Silver asset price rows: " + str(result["asset_prices_row_count"]))
    print("Silver FX rate rows: " + str(result["fx_rates_row_count"]))


def print_gold_result(result):
    print("Gold asset return rows: " + str(result["asset_returns_row_count"]))
    print("Gold portfolio return rows: " + str(result["portfolio_returns_row_count"]))
    print("Gold portfolio summary rows: " + str(result["portfolio_summary_row_count"]))


def print_postgres_result(result):
    print("PostgreSQL load run: " + result["run_id"])

    for table_result in result["results"]:
        line = (
            table_result["table_name"]
            + " -> "
            + table_result["status"]
            + " ("
            + str(table_result["row_count"])
            + " rows)"
        )
        print(line)


def parse_args():
    parser = argparse.ArgumentParser(description="Run the local market data pipeline end to end.")

    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Historical backfill start date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional exclusive end date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retry attempts per ingested symbol.",
    )
    parser.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master value.",
    )
    parser.add_argument(
        "--silver-mode",
        default="overwrite",
        help="Spark write mode for Silver tables.",
    )
    parser.add_argument(
        "--gold-mode",
        default="overwrite",
        help="Spark write mode for Gold tables.",
    )
    parser.add_argument(
        "--quarantine-invalid",
        action="store_true",
        help="Move invalid Bronze files to data/rejected after validation.",
    )
    parser.add_argument(
        "--validation-failure-threshold",
        type=float,
        default=VALIDATION_FAILURE_THRESHOLD,
        help="Fail the run if the invalid Bronze file rate is greater than this number.",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip Bronze ingestion and validation.",
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip Bronze to Silver transformation.",
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip Silver to Gold transformation.",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip PostgreSQL load.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    run_market_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        retries=args.retries,
        spark_master=args.spark_master,
        silver_mode=args.silver_mode,
        gold_mode=args.gold_mode,
        quarantine_invalid=args.quarantine_invalid,
        validation_failure_threshold=args.validation_failure_threshold,
        skip_ingestion=args.skip_ingestion,
        skip_silver=args.skip_silver,
        skip_gold=args.skip_gold,
        skip_postgres=args.skip_postgres,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
