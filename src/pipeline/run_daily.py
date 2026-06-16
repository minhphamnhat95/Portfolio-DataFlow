import argparse
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.ingestion.ingest import ingest_all
from src.utils.config import DEFAULT_START_DATE, VALIDATION_FAILURE_THRESHOLD, ensure_data_dirs
from src.utils.logger import get_logger
from src.validation.validate_bronze import (
    build_validation_summary,
    print_validation_results,
    print_validation_summary,
    quarantine_invalid_files,
    validate_bronze_file_level,
)

logger = get_logger(__name__)


def run_daily(
    start_date,
    end_date,
    retries,
    validate_after_ingestion=True,
    quarantine_invalid=False,
    validation_failure_threshold=VALIDATION_FAILURE_THRESHOLD,
):
    ensure_data_dirs()

    run_id = f"run_{uuid4().hex[:12]}"
    run_date = date.today()

    logger.info("Starting %s from %s to %s", run_id, start_date, end_date or "latest")

    ingestion_results = ingest_all(
        start_date=start_date,
        end_date=end_date,
        run_date=run_date,
        retries=retries,
    )

    successes = 0

    for result in ingestion_results:
        if result["status"] == "success":
            successes = successes + 1

    total_results = len(ingestion_results)
    failures = total_results - successes

    logger.info("Finished %s with %s successful files and %s failures", run_id, successes, failures)

    validation_summary = None
    quarantined_paths = []

    if validate_after_ingestion:
        logger.info("Validating Bronze files for run date %s", run_date.isoformat())

        validation_results = validate_bronze_file_level(run_date)
        print_validation_results(validation_results)

        validation_summary = build_validation_summary(validation_results, validation_failure_threshold)
        print_validation_summary(validation_summary)

        if quarantine_invalid:
            quarantined_paths = quarantine_invalid_files(validation_results)
            logger.info("Quarantined %s invalid Bronze files", len(quarantined_paths))

        if validation_summary.should_fail_run:
            logger.error(
                "Validation failed because %s of %s expected files were invalid",
                validation_summary.invalid_count,
                validation_summary.total_files,
            )
        else:
            logger.info("Validation passed for %s expected Bronze files", validation_summary.total_files)

    result = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "ingestion_successes": successes,
        "ingestion_failures": failures,
        "validation_summary": validation_summary,
        "quarantined_paths": quarantined_paths,
    }

    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch market data and store raw Bronze JSON locally.")

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
        help="Number of retry attempts per symbol.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip Bronze validation after ingestion.",
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
        help="Fail the run if the invalid file rate is greater than this number. Default is 0.25.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    validate_after_ingestion = not args.skip_validation

    result = run_daily(
        start_date=args.start_date,
        end_date=args.end_date,
        retries=args.retries,
        validate_after_ingestion=validate_after_ingestion,
        quarantine_invalid=args.quarantine_invalid,
        validation_failure_threshold=args.validation_failure_threshold,
    )

    validation_summary = result["validation_summary"]

    if validation_summary is not None:
        if validation_summary.should_fail_run:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
