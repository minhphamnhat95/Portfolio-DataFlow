from __future__ import annotations

import argparse
from datetime import date
from uuid import uuid4

from src.ingestion.ingest import ingest_all
from src.utils.config import DEFAULT_START_DATE, ensure_data_dirs
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_daily(start_date: str, end_date: str | None, retries: int) -> None:
    ensure_data_dirs()
    run_id = f"run_{uuid4().hex[:12]}"
    run_date = date.today()

    logger.info("Starting %s from %s to %s", run_id, start_date, end_date or "latest")
    ingestion_results = ingest_all(start_date=start_date, end_date=end_date, run_date=run_date, retries=retries)
    successes = sum(1 for result in ingestion_results if result["status"] == "success")
    failures = len(ingestion_results) - successes
    logger.info("Finished %s with %s successful files and %s failures", run_id, successes, failures)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch market data and store raw Bronze JSON locally.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Historical backfill start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Optional exclusive end date, YYYY-MM-DD.")
    parser.add_argument("--retries", type=int, default=3, help="Number of retry attempts per symbol.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_daily(
        start_date=args.start_date,
        end_date=args.end_date,
        retries=args.retries,
    )


if __name__ == "__main__":
    main()
