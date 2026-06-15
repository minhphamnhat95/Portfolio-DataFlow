from __future__ import annotations

from datetime import date
from time import sleep

from src.ingestion.binance_client import BinanceClient
from src.ingestion.bronze_writer import write_bronze
from src.ingestion.yahoo_client import YahooClient
from src.utils.config import CRYPTO_SYMBOLS, EQUITY_SYMBOLS, FX_SYMBOL
from src.utils.logger import get_logger

logger = get_logger(__name__)


def ingest_all(start_date: str, end_date: str | None, run_date: date, retries: int = 3) -> list[dict[str, object]]:
    yahoo = YahooClient()
    binance = BinanceClient()
    results: list[dict[str, object]] = []

    for symbol in EQUITY_SYMBOLS:
        results.append(_with_retry("yahoo", symbol, lambda s=symbol: yahoo.get_daily_prices(s, start_date, end_date), "equities", run_date, retries))

    results.append(_with_retry("yahoo", FX_SYMBOL, lambda: yahoo.get_daily_prices(FX_SYMBOL, start_date, end_date), "fx", run_date, retries))

    for symbol in CRYPTO_SYMBOLS:
        results.append(_with_retry("binance", symbol, lambda s=symbol: binance.get_daily_klines(s, start_date, end_date), "crypto", run_date, retries))

    return results


def _with_retry(source: str, symbol: str, fetcher, asset_class: str, run_date: date, retries: int) -> dict[str, object]:
    last_error: str | None = None
    for attempt in range(1, retries + 1):
        try:
            payload = fetcher()
            path = write_bronze(asset_class, source, symbol, payload, run_date)
            record_count = len(payload.get("records", []))
            logger.info("Ingested %s %s with %s records", source, symbol, record_count)
            return {"source": source, "symbol": symbol, "status": "success", "records_loaded": record_count, "path": str(path), "error": None}
        except Exception as exc:  # External APIs fail in wonderfully uncreative ways.
            last_error = str(exc)
            logger.warning("Attempt %s/%s failed for %s: %s", attempt, retries, symbol, last_error)
            sleep(min(attempt * 2, 10))
    return {"source": source, "symbol": symbol, "status": "failed", "records_loaded": 0, "path": None, "error": last_error}

