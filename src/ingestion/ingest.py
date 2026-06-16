from time import sleep

from src.ingestion.binance_client import BinanceClient
from src.ingestion.bronze_writer import write_bronze
from src.ingestion.yahoo_client import YahooClient
from src.utils.config import CRYPTO_SYMBOLS, EQUITY_SYMBOLS, FX_SYMBOL
from src.utils.logger import get_logger

logger = get_logger(__name__)


def ingest_all(start_date, end_date, run_date, retries=3):
    yahoo = YahooClient()
    binance = BinanceClient()
    results = []

    for symbol in EQUITY_SYMBOLS:
        result = _with_retry(
            client=yahoo,
            source="yahoo",
            symbol=symbol,
            asset_class="equities",
            start_date=start_date,
            end_date=end_date,
            run_date=run_date,
            retries=retries,
        )
        results.append(result)

    fx_result = _with_retry(
        client=yahoo,
        source="yahoo",
        symbol=FX_SYMBOL,
        asset_class="fx",
        start_date=start_date,
        end_date=end_date,
        run_date=run_date,
        retries=retries,
    )
    results.append(fx_result)

    for symbol in CRYPTO_SYMBOLS:
        result = _with_retry(
            client=binance,
            source="binance",
            symbol=symbol,
            asset_class="crypto",
            start_date=start_date,
            end_date=end_date,
            run_date=run_date,
            retries=retries,
        )
        results.append(result)

    return results


def _with_retry(client, source, symbol, asset_class, start_date, end_date, run_date, retries):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            payload = _fetch_payload(client, source, symbol, start_date, end_date)
            path = write_bronze(asset_class, source, symbol, payload, run_date)
            record_count = len(payload.get("records", []))

            logger.info("Ingested %s %s with %s records", source, symbol, record_count)

            result = _build_success_result(source, symbol, record_count, path)
            return result

        except Exception as exc:
            last_error = str(exc)
            logger.warning("Attempt %s/%s failed for %s: %s", attempt, retries, symbol, last_error)

            wait_seconds = min(attempt * 2, 10)
            sleep(wait_seconds)

    result = _build_failure_result(source, symbol, last_error)
    return result


def _fetch_payload(client, source, symbol, start_date, end_date):
    if source == "yahoo":
        payload = client.get_daily_prices(symbol, start_date, end_date)
    elif source == "binance":
        payload = client.get_daily_klines(symbol, start_date, end_date)
    else:
        raise ValueError(f"Unsupported source: {source}")

    return payload


def _build_success_result(source, symbol, record_count, path):
    result = {
        "source": source,
        "symbol": symbol,
        "status": "success",
        "records_loaded": record_count,
        "path": str(path),
        "error": None,
    }

    return result


def _build_failure_result(source, symbol, error):
    result = {
        "source": source,
        "symbol": symbol,
        "status": "failed",
        "records_loaded": 0,
        "path": None,
        "error": error,
    }

    return result
