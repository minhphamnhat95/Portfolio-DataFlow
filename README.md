# Financial Market Data Ingestion

Small local ingestion project that fetches ASX equity, crypto, and FX reference data, stores raw API payloads as local JSON files, and validates the Bronze files.

```mermaid
flowchart LR
  A[Yahoo Finance / Binance] --> B[Python ingestion clients]
  B --> C[Local Bronze JSON files]
  C --> D[Bronze validation]
```

## Scope

- Equities: `CBA.AX`, `BHP.AX`, `CSL.AX`, `WOW.AX`, `VAS.AX`
- Crypto: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BNBUSDT`, `XRPUSDT`
- FX reference: `AUDUSD=X`
- Default backfill: `2023-01-01`
- Current output: raw Bronze JSON plus validation results

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

Fetch data, store raw JSON files locally, and validate the Bronze files:

```powershell
python -m src.pipeline.run_daily --start-date 2023-01-01
```

Fetch a smaller date range:

```powershell
python -m src.pipeline.run_daily --start-date 2026-06-01 --end-date 2026-06-15
```

Outputs:

- `data/bronze/...`: raw JSON payloads from Yahoo and Binance
- `data/rejected/...`: invalid Bronze files moved only when quarantine is enabled

Example folder layout:

```text
data/
  bronze/
    equities/
      source=yahoo/
        date=YYYY-MM-DD/
          CBA.AX.json
    crypto/
      source=binance/
        date=YYYY-MM-DD/
          BTCUSDT.json
          ETHUSDT.json
          SOLUSDT.json
          BNBUSDT.json
          XRPUSDT.json
    fx/
      source=yahoo/
        date=YYYY-MM-DD/
          AUDUSD=X.json
```

Run validation for an existing Bronze run date:

```powershell
python -m src.validation.validate_bronze --run-date 2026-06-16
```

Quarantine invalid files only when you explicitly want to move them:

```powershell
python -m src.pipeline.run_daily --start-date 2026-06-01 --quarantine-invalid
```

The default validation rule fails the command if more than `25%` of expected Bronze files are invalid.

## Tests

```powershell
python -m pytest
```

## Spark

The reusable Spark session helper is:

```text
src/transformation/spark_session.py
```

Quick smoke test:

```powershell
python -c "from src.transformation.spark_session import build_spark_session, stop_spark_session; spark = build_spark_session('spark-smoke-test', 'local[1]'); print(spark.range(1).count()); stop_spark_session(spark)"
```

## Silver Transformation

Transform Bronze JSON files into Silver Parquet tables:

```powershell
python -m src.transformation.bronze_to_silver --run-date 2026-06-16 --master local[1]
```

This currently handles:

```text
Config asset list
  -> symbol/asset_class/source/currency metadata
  -> data/silver/assets/

Bronze Yahoo equities JSON
  -> Date/Open/High/Low/Close/Volume
  -> data/silver/asset_prices/

Bronze Binance crypto JSON
  -> open_time/open/high/low/close/volume
  -> data/silver/asset_prices/

Bronze Yahoo FX JSON
  -> Date/Close
  -> data/silver/fx_rates/
```

## Future Enhancements

- Gold transformations
- PostgreSQL serving tables
- Airflow orchestration
- dbt models and tests
