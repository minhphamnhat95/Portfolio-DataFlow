# Financial Market Data Ingestion

Small local ingestion project that fetches ASX equity, crypto, and FX reference data and stores the raw API payloads as local JSON files.

```mermaid
flowchart LR
  A[Yahoo Finance / Binance] --> B[Python ingestion clients]
  B --> C[Local Bronze JSON files]
```

## Scope

- Equities: `CBA.AX`, `BHP.AX`, `CSL.AX`, `WOW.AX`, `VAS.AX`
- Crypto: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`
- FX reference: `AUDUSD=X`
- Default backfill: `2023-01-01`
- Current output: raw JSON only

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

Fetch data and store raw JSON files locally:

```powershell
python -m src.pipeline.run_daily --start-date 2023-01-01
```

Fetch a smaller date range:

```powershell
python -m src.pipeline.run_daily --start-date 2026-06-01 --end-date 2026-06-15
```

Outputs:

- `data/bronze/...`: raw JSON payloads from Yahoo and Binance

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
    fx/
      source=yahoo/
        date=YYYY-MM-DD/
          AUDUSD=X.json
```

## Tests

```powershell
pytest
```

## Future Enhancements

- Bronze validation and quarantine
- Silver and Gold transformations
- PostgreSQL serving tables
- Airflow orchestration
- dbt models and tests
