from __future__ import annotations
from typing import Any
import yfinance as yf


class YahooClient:
    def get_daily_prices(self, symbol: str, start_date: str, end_date: str | None = None) -> dict[str, Any]:
        ticker = yf.Ticker(symbol)
        frame = ticker.history(start=start_date, end=end_date, interval="1d", auto_adjust=False)
        frame = frame.reset_index()
        records = []
        for row in frame.to_dict(orient="records"):
            normalized = {}
            for key, value in row.items():
                normalized[str(key)] = value.isoformat() if hasattr(value, "isoformat") else value
            records.append(normalized)
        return {
            "source": "yahoo",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "interval": "1d",
            "records": records,
        }

