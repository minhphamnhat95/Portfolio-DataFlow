from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests


class BinanceClient:
    BASE_URL = "https://api.binance.com/api/v3/klines"

    def get_daily_klines(self, symbol: str, start_date: str, end_date: str | None = None) -> dict[str, Any]:
        start_ms = self._date_to_ms(start_date)
        params: dict[str, Any] = {"symbol": symbol, "interval": "1d", "startTime": start_ms, "limit": 1000}
        if end_date:
            params["endTime"] = self._date_to_ms(end_date)

        rows: list[list[Any]] = []
        while True:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < 1000:
                break
            params["startTime"] = int(batch[-1][0]) + 24 * 60 * 60 * 1000
            if end_date and params["startTime"] >= self._date_to_ms(end_date):
                break

        return {
            "source": "binance",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "interval": "1d",
            "records": rows,
        }

    @staticmethod
    def _date_to_ms(date_value: str) -> int:
        dt = datetime.fromisoformat(date_value).replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

