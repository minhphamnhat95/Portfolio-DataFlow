from datetime import datetime, timezone

import requests


class BinanceClient:
    BASE_URL = "https://api.binance.com/api/v3/klines"

    def get_daily_klines(self, symbol, start_date, end_date=None):
        start_ms = self._date_to_ms(start_date)

        params = {
            "symbol": symbol,
            "interval": "1d",
            "startTime": start_ms,
            "limit": 1000,
        }

        if end_date:
            end_ms = self._date_to_ms(end_date)
            params["endTime"] = end_ms

        rows = []

        while True:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()

            batch = response.json()

            if not batch:
                break

            rows.extend(batch)

            if len(batch) < 1000:
                break

            last_open_time = int(batch[-1][0])
            next_start_time = last_open_time + 24 * 60 * 60 * 1000
            params["startTime"] = next_start_time

            if end_date:
                end_ms = self._date_to_ms(end_date)

                if params["startTime"] >= end_ms:
                    break

        payload = {
            "source": "binance",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "interval": "1d",
            "records": rows,
        }

        return payload

    @staticmethod
    def _date_to_ms(date_value):
        date_time = datetime.fromisoformat(date_value)
        utc_date_time = date_time.replace(tzinfo=timezone.utc)
        timestamp_ms = int(utc_date_time.timestamp() * 1000)

        return timestamp_ms
