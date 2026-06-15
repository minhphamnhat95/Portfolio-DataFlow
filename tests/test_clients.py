from __future__ import annotations

import pandas as pd

from src.ingestion.binance_client import BinanceClient
from src.ingestion.yahoo_client import YahooClient


def test_yahoo_client_normalizes_dataframe_records(monkeypatch):
    class FakeTicker:
        def __init__(self, symbol):
            self.symbol = symbol

        def history(self, start, end, interval, auto_adjust):
            return pd.DataFrame(
                [{"Open": 1.0, "High": 2.0, "Low": 0.5, "Close": 1.5, "Volume": 100}],
                index=pd.DatetimeIndex(["2026-06-12"], name="Date"),
            )

    monkeypatch.setattr("src.ingestion.yahoo_client.yf.Ticker", FakeTicker)

    payload = YahooClient().get_daily_prices("CBA.AX", "2026-01-01")

    assert payload["source"] == "yahoo"
    assert payload["symbol"] == "CBA.AX"
    assert payload["records"][0]["Date"].startswith("2026-06-12")
    assert payload["records"][0]["Close"] == 1.5


def test_binance_client_paginates_daily_klines(monkeypatch):
    calls = []

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    def fake_get(url, params, timeout):
        calls.append(params.copy())
        if len(calls) == 1:
            return FakeResponse([[1704067200000, "1", "2", "0.5", "1.5", "10", 0, 0, 5]])
        return FakeResponse([])

    monkeypatch.setattr("src.ingestion.binance_client.requests.get", fake_get)

    payload = BinanceClient().get_daily_klines("BTCUSDT", "2024-01-01")

    assert payload["source"] == "binance"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["records"][0][4] == "1.5"
    assert len(calls) == 1

