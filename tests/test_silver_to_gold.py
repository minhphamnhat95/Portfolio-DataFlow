from datetime import date

from src.transformation import silver_to_gold
from src.transformation.spark_session import build_spark_session, stop_spark_session


def test_build_asset_returns_converts_usdt_to_aud_and_calculates_returns():
    spark = build_spark_session("test-gold-asset-returns", "local[1]")

    try:
        asset_price_rows = [
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 1),
                "close_price": 100.0,
                "currency": "AUD",
            },
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 2),
                "close_price": 110.0,
                "currency": "AUD",
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 1),
                "close_price": 50.0,
                "currency": "USDT",
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 2),
                "close_price": 60.0,
                "currency": "USDT",
            },
        ]
        fx_rate_rows = [
            {
                "symbol": "AUDUSD=X",
                "source": "yahoo",
                "rate_date": date(2026, 1, 1),
                "base_currency": "AUD",
                "quote_currency": "USD",
                "rate": 0.5,
            },
            {
                "symbol": "AUDUSD=X",
                "source": "yahoo",
                "rate_date": date(2026, 1, 2),
                "base_currency": "AUD",
                "quote_currency": "USD",
                "rate": 0.5,
            },
        ]

        asset_prices_df = spark.createDataFrame(asset_price_rows)
        fx_rates_df = spark.createDataFrame(fx_rate_rows)

        asset_returns_df = silver_to_gold.build_asset_returns(asset_prices_df, fx_rates_df)
        rows = asset_returns_df.orderBy("symbol", "price_date").collect()
        btc_second_day = None

        for row in rows:
            if row.symbol == "BTCUSDT" and row.price_date == date(2026, 1, 2):
                btc_second_day = row

        assert len(rows) == 4
        assert btc_second_day is not None
        assert btc_second_day.symbol == "BTCUSDT"
        assert btc_second_day.close_price_native == 60.0
        assert btc_second_day.close_price_aud == 120.0
        assert abs(btc_second_day.daily_return - 0.2) < 0.000001
    finally:
        stop_spark_session(spark)


def test_build_portfolio_returns_and_summary_metrics(tmp_path, monkeypatch):
    spark = build_spark_session("test-gold-portfolio-metrics", "local[1]")

    try:
        asset_return_rows = [
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 1),
                "close_price_native": 100.0,
                "close_price_aud": 100.0,
                "currency": "AUD",
                "daily_return": None,
            },
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 2),
                "close_price_native": 110.0,
                "close_price_aud": 110.0,
                "currency": "AUD",
                "daily_return": 0.10,
            },
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 3),
                "close_price_native": 99.0,
                "close_price_aud": 99.0,
                "currency": "AUD",
                "daily_return": -0.10,
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 1),
                "close_price_native": 50.0,
                "close_price_aud": 100.0,
                "currency": "USDT",
                "daily_return": None,
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 2),
                "close_price_native": 60.0,
                "close_price_aud": 120.0,
                "currency": "USDT",
                "daily_return": 0.20,
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 3),
                "close_price_native": 54.0,
                "close_price_aud": 108.0,
                "currency": "USDT",
                "daily_return": -0.10,
            },
        ]
        weights = [
            {"symbol": "CBA.AX", "weight": 0.50},
            {"symbol": "BTCUSDT", "weight": 0.50},
        ]

        asset_returns_df = spark.createDataFrame(asset_return_rows)
        weights_df = silver_to_gold.build_portfolio_weights_dataframe(spark, "test_portfolio", weights)
        portfolio_returns_df = silver_to_gold.build_portfolio_returns(asset_returns_df, weights_df)
        portfolio_summary_df = silver_to_gold.build_portfolio_summary(portfolio_returns_df, 0.04)

        portfolio_rows = portfolio_returns_df.orderBy("price_date").collect()
        summary_row = portfolio_summary_df.collect()[0]

        monkeypatch.setattr(silver_to_gold, "GOLD_DIR", tmp_path / "gold")
        output_path = silver_to_gold.write_gold_table(portfolio_returns_df, "portfolio_returns")
        read_df = spark.read.parquet(str(output_path))

        assert len(portfolio_rows) == 2
        assert abs(portfolio_rows[0].daily_return - 0.15) < 0.000001
        assert abs(portfolio_rows[1].daily_return - -0.10) < 0.000001
        assert abs(portfolio_rows[1].cumulative_return - 0.035) < 0.000001
        assert summary_row.portfolio_name == "test_portfolio"
        assert summary_row.observation_count == 2
        assert abs(summary_row.annual_return - 6.3) < 0.000001
        assert summary_row.annual_volatility > 0
        assert summary_row.sharpe_ratio > 0
        assert abs(summary_row.max_drawdown - -0.10) < 0.000001
        assert read_df.count() == 2
    finally:
        stop_spark_session(spark)
