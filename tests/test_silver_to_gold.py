from datetime import date

from pyspark.sql import functions as spark_functions

from src.transformation import silver_to_gold
from src.transformation.spark_session import build_spark_session, stop_spark_session


def add_default_fact_flags(fact_asset_daily_df):
    fact_asset_daily_df = fact_asset_daily_df.withColumn("is_price_observed", spark_functions.lit(True))
    fact_asset_daily_df = fact_asset_daily_df.withColumn("is_price_forward_filled", spark_functions.lit(False))
    fact_asset_daily_df = fact_asset_daily_df.withColumn("is_fx_observed", spark_functions.lit(False))
    fact_asset_daily_df = fact_asset_daily_df.withColumn("is_fx_forward_filled", spark_functions.lit(False))
    fact_asset_daily_df = fact_asset_daily_df.withColumn("source_price_date", spark_functions.col("date_key"))
    fact_asset_daily_df = fact_asset_daily_df.withColumn("source_fx_date", spark_functions.lit(None).cast("date"))

    return fact_asset_daily_df


def test_build_fact_asset_daily_converts_usdt_to_aud_and_calculates_returns():
    spark = build_spark_session("test-gold-fact-asset-daily", "local[1]")

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
        dim_date_df = silver_to_gold.build_dim_date(asset_prices_df)

        fact_asset_daily_df = silver_to_gold.build_fact_asset_daily(asset_prices_df, fx_rates_df, dim_date_df)
        rows = fact_asset_daily_df.orderBy("symbol", "date_key").collect()
        btc_second_day = None

        for row in rows:
            if row.symbol == "BTCUSDT" and row.date_key == date(2026, 1, 2):
                btc_second_day = row

        assert len(rows) == 4
        assert btc_second_day is not None
        assert btc_second_day.symbol == "BTCUSDT"
        assert btc_second_day.close_price_native == 60.0
        assert btc_second_day.close_price_aud == 120.0
        assert abs(btc_second_day.daily_return - 0.2) < 0.000001
    finally:
        stop_spark_session(spark)


def test_build_fact_portfolio_daily_and_summary_metrics(tmp_path, monkeypatch):
    spark = build_spark_session("test-gold-portfolio-facts", "local[1]")

    try:
        asset_return_rows = [
            {
                "symbol": "CBA.AX",
                "date_key": date(2026, 1, 1),
                "close_price_native": 100.0,
                "close_price_aud": 100.0,
                "daily_return": None,
            },
            {
                "symbol": "CBA.AX",
                "date_key": date(2026, 1, 2),
                "close_price_native": 110.0,
                "close_price_aud": 110.0,
                "daily_return": 0.10,
            },
            {
                "symbol": "CBA.AX",
                "date_key": date(2026, 1, 3),
                "close_price_native": 99.0,
                "close_price_aud": 99.0,
                "daily_return": -0.10,
            },
            {
                "symbol": "BTCUSDT",
                "date_key": date(2026, 1, 1),
                "close_price_native": 50.0,
                "close_price_aud": 100.0,
                "daily_return": None,
            },
            {
                "symbol": "BTCUSDT",
                "date_key": date(2026, 1, 2),
                "close_price_native": 60.0,
                "close_price_aud": 120.0,
                "daily_return": 0.20,
            },
            {
                "symbol": "BTCUSDT",
                "date_key": date(2026, 1, 3),
                "close_price_native": 54.0,
                "close_price_aud": 108.0,
                "daily_return": -0.10,
            },
        ]
        weights = [
            {"symbol": "CBA.AX", "weight": 0.50},
            {"symbol": "BTCUSDT", "weight": 0.50},
        ]

        fact_asset_daily_df = spark.createDataFrame(asset_return_rows)
        fact_asset_daily_df = add_default_fact_flags(fact_asset_daily_df)
        weights_df = silver_to_gold.build_portfolio_weights_dataframe(spark, "test_portfolio", weights)
        fact_portfolio_daily_df = silver_to_gold.build_fact_portfolio_daily(fact_asset_daily_df, weights_df)
        fact_portfolio_summary_daily_df = silver_to_gold.build_fact_portfolio_summary_daily(
            fact_portfolio_daily_df,
            0.04,
        )

        portfolio_rows = fact_portfolio_daily_df.orderBy("date_key").collect()
        summary_rows = fact_portfolio_summary_daily_df.orderBy("date_key").collect()
        summary_row = summary_rows[1]

        monkeypatch.setattr(silver_to_gold, "GOLD_DIR", tmp_path / "gold")
        output_path = silver_to_gold.write_gold_table(fact_portfolio_daily_df, "fact_portfolio_daily")
        read_df = spark.read.parquet(str(output_path))

        assert len(portfolio_rows) == 2
        assert len(summary_rows) == 2
        assert abs(portfolio_rows[0].daily_return - 0.15) < 0.000001
        assert abs(portfolio_rows[1].daily_return - -0.10) < 0.000001
        assert abs(portfolio_rows[1].cumulative_return - 0.035) < 0.000001
        assert summary_row.portfolio_name == "test_portfolio"
        assert summary_row.date_key == date(2026, 1, 3)
        assert summary_row.observation_count == 2
        assert abs(summary_row.ytd_return - 0.035) < 0.000001
        assert summary_row.annual_return is None
        assert summary_row.annual_volatility is None
        assert summary_row.sharpe_ratio is None
        assert abs(summary_row.max_drawdown - -0.10) < 0.000001
        assert read_df.count() == 2
    finally:
        stop_spark_session(spark)


def test_build_portfolio_summary_resets_metrics_at_start_of_year():
    spark = build_spark_session("test-gold-portfolio-summary-ytd", "local[1]")

    try:
        portfolio_return_rows = [
            {
                "portfolio_name": "test_portfolio",
                "date_key": date(2025, 12, 31),
                "daily_return": 0.10,
                "cumulative_return": 0.10,
            },
            {
                "portfolio_name": "test_portfolio",
                "date_key": date(2026, 1, 1),
                "daily_return": 0.02,
                "cumulative_return": 0.122,
            },
            {
                "portfolio_name": "test_portfolio",
                "date_key": date(2026, 1, 2),
                "daily_return": 0.04,
                "cumulative_return": 0.16688,
            },
        ]

        fact_portfolio_daily_df = spark.createDataFrame(portfolio_return_rows)
        fact_portfolio_summary_daily_df = silver_to_gold.build_portfolio_summary(fact_portfolio_daily_df, 0.04)
        summary_rows = fact_portfolio_summary_daily_df.orderBy("date_key").collect()
        jan_first_row = summary_rows[1]
        jan_second_row = summary_rows[2]

        assert len(summary_rows) == 3
        assert jan_first_row.date_key == date(2026, 1, 1)
        assert jan_first_row.observation_count == 1
        assert abs(jan_first_row.ytd_return - 0.02) < 0.000001
        assert jan_first_row.annual_return is None
        assert jan_second_row.date_key == date(2026, 1, 2)
        assert jan_second_row.observation_count == 2
        assert abs(jan_second_row.ytd_return - 0.0608) < 0.000001
        assert jan_second_row.annual_return is None
    finally:
        stop_spark_session(spark)


def test_build_portfolio_summary_annualizes_after_minimum_observations():
    spark = build_spark_session("test-gold-portfolio-summary-min-observations", "local[1]")

    try:
        portfolio_return_rows = []

        for day_number in range(1, 31):
            row = {
                "portfolio_name": "test_portfolio",
                "date_key": date(2026, 1, day_number),
                "daily_return": 0.001,
                "cumulative_return": (1.001 ** day_number) - 1.0,
            }
            portfolio_return_rows.append(row)

        fact_portfolio_daily_df = spark.createDataFrame(portfolio_return_rows)
        fact_portfolio_summary_daily_df = silver_to_gold.build_portfolio_summary(fact_portfolio_daily_df, 0.04)
        summary_rows = fact_portfolio_summary_daily_df.orderBy("date_key").collect()
        early_row = summary_rows[0]
        final_row = summary_rows[29]

        assert early_row.annual_return is None
        assert final_row.observation_count == 30
        assert abs(final_row.ytd_return - ((1.001 ** 30) - 1.0)) < 0.000001
        assert final_row.annual_return is not None
        assert final_row.annual_return > 0
    finally:
        stop_spark_session(spark)


def test_build_dim_date_creates_every_calendar_day():
    spark = build_spark_session("test-gold-dim-date", "local[1]")

    try:
        asset_price_rows = [
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 2),
                "close_price": 100.0,
                "currency": "AUD",
            },
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 5),
                "close_price": 110.0,
                "currency": "AUD",
            },
        ]

        asset_prices_df = spark.createDataFrame(asset_price_rows)
        dim_date_df = silver_to_gold.build_dim_date(asset_prices_df)
        rows = dim_date_df.orderBy("date_key").collect()

        assert len(rows) == 4
        assert rows[0].date_key == date(2026, 1, 2)
        assert rows[1].date_key == date(2026, 1, 3)
        assert rows[2].date_key == date(2026, 1, 4)
        assert rows[3].date_key == date(2026, 1, 5)
        assert rows[1].is_weekend is True
    finally:
        stop_spark_session(spark)


def test_build_dim_asset_and_dim_portfolio():
    spark = build_spark_session("test-gold-dimensions", "local[1]")

    try:
        asset_rows = [
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "currency": "AUD",
                "description": "CBA.AX equity",
                "is_active": True,
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "currency": "USDT",
                "description": "BTCUSDT crypto",
                "is_active": True,
            },
        ]

        assets_df = spark.createDataFrame(asset_rows)
        dim_asset_df = silver_to_gold.build_dim_asset(assets_df)
        dim_portfolio_df = silver_to_gold.build_dim_portfolio_dataframe(spark)

        asset_rows = dim_asset_df.orderBy("symbol").collect()
        portfolio_rows = dim_portfolio_df.orderBy("portfolio_name").collect()

        assert len(asset_rows) == 2
        assert asset_rows[0].symbol == "BTCUSDT"
        assert len(portfolio_rows) == 2
    finally:
        stop_spark_session(spark)


def test_fact_asset_daily_forward_fills_prices_and_fx():
    spark = build_spark_session("test-gold-fact-asset-daily-fill", "local[1]")

    try:
        asset_price_rows = [
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 2),
                "close_price": 100.0,
                "currency": "AUD",
            },
            {
                "symbol": "CBA.AX",
                "asset_class": "equity",
                "source": "yahoo",
                "price_date": date(2026, 1, 5),
                "close_price": 110.0,
                "currency": "AUD",
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 2),
                "close_price": 50.0,
                "currency": "USDT",
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 3),
                "close_price": 55.0,
                "currency": "USDT",
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 4),
                "close_price": 60.0,
                "currency": "USDT",
            },
            {
                "symbol": "BTCUSDT",
                "asset_class": "crypto",
                "source": "binance",
                "price_date": date(2026, 1, 5),
                "close_price": 66.0,
                "currency": "USDT",
            },
        ]
        fx_rate_rows = [
            {
                "symbol": "AUDUSD=X",
                "source": "yahoo",
                "rate_date": date(2026, 1, 2),
                "base_currency": "AUD",
                "quote_currency": "USD",
                "rate": 0.5,
            },
            {
                "symbol": "AUDUSD=X",
                "source": "yahoo",
                "rate_date": date(2026, 1, 5),
                "base_currency": "AUD",
                "quote_currency": "USD",
                "rate": 0.6,
            },
        ]

        asset_prices_df = spark.createDataFrame(asset_price_rows)
        fx_rates_df = spark.createDataFrame(fx_rate_rows)
        dim_date_df = silver_to_gold.build_dim_date(asset_prices_df)
        fact_asset_daily_df = silver_to_gold.build_fact_asset_daily(asset_prices_df, fx_rates_df, dim_date_df)
        rows = fact_asset_daily_df.orderBy("symbol", "date_key").collect()

        cba_weekend_row = None
        btc_weekend_row = None
        btc_monday_row = None

        for row in rows:
            if row.symbol == "CBA.AX" and row.date_key == date(2026, 1, 3):
                cba_weekend_row = row

            if row.symbol == "BTCUSDT" and row.date_key == date(2026, 1, 3):
                btc_weekend_row = row

            if row.symbol == "BTCUSDT" and row.date_key == date(2026, 1, 5):
                btc_monday_row = row

        assert len(rows) == 8
        assert cba_weekend_row is not None
        assert cba_weekend_row.close_price_native == 100.0
        assert cba_weekend_row.close_price_aud == 100.0
        assert cba_weekend_row.daily_return == 0.0
        assert cba_weekend_row.is_price_observed is False
        assert cba_weekend_row.is_price_forward_filled is True
        assert cba_weekend_row.is_fx_observed is False
        assert cba_weekend_row.is_fx_forward_filled is False
        assert cba_weekend_row.source_price_date == date(2026, 1, 2)
        assert cba_weekend_row.source_fx_date is None

        assert btc_weekend_row is not None
        assert btc_weekend_row.close_price_native == 55.0
        assert btc_weekend_row.close_price_aud == 110.0
        assert abs(btc_weekend_row.daily_return - 0.10) < 0.000001
        assert btc_weekend_row.is_price_observed is True
        assert btc_weekend_row.is_price_forward_filled is False
        assert btc_weekend_row.is_fx_observed is False
        assert btc_weekend_row.is_fx_forward_filled is True
        assert btc_weekend_row.source_fx_date == date(2026, 1, 2)

        assert btc_monday_row is not None
        assert btc_monday_row.close_price_native == 66.0
        assert btc_monday_row.close_price_aud == 110.0
        assert abs(btc_monday_row.daily_return - -0.08333333333333337) < 0.000001
        assert btc_monday_row.is_fx_observed is True
        assert btc_monday_row.is_fx_forward_filled is False
        assert btc_monday_row.source_fx_date == date(2026, 1, 5)
    finally:
        stop_spark_session(spark)


def test_fact_portfolio_daily_has_rows_and_fill_flags():
    spark = build_spark_session("test-gold-fact-portfolio-daily", "local[1]")

    try:
        asset_return_rows = [
            {
                "symbol": "CBA.AX",
                "date_key": date(2026, 1, 2),
                "close_price_native": 100.0,
                "close_price_aud": 100.0,
                "daily_return": None,
                "is_price_observed": True,
                "is_price_forward_filled": False,
                "is_fx_observed": False,
                "is_fx_forward_filled": False,
                "source_price_date": date(2026, 1, 2),
                "source_fx_date": None,
            },
            {
                "symbol": "CBA.AX",
                "date_key": date(2026, 1, 3),
                "close_price_native": 100.0,
                "close_price_aud": 100.0,
                "daily_return": 0.0,
                "is_price_observed": False,
                "is_price_forward_filled": True,
                "is_fx_observed": False,
                "is_fx_forward_filled": False,
                "source_price_date": date(2026, 1, 2),
                "source_fx_date": None,
            },
            {
                "symbol": "BTCUSDT",
                "date_key": date(2026, 1, 2),
                "close_price_native": 50.0,
                "close_price_aud": 100.0,
                "daily_return": None,
                "is_price_observed": True,
                "is_price_forward_filled": False,
                "is_fx_observed": True,
                "is_fx_forward_filled": False,
                "source_price_date": date(2026, 1, 2),
                "source_fx_date": date(2026, 1, 2),
            },
            {
                "symbol": "BTCUSDT",
                "date_key": date(2026, 1, 3),
                "close_price_native": 55.0,
                "close_price_aud": 110.0,
                "daily_return": 0.10,
                "is_price_observed": True,
                "is_price_forward_filled": False,
                "is_fx_observed": False,
                "is_fx_forward_filled": True,
                "source_price_date": date(2026, 1, 3),
                "source_fx_date": date(2026, 1, 2),
            },
        ]
        weights = [
            {"symbol": "CBA.AX", "weight": 0.50},
            {"symbol": "BTCUSDT", "weight": 0.50},
        ]

        fact_asset_daily_df = spark.createDataFrame(asset_return_rows)
        weights_df = silver_to_gold.build_portfolio_weights_dataframe(spark, "test_portfolio", weights)
        fact_portfolio_daily_df = silver_to_gold.build_fact_portfolio_daily(fact_asset_daily_df, weights_df)
        fact_portfolio_summary_daily_df = silver_to_gold.build_fact_portfolio_summary_daily(
            fact_portfolio_daily_df,
            0.04,
        )

        portfolio_rows = fact_portfolio_daily_df.orderBy("date_key").collect()
        summary_row = fact_portfolio_summary_daily_df.collect()[0]

        assert len(portfolio_rows) == 1
        assert portfolio_rows[0].date_key == date(2026, 1, 3)
        assert abs(portfolio_rows[0].daily_return - 0.05) < 0.000001
        assert abs(portfolio_rows[0].cumulative_return - 0.05) < 0.000001
        assert portfolio_rows[0].has_forward_filled_price is True
        assert portfolio_rows[0].has_forward_filled_fx is True
        assert portfolio_rows[0].observed_asset_count == 1
        assert portfolio_rows[0].forward_filled_price_count == 1
        assert portfolio_rows[0].forward_filled_fx_count == 1
        assert summary_row.portfolio_name == "test_portfolio"
        assert summary_row.date_key == date(2026, 1, 3)
        assert summary_row.observation_count == 1
        assert abs(summary_row.ytd_return - 0.05) < 0.000001
        assert summary_row.annual_return is None
    finally:
        stop_spark_session(spark)
