from datetime import date

from src.transformation import portfolio_optimizer
from src.transformation.spark_session import build_spark_session, stop_spark_session


def test_build_return_matrix_pivots_symbols_into_columns_and_keeps_complete_dates():
    spark = build_spark_session("test-portfolio-return-matrix", "local[1]")

    try:
        asset_return_rows = [
            {
                "price_date": date(2026, 1, 1),
                "symbol": "CBA.AX",
                "daily_return": None,
            },
            {
                "price_date": date(2026, 1, 1),
                "symbol": "BTCUSDT",
                "daily_return": None,
            },
            {
                "price_date": date(2026, 1, 2),
                "symbol": "CBA.AX",
                "daily_return": 0.01,
            },
            {
                "price_date": date(2026, 1, 2),
                "symbol": "BTCUSDT",
                "daily_return": 0.02,
            },
            {
                "price_date": date(2026, 1, 3),
                "symbol": "CBA.AX",
                "daily_return": 0.03,
            },
            {
                "price_date": date(2026, 1, 3),
                "symbol": "ETHUSDT",
                "daily_return": 0.50,
            },
        ]

        asset_returns_calendar_df = spark.createDataFrame(asset_return_rows)
        symbols = ["CBA.AX", "BTCUSDT"]

        return_matrix_df = portfolio_optimizer.build_return_matrix(asset_returns_calendar_df, symbols)
        rows = return_matrix_df.collect()
        row = rows[0].asDict()

        assert return_matrix_df.columns == ["price_date", "CBA.AX", "BTCUSDT"]
        assert len(rows) == 1
        assert row["price_date"] == date(2026, 1, 2)
        assert abs(row["CBA.AX"] - 0.01) < 0.000001
        assert abs(row["BTCUSDT"] - 0.02) < 0.000001
    finally:
        stop_spark_session(spark)


def test_get_default_optimizer_symbols_uses_fixed_portfolio_symbols():
    symbols = portfolio_optimizer.get_default_optimizer_symbols()

    assert symbols == ["VAS.AX", "CBA.AX", "BHP.AX", "BTCUSDT", "ETHUSDT"]
