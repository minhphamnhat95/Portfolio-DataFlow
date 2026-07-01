from datetime import date

import numpy

from src.transformation import portfolio_optimizer
from src.transformation.spark_session import build_spark_session, stop_spark_session


def build_optimizer_test_return_matrix(spark):
    symbols = ["VAS.AX", "CBA.AX", "BHP.AX", "BTCUSDT", "ETHUSDT"]
    price_dates = [
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
        date(2026, 1, 6),
        date(2026, 1, 7),
    ]
    returns_by_symbol = {
        "VAS.AX": [0.0010, 0.0020, 0.0015, -0.0010, 0.0025, 0.0005],
        "CBA.AX": [0.0005, 0.0025, -0.0010, 0.0010, 0.0020, 0.0000],
        "BHP.AX": [-0.0005, 0.0010, 0.0020, -0.0020, 0.0015, 0.0005],
        "BTCUSDT": [0.0100, -0.0080, 0.0120, -0.0060, 0.0040, -0.0030],
        "ETHUSDT": [0.0120, -0.0100, 0.0140, -0.0080, 0.0060, -0.0040],
    }
    rows = []

    for date_index in range(len(price_dates)):
        price_date = price_dates[date_index]

        for symbol in symbols:
            row = {
                "price_date": price_date,
                "symbol": symbol,
                "daily_return": returns_by_symbol[symbol][date_index],
            }
            rows.append(row)

    asset_returns_calendar_df = spark.createDataFrame(rows)
    return_matrix_df = portfolio_optimizer.build_return_matrix(asset_returns_calendar_df, symbols)

    result = {
        "symbols": symbols,
        "return_matrix_df": return_matrix_df,
    }

    return result


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


def test_build_optimizer_inputs_converts_return_matrix_to_numpy_values():
    spark = build_spark_session("test-optimizer-inputs", "local[1]")

    try:
        matrix_result = build_optimizer_test_return_matrix(spark)
        symbols = matrix_result["symbols"]
        return_matrix_df = matrix_result["return_matrix_df"]

        optimizer_inputs = portfolio_optimizer.build_optimizer_inputs(return_matrix_df, symbols)

        assert optimizer_inputs["observation_count"] == 6
        assert optimizer_inputs["lookback_start_date"] == date(2026, 1, 2)
        assert optimizer_inputs["lookback_end_date"] == date(2026, 1, 7)
        assert optimizer_inputs["return_values"].shape == (6, 5)
        assert optimizer_inputs["annual_returns"].shape == (5,)
        assert optimizer_inputs["annual_covariance_matrix"].shape == (5, 5)
    finally:
        stop_spark_session(spark)


def test_calculate_portfolio_metrics_calculates_return_volatility_and_sharpe():
    weights = numpy.array([0.50, 0.50], dtype=float)
    annual_returns = numpy.array([0.10, 0.20], dtype=float)
    annual_covariance_matrix = numpy.array(
        [
            [0.04, 0.00],
            [0.00, 0.09],
        ],
        dtype=float,
    )

    metrics = portfolio_optimizer.calculate_portfolio_metrics(
        weights,
        annual_returns,
        annual_covariance_matrix,
        0.02,
    )

    assert abs(metrics["annual_return"] - 0.15) < 0.000001
    assert abs(metrics["annual_volatility"] - 0.1802775638) < 0.000001
    assert abs(metrics["sharpe_ratio"] - 0.7211102551) < 0.000001


def test_run_slsqp_optimizer_respects_weight_and_crypto_constraints():
    spark = build_spark_session("test-slsqp-optimizer", "local[1]")

    try:
        matrix_result = build_optimizer_test_return_matrix(spark)
        symbols = matrix_result["symbols"]
        return_matrix_df = matrix_result["return_matrix_df"]

        optimizer_inputs = portfolio_optimizer.build_optimizer_inputs(return_matrix_df, symbols)
        optimizer_result = portfolio_optimizer.run_slsqp_optimizer(optimizer_inputs, 0.04)
        optimized_weights = optimizer_result["optimized_weights"]
        crypto_total_weight = portfolio_optimizer.calculate_crypto_total_weight(optimized_weights, symbols)

        assert optimizer_result["success"] is True
        assert abs(float(numpy.sum(optimized_weights)) - 1.0) < 0.000001
        assert crypto_total_weight <= portfolio_optimizer.MAX_CRYPTO_TOTAL_WEIGHT + 0.000001

        for index in range(len(symbols)):
            symbol = symbols[index]
            optimized_weight = optimized_weights[index]

            assert optimized_weight >= -0.000001

            if portfolio_optimizer.is_crypto_symbol(symbol):
                assert optimized_weight <= portfolio_optimizer.MAX_CRYPTO_ASSET_WEIGHT + 0.000001
            else:
                assert optimized_weight <= portfolio_optimizer.MAX_SINGLE_ASSET_WEIGHT + 0.000001
    finally:
        stop_spark_session(spark)


def test_optimize_return_matrix_to_gold_writes_summary_and_weights(tmp_path, monkeypatch):
    spark = build_spark_session("test-optimizer-write-gold", "local[1]")

    try:
        monkeypatch.setattr(portfolio_optimizer, "GOLD_DIR", tmp_path)

        matrix_result = build_optimizer_test_return_matrix(spark)
        symbols = matrix_result["symbols"]
        return_matrix_df = matrix_result["return_matrix_df"]

        result = portfolio_optimizer.optimize_return_matrix_to_gold(
            spark,
            return_matrix_df,
            symbols,
            "overwrite",
            "test_max_sharpe",
            0.04,
        )

        summary_path = tmp_path / "optimized_portfolio_summary"
        weights_path = tmp_path / "optimized_portfolio_weights"

        assert result["optimized_portfolio_summary_row_count"] == 1
        assert result["optimized_portfolio_weights_row_count"] == 5
        assert summary_path.exists()
        assert weights_path.exists()
    finally:
        stop_spark_session(spark)
