import argparse
import math
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import numpy
from pyspark.sql import functions as spark_functions
from scipy.optimize import minimize

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.transformation.schemas import (
    gold_optimized_portfolio_summary_schema,
    gold_optimized_portfolio_weight_schema,
)
from src.transformation.spark_session import build_spark_session, stop_spark_session
from src.utils.config import (
    CALENDAR_DAYS_PER_YEAR,
    FIXED_PORTFOLIO_WEIGHTS,
    GOLD_DIR,
    RISK_FREE_RATE,
)


OPTIMIZER_METHOD = "scipy_slsqp_max_sharpe"
CONSTRAINT_SET_NAME = "personal_mvp"
DEFAULT_OPTIMIZED_PORTFOLIO_NAME = "max_sharpe_calendar"
MAX_SINGLE_ASSET_WEIGHT = 0.60
MAX_CRYPTO_TOTAL_WEIGHT = 0.25
MAX_CRYPTO_ASSET_WEIGHT = 0.15
WEIGHT_TOLERANCE = 0.000001


def read_gold_asset_returns_calendar(spark):
    path = GOLD_DIR / "asset_returns_calendar"
    asset_returns_calendar_df = spark.read.parquet(str(path))

    return asset_returns_calendar_df


def get_default_optimizer_symbols():
    symbols = []

    for weight in FIXED_PORTFOLIO_WEIGHTS:
        symbol = weight["symbol"]
        symbols.append(symbol)

    return symbols


def build_symbol_filter(symbols):
    symbol_filter = None

    for symbol in symbols:
        current_symbol_filter = spark_functions.col("symbol") == spark_functions.lit(symbol)

        if symbol_filter is None:
            symbol_filter = current_symbol_filter
        else:
            symbol_filter = symbol_filter | current_symbol_filter

    return symbol_filter


def build_symbol_column(symbol):
    column_name = "`" + symbol + "`"
    column = spark_functions.col(column_name)

    return column


def validate_return_matrix_symbols(symbols):
    if symbols is None:
        raise ValueError("At least one symbol is required to build the return matrix.")

    if len(symbols) == 0:
        raise ValueError("At least one symbol is required to build the return matrix.")


def build_return_matrix(asset_returns_calendar_df, symbols, start_date=None, end_date=None):
    validate_return_matrix_symbols(symbols)

    selected_df = asset_returns_calendar_df.select(
        spark_functions.col("price_date"),
        spark_functions.col("symbol"),
        spark_functions.col("daily_return"),
    )

    symbol_filter = build_symbol_filter(symbols)
    selected_df = selected_df.filter(symbol_filter)

    if start_date is not None:
        selected_df = selected_df.filter(
            spark_functions.col("price_date") >= spark_functions.lit(start_date).cast("date")
        )

    if end_date is not None:
        selected_df = selected_df.filter(
            spark_functions.col("price_date") <= spark_functions.lit(end_date).cast("date")
        )

    selected_df = selected_df.filter(spark_functions.col("daily_return").isNotNull())

    matrix_df = selected_df.groupBy("price_date").pivot("symbol", symbols).agg(
        spark_functions.first("daily_return", True)
    )

    complete_matrix_df = matrix_df

    for symbol in symbols:
        symbol_column = build_symbol_column(symbol)
        complete_matrix_df = complete_matrix_df.filter(symbol_column.isNotNull())

    selected_columns = []
    selected_columns.append(spark_functions.col("price_date"))

    for symbol in symbols:
        symbol_column = build_symbol_column(symbol)
        selected_column = symbol_column.cast("double").alias(symbol)
        selected_columns.append(selected_column)

    return_matrix_df = complete_matrix_df.select(selected_columns)
    return_matrix_df = return_matrix_df.orderBy("price_date")

    return return_matrix_df


def is_crypto_symbol(symbol):
    is_crypto = symbol.endswith("USDT")

    return is_crypto


def build_current_weight_lookup():
    current_weight_lookup = {}

    for weight in FIXED_PORTFOLIO_WEIGHTS:
        symbol = weight["symbol"]
        current_weight = float(weight["weight"])
        current_weight_lookup[symbol] = current_weight

    return current_weight_lookup


def get_current_weights_for_symbols(symbols):
    current_weight_lookup = build_current_weight_lookup()
    current_weights = []

    for symbol in symbols:
        current_weight = current_weight_lookup.get(symbol, 0.0)
        current_weights.append(current_weight)

    return numpy.array(current_weights, dtype=float)


def calculate_crypto_total_weight(weights, symbols):
    crypto_total_weight = 0.0

    for index in range(len(symbols)):
        symbol = symbols[index]

        if is_crypto_symbol(symbol):
            crypto_total_weight = crypto_total_weight + float(weights[index])

    return crypto_total_weight


def build_optimizer_bounds(symbols):
    bounds = []

    for symbol in symbols:
        upper_bound = MAX_SINGLE_ASSET_WEIGHT

        if is_crypto_symbol(symbol):
            upper_bound = MAX_CRYPTO_ASSET_WEIGHT

        bound = (0.0, upper_bound)
        bounds.append(bound)

    return bounds


def validate_optimizer_feasibility(symbols):
    bounds = build_optimizer_bounds(symbols)
    total_capacity = 0.0
    crypto_capacity = 0.0

    for index in range(len(symbols)):
        symbol = symbols[index]
        upper_bound = bounds[index][1]

        if is_crypto_symbol(symbol):
            crypto_capacity = crypto_capacity + upper_bound
        else:
            total_capacity = total_capacity + upper_bound

    crypto_capacity = min(crypto_capacity, MAX_CRYPTO_TOTAL_WEIGHT)
    total_capacity = total_capacity + crypto_capacity

    if total_capacity + WEIGHT_TOLERANCE < 1.0:
        raise ValueError("Optimizer constraints are infeasible for the selected symbols.")


def are_weights_feasible(weights, symbols):
    total_weight = float(numpy.sum(weights))

    if abs(total_weight - 1.0) > WEIGHT_TOLERANCE:
        return False

    bounds = build_optimizer_bounds(symbols)

    for index in range(len(symbols)):
        weight = float(weights[index])
        lower_bound = bounds[index][0]
        upper_bound = bounds[index][1]

        if weight < lower_bound - WEIGHT_TOLERANCE:
            return False

        if weight > upper_bound + WEIGHT_TOLERANCE:
            return False

    crypto_total_weight = calculate_crypto_total_weight(weights, symbols)

    if crypto_total_weight > MAX_CRYPTO_TOTAL_WEIGHT + WEIGHT_TOLERANCE:
        return False

    return True


def build_initial_weights(symbols):
    validate_optimizer_feasibility(symbols)

    current_weights = get_current_weights_for_symbols(symbols)
    current_total_weight = float(numpy.sum(current_weights))

    if current_total_weight > 0.0:
        normalized_current_weights = current_weights / current_total_weight

        if are_weights_feasible(normalized_current_weights, symbols):
            return normalized_current_weights

    asset_count = len(symbols)
    equal_weight = 1.0 / asset_count
    equal_weights = numpy.full(asset_count, equal_weight, dtype=float)

    if are_weights_feasible(equal_weights, symbols):
        return equal_weights

    raise ValueError("Could not build a feasible starting weight vector for the selected symbols.")


def build_optimizer_constraints(symbols):
    constraints = []

    weight_sum_constraint = {
        "type": "eq",
        "fun": lambda weights: float(numpy.sum(weights)) - 1.0,
    }
    constraints.append(weight_sum_constraint)

    crypto_total_constraint = {
        "type": "ineq",
        "fun": lambda weights: MAX_CRYPTO_TOTAL_WEIGHT - calculate_crypto_total_weight(weights, symbols),
    }
    constraints.append(crypto_total_constraint)

    return constraints


def normalize_date_value(value):
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()

    if hasattr(value, "date"):
        value = value.date()

    return value


def build_optimizer_inputs(return_matrix_df, symbols, annualization_days=CALENDAR_DAYS_PER_YEAR):
    return_matrix_pandas = return_matrix_df.toPandas()
    observation_count = len(return_matrix_pandas)

    if observation_count < 2:
        raise ValueError("At least two complete return observations are required for optimization.")

    return_values = return_matrix_pandas[symbols].to_numpy(dtype=float)

    daily_mean_returns = numpy.mean(return_values, axis=0)
    daily_covariance_matrix = numpy.cov(return_values, rowvar=False)

    if len(symbols) == 1:
        daily_covariance_matrix = numpy.array([[float(daily_covariance_matrix)]])

    annual_returns = daily_mean_returns * annualization_days
    annual_covariance_matrix = daily_covariance_matrix * annualization_days

    lookback_start_date = return_matrix_pandas["price_date"].min()
    lookback_end_date = return_matrix_pandas["price_date"].max()
    lookback_start_date = normalize_date_value(lookback_start_date)
    lookback_end_date = normalize_date_value(lookback_end_date)

    optimizer_inputs = {
        "symbols": symbols,
        "return_values": return_values,
        "annual_returns": annual_returns,
        "annual_covariance_matrix": annual_covariance_matrix,
        "lookback_start_date": lookback_start_date,
        "lookback_end_date": lookback_end_date,
        "observation_count": observation_count,
    }

    return optimizer_inputs


def calculate_portfolio_metrics(weights, annual_returns, annual_covariance_matrix, risk_free_rate):
    portfolio_return = float(numpy.dot(weights, annual_returns))

    weighted_covariance = numpy.dot(annual_covariance_matrix, weights)
    portfolio_variance = float(numpy.dot(weights, weighted_covariance))

    if portfolio_variance < 0.0 and portfolio_variance > -0.000000000001:
        portfolio_variance = 0.0

    if portfolio_variance < 0.0:
        portfolio_volatility = None
        sharpe_ratio = None
    else:
        portfolio_volatility = math.sqrt(portfolio_variance)

        if portfolio_volatility <= 0.0:
            sharpe_ratio = None
        else:
            sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_volatility
            sharpe_ratio = float(sharpe_ratio)

    metrics = {
        "annual_return": portfolio_return,
        "annual_volatility": portfolio_volatility,
        "sharpe_ratio": sharpe_ratio,
    }

    return metrics


def calculate_negative_sharpe(weights, annual_returns, annual_covariance_matrix, risk_free_rate):
    metrics = calculate_portfolio_metrics(
        weights,
        annual_returns,
        annual_covariance_matrix,
        risk_free_rate,
    )

    sharpe_ratio = metrics["sharpe_ratio"]

    if sharpe_ratio is None:
        return 1000000000.0

    if numpy.isnan(sharpe_ratio):
        return 1000000000.0

    negative_sharpe = -1.0 * sharpe_ratio

    return negative_sharpe


def clean_optimized_weights(weights):
    cleaned_weights = []

    for weight in weights:
        cleaned_weight = float(weight)

        if abs(cleaned_weight) < 0.0000000001:
            cleaned_weight = 0.0

        cleaned_weights.append(cleaned_weight)

    cleaned_weights = numpy.array(cleaned_weights, dtype=float)
    total_weight = float(numpy.sum(cleaned_weights))

    if total_weight > 0.0:
        cleaned_weights = cleaned_weights / total_weight

    return cleaned_weights


def run_slsqp_optimizer(optimizer_inputs, risk_free_rate=RISK_FREE_RATE):
    symbols = optimizer_inputs["symbols"]
    annual_returns = optimizer_inputs["annual_returns"]
    annual_covariance_matrix = optimizer_inputs["annual_covariance_matrix"]

    initial_weights = build_initial_weights(symbols)
    bounds = build_optimizer_bounds(symbols)
    constraints = build_optimizer_constraints(symbols)

    result = minimize(
        calculate_negative_sharpe,
        initial_weights,
        args=(annual_returns, annual_covariance_matrix, risk_free_rate),
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={
            "maxiter": 1000,
            "ftol": 0.000000000001,
        },
    )

    optimized_weights = clean_optimized_weights(result.x)

    if not are_weights_feasible(optimized_weights, symbols):
        raise RuntimeError("Optimizer returned weights that do not satisfy the configured constraints.")

    metrics = calculate_portfolio_metrics(
        optimized_weights,
        annual_returns,
        annual_covariance_matrix,
        risk_free_rate,
    )

    optimizer_result = {
        "success": bool(result.success),
        "message": str(result.message),
        "optimized_weights": optimized_weights,
        "annual_return": metrics["annual_return"],
        "annual_volatility": metrics["annual_volatility"],
        "sharpe_ratio": metrics["sharpe_ratio"],
    }

    return optimizer_result


def build_optimizer_output_rows(
    symbols,
    optimizer_inputs,
    optimizer_result,
    portfolio_name=DEFAULT_OPTIMIZED_PORTFOLIO_NAME,
    risk_free_rate=RISK_FREE_RATE,
):
    as_of_date = optimizer_inputs["lookback_end_date"]
    current_weight_lookup = build_current_weight_lookup()

    summary_row = {
        "portfolio_name": portfolio_name,
        "as_of_date": as_of_date,
        "lookback_start_date": optimizer_inputs["lookback_start_date"],
        "lookback_end_date": optimizer_inputs["lookback_end_date"],
        "observation_count": int(optimizer_inputs["observation_count"]),
        "risk_free_rate": float(risk_free_rate),
        "annual_return": optimizer_result["annual_return"],
        "annual_volatility": optimizer_result["annual_volatility"],
        "sharpe_ratio": optimizer_result["sharpe_ratio"],
        "optimizer_method": OPTIMIZER_METHOD,
        "constraint_set_name": CONSTRAINT_SET_NAME,
        "optimization_success": optimizer_result["success"],
        "optimization_message": optimizer_result["message"],
    }

    weight_rows = []

    for index in range(len(symbols)):
        symbol = symbols[index]
        current_weight = current_weight_lookup.get(symbol, 0.0)
        optimized_weight = float(optimizer_result["optimized_weights"][index])
        weight_difference = optimized_weight - current_weight

        weight_row = {
            "portfolio_name": portfolio_name,
            "as_of_date": as_of_date,
            "symbol": symbol,
            "current_weight": float(current_weight),
            "optimized_weight": optimized_weight,
            "weight_difference": weight_difference,
        }
        weight_rows.append(weight_row)

    output_rows = {
        "summary_rows": [summary_row],
        "weight_rows": weight_rows,
    }

    return output_rows


def write_optimizer_table(spark, rows, schema, table_name, mode="overwrite"):
    output_path = GOLD_DIR / table_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if mode.lower() == "overwrite":
        remove_existing_output_path(output_path)

    dataframe = spark.createDataFrame(rows, schema)
    dataframe.write.mode(mode).parquet(str(output_path))

    row_count = len(rows)

    result = {
        "output_path": str(output_path),
        "row_count": row_count,
    }

    return result


def remove_existing_output_path(output_path):
    if not output_path.exists():
        return

    max_attempts = 5

    for attempt_number in range(max_attempts):
        try:
            if output_path.is_dir():
                shutil.rmtree(output_path, onexc=make_path_writable_and_retry)
            else:
                os.chmod(output_path, stat.S_IWRITE)
                output_path.unlink()

            return
        except OSError as error:
            is_last_attempt = attempt_number == max_attempts - 1

            if is_last_attempt:
                powershell_result = remove_existing_output_path_with_powershell(output_path)

                if powershell_result["removed"]:
                    return

                message = (
                    "Could not remove existing optimizer output folder: "
                    + str(output_path)
                    + ". Close Spark, Power BI, Explorer preview, or any process reading this folder, then rerun. "
                    + "Original error: "
                    + str(error)
                )
                if powershell_result["error_message"] is not None:
                    message = message + " PowerShell fallback error: " + powershell_result["error_message"]

                raise OSError(message) from error

            time.sleep(0.5)


def make_path_writable_and_retry(function, path, excinfo):
    os.chmod(path, stat.S_IWRITE)
    function(path)


def remove_existing_output_path_with_powershell(output_path):
    result = {
        "removed": False,
        "error_message": None,
    }

    if not sys.platform.startswith("win"):
        return result

    quoted_output_path = quote_powershell_literal_path(str(output_path))
    command = "Remove-Item -LiteralPath " + quoted_output_path + " -Recurse -Force"

    process = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            command,
        ],
        capture_output=True,
        text=True,
    )

    if process.returncode == 0 and not output_path.exists():
        result["removed"] = True
        return result

    error_message = process.stderr.strip()
    if error_message == "":
        error_message = process.stdout.strip()

    if error_message == "":
        error_message = "PowerShell returned exit code " + str(process.returncode)

    result["error_message"] = error_message

    return result


def quote_powershell_literal_path(path):
    escaped_path = path.replace("'", "''")
    quoted_path = "'" + escaped_path + "'"

    return quoted_path


def optimize_return_matrix_to_gold(
    spark,
    return_matrix_df,
    symbols,
    mode="overwrite",
    portfolio_name=DEFAULT_OPTIMIZED_PORTFOLIO_NAME,
    risk_free_rate=RISK_FREE_RATE,
):
    optimizer_inputs = build_optimizer_inputs(return_matrix_df, symbols)
    optimizer_result = run_slsqp_optimizer(optimizer_inputs, risk_free_rate)

    output_rows = build_optimizer_output_rows(
        symbols,
        optimizer_inputs,
        optimizer_result,
        portfolio_name,
        risk_free_rate,
    )

    summary_result = write_optimizer_table(
        spark,
        output_rows["summary_rows"],
        gold_optimized_portfolio_summary_schema(),
        "optimized_portfolio_summary",
        mode,
    )
    weights_result = write_optimizer_table(
        spark,
        output_rows["weight_rows"],
        gold_optimized_portfolio_weight_schema(),
        "optimized_portfolio_weights",
        mode,
    )

    result = {
        "optimized_portfolio_summary_row_count": summary_result["row_count"],
        "optimized_portfolio_summary_output_path": summary_result["output_path"],
        "optimized_portfolio_weights_row_count": weights_result["row_count"],
        "optimized_portfolio_weights_output_path": weights_result["output_path"],
        "optimization_success": optimizer_result["success"],
        "optimization_message": optimizer_result["message"],
        "optimized_sharpe_ratio": optimizer_result["sharpe_ratio"],
    }

    return result


def optimize_portfolio_from_gold(
    spark,
    mode="overwrite",
    symbols=None,
    start_date=None,
    end_date=None,
    portfolio_name=DEFAULT_OPTIMIZED_PORTFOLIO_NAME,
):
    if symbols is None:
        symbols = get_default_optimizer_symbols()

    asset_returns_calendar_df = read_gold_asset_returns_calendar(spark)
    return_matrix_df = build_return_matrix(asset_returns_calendar_df, symbols, start_date, end_date)

    result = optimize_return_matrix_to_gold(
        spark,
        return_matrix_df,
        symbols,
        mode,
        portfolio_name,
        RISK_FREE_RATE,
    )

    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Optimize portfolio weights by maximizing historical Sharpe ratio.")
    parser.add_argument(
        "--master",
        default="local[1]",
        help="Spark master URL.",
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        help="Spark write mode for optimizer Gold tables.",
    )
    parser.add_argument(
        "--portfolio-name",
        default=DEFAULT_OPTIMIZED_PORTFOLIO_NAME,
        help="Name to store for the optimized portfolio.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help="Symbol to include. Repeat this option for multiple symbols. Defaults to fixed portfolio symbols.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional first return date to include, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional last return date to include, formatted as YYYY-MM-DD.",
    )
    parser.add_argument(
        "--show-rows",
        type=int,
        default=0,
        help="Number of return matrix rows to print for inspection before optimization.",
    )

    args = parser.parse_args()
    return args


def print_optimizer_result(result):
    print(
        "Wrote "
        + str(result["optimized_portfolio_summary_row_count"])
        + " rows to "
        + result["optimized_portfolio_summary_output_path"]
    )
    print(
        "Wrote "
        + str(result["optimized_portfolio_weights_row_count"])
        + " rows to "
        + result["optimized_portfolio_weights_output_path"]
    )
    print("Optimization success: " + str(result["optimization_success"]))
    print("Optimization message: " + str(result["optimization_message"]))
    print("Optimized Sharpe ratio: " + str(result["optimized_sharpe_ratio"]))


def main():
    args = parse_args()
    spark = build_spark_session("portfolio-max-sharpe-optimizer", args.master)

    try:
        asset_returns_calendar_df = read_gold_asset_returns_calendar(spark)

        symbols = args.symbols
        if symbols is None:
            symbols = get_default_optimizer_symbols()

        return_matrix_df = build_return_matrix(
            asset_returns_calendar_df,
            symbols,
            args.start_date,
            args.end_date,
        )

        if args.show_rows > 0:
            print("Return matrix symbols: " + ", ".join(symbols))
            return_matrix_df.show(args.show_rows, truncate=False)

        result = optimize_return_matrix_to_gold(
            spark,
            return_matrix_df,
            symbols,
            args.mode,
            args.portfolio_name,
            RISK_FREE_RATE,
        )

        print_optimizer_result(result)
    finally:
        stop_spark_session(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
