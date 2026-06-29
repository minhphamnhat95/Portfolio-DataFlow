from datetime import date

from src.pipeline import run_market_pipeline as pipeline_module
from src.validation.validate_bronze import ValidationSummary


def test_run_market_pipeline_calls_steps_in_order(monkeypatch):
    calls = []

    class FakeSparkSession:
        pass

    def fake_run_daily(
        start_date,
        end_date,
        retries,
        validate_after_ingestion,
        quarantine_invalid,
        validation_failure_threshold,
    ):
        calls.append("ingestion")

        validation_summary = ValidationSummary(
            total_files=1,
            valid_count=1,
            invalid_count=0,
            failure_rate=0,
            failure_threshold=validation_failure_threshold,
            should_fail_run=False,
        )

        result = {
            "run_id": "run_test",
            "run_date": "2026-06-29",
            "ingestion_successes": 1,
            "ingestion_failures": 0,
            "validation_summary": validation_summary,
            "quarantined_paths": [],
        }

        return result

    def fake_build_spark_session(app_name, master):
        calls.append("spark_start")
        spark = FakeSparkSession()

        return spark

    def fake_transform_bronze_to_silver_for_run_date(spark, run_date, mode):
        calls.append("silver")

        assert run_date == date(2026, 6, 29)
        assert mode == "overwrite"

        result = {
            "assets_row_count": 11,
            "asset_prices_row_count": 100,
            "fx_rates_row_count": 10,
        }

        return result

    def fake_transform_silver_to_gold(spark, mode):
        calls.append("gold")

        assert mode == "overwrite"

        result = {
            "asset_returns_row_count": 100,
            "portfolio_returns_row_count": 10,
            "portfolio_summary_row_count": 1,
        }

        return result

    def fake_stop_spark_session(spark):
        calls.append("spark_stop")

    def fake_load_gold_tables():
        calls.append("postgres")

        result = {
            "run_id": "load_test",
            "results": [
                {
                    "table_name": "gold.portfolio_summary",
                    "status": "success",
                    "row_count": 1,
                }
            ],
        }

        return result

    monkeypatch.setattr(pipeline_module, "run_daily", fake_run_daily)
    monkeypatch.setattr(pipeline_module, "build_spark_session", fake_build_spark_session)
    monkeypatch.setattr(
        pipeline_module,
        "transform_bronze_to_silver_for_run_date",
        fake_transform_bronze_to_silver_for_run_date,
    )
    monkeypatch.setattr(pipeline_module, "transform_silver_to_gold", fake_transform_silver_to_gold)
    monkeypatch.setattr(pipeline_module, "stop_spark_session", fake_stop_spark_session)
    monkeypatch.setattr(pipeline_module, "load_gold_tables", fake_load_gold_tables)

    result = pipeline_module.run_market_pipeline(
        start_date="2026-06-01",
        end_date="2026-06-10",
        retries=2,
        spark_master="local[1]",
        silver_mode="overwrite",
        gold_mode="overwrite",
    )

    assert calls == ["ingestion", "spark_start", "silver", "gold", "spark_stop", "postgres"]
    assert result["ingestion"]["run_id"] == "run_test"
    assert result["silver"]["asset_prices_row_count"] == 100
    assert result["gold"]["portfolio_summary_row_count"] == 1
    assert result["postgres"]["run_id"] == "load_test"


def test_run_market_pipeline_stops_when_validation_fails(monkeypatch):
    calls = []

    def fake_run_daily(
        start_date,
        end_date,
        retries,
        validate_after_ingestion,
        quarantine_invalid,
        validation_failure_threshold,
    ):
        calls.append("ingestion")

        validation_summary = ValidationSummary(
            total_files=2,
            valid_count=1,
            invalid_count=1,
            failure_rate=0.5,
            failure_threshold=validation_failure_threshold,
            should_fail_run=True,
        )

        result = {
            "run_id": "run_failed",
            "run_date": "2026-06-29",
            "ingestion_successes": 1,
            "ingestion_failures": 0,
            "validation_summary": validation_summary,
            "quarantined_paths": [],
        }

        return result

    def fake_build_spark_session(app_name, master):
        calls.append("spark_start")
        return None

    monkeypatch.setattr(pipeline_module, "run_daily", fake_run_daily)
    monkeypatch.setattr(pipeline_module, "build_spark_session", fake_build_spark_session)

    try:
        pipeline_module.run_market_pipeline(
            start_date="2026-06-01",
            end_date="2026-06-10",
            retries=2,
            spark_master="local[1]",
            silver_mode="overwrite",
            gold_mode="overwrite",
        )
        assert False
    except RuntimeError as error:
        assert str(error) == "Stopping pipeline because Bronze validation failed."

    assert calls == ["ingestion"]
