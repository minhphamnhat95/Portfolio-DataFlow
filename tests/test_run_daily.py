from src.pipeline import run_daily as run_daily_module
from src.validation.validate_bronze import FileValidationResult


def test_run_daily_validates_after_ingestion(tmp_path, monkeypatch):
    calls = {
        "ingest_called": False,
        "validate_called": False,
        "printed_results": False,
        "printed_summary": False,
    }

    def fake_ensure_data_dirs():
        return None

    def fake_ingest_all(start_date, end_date, run_date, retries):
        calls["ingest_called"] = True
        calls["ingest_start_date"] = start_date
        calls["ingest_end_date"] = end_date
        calls["ingest_retries"] = retries

        results = [
            {
                "source": "binance",
                "symbol": "BTCUSDT",
                "status": "success",
                "records_loaded": 1,
                "path": "fake-path",
                "error": None,
            }
        ]

        return results

    def fake_validate_bronze_file_level(run_date):
        calls["validate_called"] = True
        calls["validation_run_date"] = run_date

        result = FileValidationResult(
            "crypto",
            "binance",
            "BTCUSDT",
            tmp_path / "BTCUSDT.json",
            "valid",
            100,
            [],
        )
        results = [result]

        return results

    def fake_print_validation_results(results):
        calls["printed_results"] = True

    def fake_print_validation_summary(summary):
        calls["printed_summary"] = True

    monkeypatch.setattr(run_daily_module, "ensure_data_dirs", fake_ensure_data_dirs)
    monkeypatch.setattr(run_daily_module, "ingest_all", fake_ingest_all)
    monkeypatch.setattr(run_daily_module, "validate_bronze_file_level", fake_validate_bronze_file_level)
    monkeypatch.setattr(run_daily_module, "print_validation_results", fake_print_validation_results)
    monkeypatch.setattr(run_daily_module, "print_validation_summary", fake_print_validation_summary)

    result = run_daily_module.run_daily(
        start_date="2026-06-01",
        end_date="2026-06-05",
        retries=2,
        validate_after_ingestion=True,
        quarantine_invalid=False,
        validation_failure_threshold=0.25,
    )

    assert calls["ingest_called"]
    assert calls["validate_called"]
    assert calls["printed_results"]
    assert calls["printed_summary"]
    assert calls["ingest_start_date"] == "2026-06-01"
    assert calls["ingest_end_date"] == "2026-06-05"
    assert calls["ingest_retries"] == 2
    assert result["ingestion_successes"] == 1
    assert result["ingestion_failures"] == 0
    assert result["validation_summary"].should_fail_run is False


def test_run_daily_marks_validation_failure_above_threshold(tmp_path, monkeypatch):
    def fake_ensure_data_dirs():
        return None

    def fake_ingest_all(start_date, end_date, run_date, retries):
        results = [
            {
                "source": "binance",
                "symbol": "BTCUSDT",
                "status": "success",
                "records_loaded": 1,
                "path": "fake-path",
                "error": None,
            }
        ]

        return results

    def fake_validate_bronze_file_level(run_date):
        first_result = FileValidationResult(
            "crypto",
            "binance",
            "BTCUSDT",
            tmp_path / "BTCUSDT.json",
            "valid",
            100,
            [],
        )
        second_result = FileValidationResult(
            "crypto",
            "binance",
            "ETHUSDT",
            tmp_path / "ETHUSDT.json",
            "invalid",
            100,
            ["records is empty"],
        )
        results = [first_result, second_result]

        return results

    def fake_print_validation_results(results):
        return None

    def fake_print_validation_summary(summary):
        return None

    monkeypatch.setattr(run_daily_module, "ensure_data_dirs", fake_ensure_data_dirs)
    monkeypatch.setattr(run_daily_module, "ingest_all", fake_ingest_all)
    monkeypatch.setattr(run_daily_module, "validate_bronze_file_level", fake_validate_bronze_file_level)
    monkeypatch.setattr(run_daily_module, "print_validation_results", fake_print_validation_results)
    monkeypatch.setattr(run_daily_module, "print_validation_summary", fake_print_validation_summary)

    result = run_daily_module.run_daily(
        start_date="2026-06-01",
        end_date="2026-06-05",
        retries=2,
        validate_after_ingestion=True,
        quarantine_invalid=False,
        validation_failure_threshold=0.25,
    )

    assert result["validation_summary"].invalid_count == 1
    assert result["validation_summary"].failure_rate == 0.5
    assert result["validation_summary"].should_fail_run
