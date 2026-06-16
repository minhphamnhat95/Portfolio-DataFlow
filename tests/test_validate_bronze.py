import json
from datetime import date

from src.validation import validate_bronze


def test_locate_expected_bronze_files_builds_all_expected_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(validate_bronze, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(validate_bronze, "EQUITY_SYMBOLS", ["CBA.AX"])
    monkeypatch.setattr(validate_bronze, "CRYPTO_SYMBOLS", ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr(validate_bronze, "FX_SYMBOL", "AUDUSD=X")

    expected = validate_bronze.locate_expected_bronze_files(date(2026, 6, 16))
    actual_symbols = []

    for item in expected:
        actual_symbols.append(item.symbol)

    assert actual_symbols == ["CBA.AX", "AUDUSD=X", "BTCUSDT", "ETHUSDT"]
    assert expected[0].path == tmp_path / "bronze/equities/source=yahoo/date=2026-06-16/CBA.AX.json"
    assert expected[1].path == tmp_path / "bronze/fx/source=yahoo/date=2026-06-16/AUDUSD=X.json"
    assert expected[2].path == tmp_path / "bronze/crypto/source=binance/date=2026-06-16/BTCUSDT.json"


def test_validate_file_accepts_non_empty_json_file(tmp_path):
    path = tmp_path / "BTCUSDT.json"
    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [
            [
                1780272000000,
                "73674.39000000",
                "74092.00000000",
                "70686.68000000",
                "71408.90000000",
                "23921.09184000",
                1780358399999,
                "1723958338.68287760",
                4237773,
                "11600.30396000",
                "835109172.11680740",
                "0",
            ]
        ],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert result.is_valid()
    assert result.file_size_bytes
    assert result.file_size_bytes > 0
    assert result.errors == []


def test_validate_file_reports_missing_file(tmp_path):
    path = tmp_path / "missing.json"
    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)

    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert result.errors == ["file does not exist"]


def test_validate_file_reports_empty_file(tmp_path):
    path = tmp_path / "empty.json"
    path.write_text("", encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert result.errors == ["file is empty"]


def test_validate_file_reports_invalid_json(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not valid", encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert result.errors[0].startswith("file is not valid JSON")


def test_validate_file_reports_missing_top_level_records(tmp_path):
    path = tmp_path / "BTCUSDT.json"
    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "missing top-level field records" in result.errors


def test_validate_file_reports_yahoo_record_missing_price_field(tmp_path):
    path = tmp_path / "CBA.AX.json"
    payload = {
        "source": "yahoo",
        "symbol": "CBA.AX",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [
            {
                "Date": "2026-06-01T00:00:00",
                "Open": 100.0,
                "High": 105.0,
                "Low": 99.0,
                "Volume": 1000,
            }
        ],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("equities", "yahoo", "CBA.AX", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 0 missing field Close" in result.errors


def test_validate_file_reports_yahoo_record_missing_date(tmp_path):
    path = tmp_path / "CBA.AX.json"
    payload = {
        "source": "yahoo",
        "symbol": "CBA.AX",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [
            {
                "Open": 100.0,
                "High": 105.0,
                "Low": 99.0,
                "Close": 102.0,
                "Volume": 1000,
            }
        ],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("equities", "yahoo", "CBA.AX", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 0 missing field Date" in result.errors


def test_validate_file_reports_yahoo_duplicate_date(tmp_path):
    path = tmp_path / "CBA.AX.json"
    first_record = {
        "Date": "2026-06-01T00:00:00",
        "Open": 100.0,
        "High": 105.0,
        "Low": 99.0,
        "Close": 102.0,
        "Volume": 1000,
    }
    second_record = {
        "Date": "2026-06-01T00:00:00",
        "Open": 101.0,
        "High": 106.0,
        "Low": 100.0,
        "Close": 103.0,
        "Volume": 1100,
    }
    payload = {
        "source": "yahoo",
        "symbol": "CBA.AX",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [first_record, second_record],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("equities", "yahoo", "CBA.AX", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 1 has duplicate date 2026-06-01" in result.errors


def test_validate_file_reports_yahoo_date_before_start_date(tmp_path):
    path = tmp_path / "CBA.AX.json"
    payload = {
        "source": "yahoo",
        "symbol": "CBA.AX",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [
            {
                "Date": "2026-05-31T00:00:00",
                "Open": 100.0,
                "High": 105.0,
                "Low": 99.0,
                "Close": 102.0,
                "Volume": 1000,
            }
        ],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("equities", "yahoo", "CBA.AX", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 0 date is before start_date" in result.errors


def test_validate_file_reports_binance_short_record(tmp_path):
    path = tmp_path / "BTCUSDT.json"
    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [[1780272000000, "73674.39000000"]],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 0 has fewer than 12 fields" in result.errors


def test_validate_file_reports_binance_negative_volume(tmp_path):
    path = tmp_path / "BTCUSDT.json"
    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [
            [
                1780272000000,
                "73674.39000000",
                "74092.00000000",
                "70686.68000000",
                "71408.90000000",
                "-1",
                1780358399999,
                "1723958338.68287760",
                4237773,
                "11600.30396000",
                "835109172.11680740",
                "0",
            ]
        ],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 0 field volume is negative" in result.errors


def test_validate_file_reports_binance_duplicate_open_time(tmp_path):
    path = tmp_path / "BTCUSDT.json"
    first_record = [
        1780272000000,
        "73674.39000000",
        "74092.00000000",
        "70686.68000000",
        "71408.90000000",
        "23921.09184000",
        1780358399999,
        "1723958338.68287760",
        4237773,
        "11600.30396000",
        "835109172.11680740",
        "0",
    ]
    second_record = [
        1780272000000,
        "73675.39000000",
        "74093.00000000",
        "70687.68000000",
        "71409.90000000",
        "23922.09184000",
        1780358399999,
        "1723958339.68287760",
        4237774,
        "11601.30396000",
        "835109173.11680740",
        "0",
    ]
    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [first_record, second_record],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 1 has duplicate open_time 1780272000000" in result.errors


def test_validate_file_reports_binance_date_after_end_date(tmp_path):
    path = tmp_path / "BTCUSDT.json"
    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
        "records": [
            [
                1780704000000,
                "73674.39000000",
                "74092.00000000",
                "70686.68000000",
                "71408.90000000",
                "23921.09184000",
                1780790399999,
                "1723958338.68287760",
                4237773,
                "11600.30396000",
                "835109172.11680740",
                "0",
            ]
        ],
    }
    json_text = json.dumps(payload)
    path.write_text(json_text, encoding="utf-8")

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", path)
    result = validate_bronze.validate_file(expected)

    assert not result.is_valid()
    assert "record 0 date is after end_date" in result.errors


def test_quarantine_invalid_files_moves_bad_existing_file(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    rejected_dir = tmp_path / "rejected"
    file_path = bronze_dir / "crypto" / "source=binance" / "date=2026-06-16" / "BTCUSDT.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "source": "binance",
        "symbol": "BTCUSDT",
        "start_date": "2026-06-01",
        "end_date": "2026-06-05",
        "interval": "1d",
    }
    json_text = json.dumps(payload)
    file_path.write_text(json_text, encoding="utf-8")

    monkeypatch.setattr(validate_bronze, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(validate_bronze, "REJECTED_DIR", rejected_dir)

    expected = validate_bronze.ExpectedBronzeFile("crypto", "binance", "BTCUSDT", file_path)
    result = validate_bronze.validate_file(expected)
    quarantined_paths = validate_bronze.quarantine_invalid_files([result])

    rejected_path = rejected_dir / "crypto" / "source=binance" / "date=2026-06-16" / "BTCUSDT.json"

    assert not result.is_valid()
    assert not file_path.exists()
    assert rejected_path.exists()
    assert quarantined_paths == [rejected_path]


def test_build_validation_summary_passes_when_failure_rate_equals_threshold(tmp_path):
    valid_path = tmp_path / "valid.json"
    invalid_path = tmp_path / "invalid.json"

    first_result = validate_bronze.FileValidationResult(
        "crypto",
        "binance",
        "BTCUSDT",
        valid_path,
        "valid",
        100,
        [],
    )
    second_result = validate_bronze.FileValidationResult(
        "crypto",
        "binance",
        "ETHUSDT",
        valid_path,
        "valid",
        100,
        [],
    )
    third_result = validate_bronze.FileValidationResult(
        "crypto",
        "binance",
        "SOLUSDT",
        valid_path,
        "valid",
        100,
        [],
    )
    fourth_result = validate_bronze.FileValidationResult(
        "crypto",
        "binance",
        "BNBUSDT",
        invalid_path,
        "invalid",
        100,
        ["records is empty"],
    )
    results = [first_result, second_result, third_result, fourth_result]

    summary = validate_bronze.build_validation_summary(results, 0.25)

    assert summary.total_files == 4
    assert summary.valid_count == 3
    assert summary.invalid_count == 1
    assert summary.failure_rate == 0.25
    assert not summary.should_fail_run


def test_build_validation_summary_fails_when_failure_rate_is_above_threshold(tmp_path):
    valid_path = tmp_path / "valid.json"
    invalid_path = tmp_path / "invalid.json"

    first_result = validate_bronze.FileValidationResult(
        "crypto",
        "binance",
        "BTCUSDT",
        valid_path,
        "valid",
        100,
        [],
    )
    second_result = validate_bronze.FileValidationResult(
        "crypto",
        "binance",
        "ETHUSDT",
        invalid_path,
        "invalid",
        100,
        ["records is empty"],
    )
    results = [first_result, second_result]

    summary = validate_bronze.build_validation_summary(results, 0.25)

    assert summary.total_files == 2
    assert summary.valid_count == 1
    assert summary.invalid_count == 1
    assert summary.failure_rate == 0.5
    assert summary.should_fail_run
