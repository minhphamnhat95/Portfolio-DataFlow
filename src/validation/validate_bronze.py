import argparse
import json
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path

if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))

from src.utils.config import (
    BRONZE_DIR,
    CRYPTO_SYMBOLS,
    EQUITY_SYMBOLS,
    FX_SYMBOL,
    REJECTED_DIR,
    VALIDATION_FAILURE_THRESHOLD,
)


class ExpectedBronzeFile:
    def __init__(self, asset_class, source, symbol, path):
        self.asset_class = asset_class
        self.source = source
        self.symbol = symbol
        self.path = path


class FileValidationResult:
    def __init__(self, asset_class, source, symbol, path, status, file_size_bytes, errors):
        self.asset_class = asset_class
        self.source = source
        self.symbol = symbol
        self.path = path
        self.status = status
        self.file_size_bytes = file_size_bytes
        self.errors = errors

    def is_valid(self):
        is_status_valid = self.status == "valid"
        return is_status_valid


class ValidationSummary:
    def __init__(self, total_files, valid_count, invalid_count, failure_rate, failure_threshold, should_fail_run):
        self.total_files = total_files
        self.valid_count = valid_count
        self.invalid_count = invalid_count
        self.failure_rate = failure_rate
        self.failure_threshold = failure_threshold
        self.should_fail_run = should_fail_run


def locate_expected_bronze_files(run_date):
    date_partition = f"date={run_date.isoformat()}"
    expected = []

    for symbol in EQUITY_SYMBOLS:
        file_name = f"{_safe_symbol(symbol)}.json"
        path = BRONZE_DIR / "equities" / "source=yahoo" / date_partition / file_name

        expected_file = ExpectedBronzeFile(
            asset_class="equities",
            source="yahoo",
            symbol=symbol,
            path=path,
        )

        expected.append(expected_file)

    fx_file_name = f"{_safe_symbol(FX_SYMBOL)}.json"
    fx_path = BRONZE_DIR / "fx" / "source=yahoo" / date_partition / fx_file_name

    fx_expected_file = ExpectedBronzeFile(
        asset_class="fx",
        source="yahoo",
        symbol=FX_SYMBOL,
        path=fx_path,
    )

    expected.append(fx_expected_file)

    for symbol in CRYPTO_SYMBOLS:
        file_name = f"{_safe_symbol(symbol)}.json"
        path = BRONZE_DIR / "crypto" / "source=binance" / date_partition / file_name

        expected_file = ExpectedBronzeFile(
            asset_class="crypto",
            source="binance",
            symbol=symbol,
            path=path,
        )

        expected.append(expected_file)

    return expected


def validate_bronze_file_level(run_date):
    results = []
    expected_files = locate_expected_bronze_files(run_date)

    for expected_file in expected_files:
        result = validate_file(expected_file)
        results.append(result)

    return results


def validate_file(expected_file):
    errors = []
    file_size = None
    path = expected_file.path

    if path.suffix.lower() != ".json":
        errors.append("file extension is not .json")

    if not path.exists():
        errors.append("file does not exist")
        result = _create_result(expected_file, None, errors)
        return result

    if not path.is_file():
        errors.append("path is not a file")
        result = _create_result(expected_file, None, errors)
        return result

    try:
        file_size = path.stat().st_size
    except OSError as exc:
        errors.append(f"cannot read file metadata: {exc}")
        result = _create_result(expected_file, None, errors)
        return result

    if file_size == 0:
        errors.append("file is empty")
        result = _create_result(expected_file, file_size, errors)
        return result

    payload = None

    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except json.JSONDecodeError as exc:
        errors.append(f"file is not valid JSON: {exc.msg}")
    except OSError as exc:
        errors.append(f"cannot open file: {exc}")

    if payload is not None:
        validate_payload_structure(expected_file, payload, errors)

    result = _create_result(expected_file, file_size, errors)
    return result


def validate_payload_structure(expected_file, payload, errors):
    if not isinstance(payload, dict):
        errors.append("top-level JSON is not an object")
        return

    _validate_required_top_level_field(payload, "source", errors)
    _validate_required_top_level_field(payload, "symbol", errors)
    _validate_required_top_level_field(payload, "start_date", errors)
    _validate_required_top_level_field(payload, "end_date", errors)
    _validate_required_top_level_field(payload, "interval", errors)
    has_records = _validate_required_top_level_field(payload, "records", errors)

    if "source" in payload:
        if payload["source"] != expected_file.source:
            errors.append(f"source mismatch: expected {expected_file.source}, got {payload['source']}")

    if "symbol" in payload:
        if payload["symbol"] != expected_file.symbol:
            errors.append(f"symbol mismatch: expected {expected_file.symbol}, got {payload['symbol']}")

    start_date = None
    end_date = None

    if "start_date" in payload:
        start_date = _parse_payload_date(payload["start_date"], "start_date", errors)

    if "end_date" in payload:
        if payload["end_date"] is not None:
            end_date = _parse_payload_date(payload["end_date"], "end_date", errors)

    if not has_records:
        return

    records = payload["records"]

    if not isinstance(records, list):
        errors.append("records is not a list")
        return

    if len(records) == 0:
        errors.append("records is empty")
        return

    if expected_file.source == "yahoo":
        validate_yahoo_records(records, errors, start_date, end_date)
    elif expected_file.source == "binance":
        validate_binance_records(records, errors, start_date, end_date)
    else:
        errors.append(f"unsupported source for record validation: {expected_file.source}")


def validate_yahoo_records(records, errors, start_date, end_date):
    record_index = 0
    seen_dates = set()

    for record in records:
        if not isinstance(record, dict):
            errors.append(f"record {record_index} is not an object")
            record_index = record_index + 1
            continue

        _validate_required_record_field(record, "Date", record_index, errors)

        record_date = None

        if "Date" in record:
            record_date = _parse_record_date(record["Date"], record_index, "Date", errors)

        if record_date is not None:
            _validate_duplicate_date(record_date, seen_dates, record_index, errors)
            _validate_record_date_range(record_date, start_date, end_date, record_index, errors)

        _get_non_negative_number_from_record(record, "Open", record_index, errors)
        _get_non_negative_number_from_record(record, "High", record_index, errors)
        _get_non_negative_number_from_record(record, "Low", record_index, errors)
        _get_non_negative_number_from_record(record, "Close", record_index, errors)
        _get_non_negative_number_from_record(record, "Volume", record_index, errors)

        record_index = record_index + 1


def validate_binance_records(records, errors, start_date, end_date):
    record_index = 0
    seen_open_times = set()

    for record in records:
        if not isinstance(record, list):
            errors.append(f"record {record_index} is not a list")
            record_index = record_index + 1
            continue

        if len(record) < 12:
            errors.append(f"record {record_index} has fewer than 12 fields")
            record_index = record_index + 1
            continue

        open_time = _get_non_negative_integer_from_list(record, 0, "open_time", record_index, errors)
        _get_non_negative_number_from_list(record, 1, "open_price", record_index, errors)
        _get_non_negative_number_from_list(record, 2, "high_price", record_index, errors)
        _get_non_negative_number_from_list(record, 3, "low_price", record_index, errors)
        _get_non_negative_number_from_list(record, 4, "close_price", record_index, errors)
        _get_non_negative_number_from_list(record, 5, "volume", record_index, errors)
        close_time = _get_non_negative_integer_from_list(record, 6, "close_time", record_index, errors)
        _get_non_negative_number_from_list(record, 7, "quote_asset_volume", record_index, errors)
        _get_non_negative_integer_from_list(record, 8, "number_of_trades", record_index, errors)
        _get_non_negative_number_from_list(record, 9, "taker_buy_base_asset_volume", record_index, errors)
        _get_non_negative_number_from_list(record, 10, "taker_buy_quote_asset_volume", record_index, errors)

        if open_time is not None and close_time is not None:
            if close_time <= open_time:
                errors.append(f"record {record_index} close_time must be greater than open_time")

        if open_time is not None:
            _validate_duplicate_open_time(open_time, seen_open_times, record_index, errors)
            record_date = _date_from_epoch_milliseconds(open_time)
            _validate_record_date_range(record_date, start_date, end_date, record_index, errors)

        record_index = record_index + 1


def _validate_required_top_level_field(payload, field_name, errors):
    if field_name not in payload:
        errors.append(f"missing top-level field {field_name}")
        return False

    return True


def _validate_required_record_field(record, field_name, record_index, errors):
    if field_name not in record:
        errors.append(f"record {record_index} missing field {field_name}")
        return False

    return True


def _get_non_negative_number_from_record(record, field_name, record_index, errors):
    has_field = _validate_required_record_field(record, field_name, record_index, errors)

    if not has_field:
        return None

    value = record[field_name]
    label = f"record {record_index} field {field_name}"
    number = _convert_to_non_negative_number(value, label, errors)

    return number


def _get_non_negative_number_from_list(record, value_index, field_name, record_index, errors):
    value = record[value_index]
    label = f"record {record_index} field {field_name}"
    number = _convert_to_non_negative_number(value, label, errors)

    return number


def _get_non_negative_integer_from_list(record, value_index, field_name, record_index, errors):
    value = record[value_index]
    label = f"record {record_index} field {field_name}"

    try:
        number = int(value)
    except TypeError:
        errors.append(f"{label} is not an integer")
        return None
    except ValueError:
        errors.append(f"{label} is not an integer")
        return None

    if number < 0:
        errors.append(f"{label} is negative")
        return None

    return number


def _parse_payload_date(value, field_name, errors):
    if value is None:
        return None

    try:
        parsed_date = date.fromisoformat(str(value))
    except ValueError:
        errors.append(f"top-level field {field_name} is not a valid date")
        return None

    return parsed_date


def _parse_record_date(value, record_index, field_name, errors):
    value_text = str(value)
    date_text = value_text[0:10]

    try:
        parsed_date = date.fromisoformat(date_text)
    except ValueError:
        errors.append(f"record {record_index} field {field_name} is not a valid date")
        return None

    return parsed_date


def _date_from_epoch_milliseconds(epoch_milliseconds):
    seconds = epoch_milliseconds / 1000
    date_time = datetime.fromtimestamp(seconds, tz=timezone.utc)
    record_date = date_time.date()

    return record_date


def _validate_duplicate_date(record_date, seen_dates, record_index, errors):
    if record_date in seen_dates:
        errors.append(f"record {record_index} has duplicate date {record_date.isoformat()}")
    else:
        seen_dates.add(record_date)


def _validate_duplicate_open_time(open_time, seen_open_times, record_index, errors):
    if open_time in seen_open_times:
        errors.append(f"record {record_index} has duplicate open_time {open_time}")
    else:
        seen_open_times.add(open_time)


def _validate_record_date_range(record_date, start_date, end_date, record_index, errors):
    if start_date is not None:
        if record_date < start_date:
            errors.append(f"record {record_index} date is before start_date")

    if end_date is not None:
        if record_date > end_date:
            errors.append(f"record {record_index} date is after end_date")


def _convert_to_non_negative_number(value, label, errors):
    try:
        number = float(value)
    except TypeError:
        errors.append(f"{label} is not numeric")
        return None
    except ValueError:
        errors.append(f"{label} is not numeric")
        return None

    if number < 0:
        errors.append(f"{label} is negative")
        return None

    return number


def quarantine_invalid_files(results):
    quarantined_paths = []

    for result in results:
        if result.is_valid():
            continue

        quarantined_path = quarantine_file(result)

        if quarantined_path is not None:
            quarantined_paths.append(quarantined_path)

    return quarantined_paths


def quarantine_file(result):
    source_path = result.path

    if not source_path.exists():
        return None

    try:
        relative_path = source_path.relative_to(BRONZE_DIR)
    except ValueError:
        relative_path = Path(source_path.name)

    rejected_path = REJECTED_DIR / relative_path
    rejected_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_path), str(rejected_path))

    return rejected_path


def build_validation_summary(results, failure_threshold=VALIDATION_FAILURE_THRESHOLD):
    valid_count = 0

    for result in results:
        if result.is_valid():
            valid_count = valid_count + 1

    total_files = len(results)
    invalid_count = total_files - valid_count

    if total_files == 0:
        failure_rate = 0
    else:
        failure_rate = invalid_count / total_files

    if failure_rate > failure_threshold:
        should_fail_run = True
    else:
        should_fail_run = False

    summary = ValidationSummary(
        total_files=total_files,
        valid_count=valid_count,
        invalid_count=invalid_count,
        failure_rate=failure_rate,
        failure_threshold=failure_threshold,
        should_fail_run=should_fail_run,
    )

    return summary


def print_validation_results(results):
    summary = build_validation_summary(results)
    total_count = summary.total_files
    valid_count = summary.valid_count
    invalid_count = summary.invalid_count

    print(f"Validated {total_count} expected Bronze files: {valid_count} valid, {invalid_count} invalid")

    for result in results:
        if result.is_valid():
            print(f"VALID   {result.symbol:<12} {result.path}")
        else:
            print(f"INVALID {result.symbol:<12} {result.path}")

            for error in result.errors:
                print(f"        - {error}")


def print_validation_summary(summary):
    failure_percentage = summary.failure_rate * 100
    threshold_percentage = summary.failure_threshold * 100

    print(f"Validation failure rate: {failure_percentage:.2f}%")
    print(f"Validation failure threshold: {threshold_percentage:.2f}%")

    if summary.should_fail_run:
        print("Validation decision: FAIL run")
    else:
        print("Validation decision: PASS run")


def _create_result(expected_file, file_size, errors):
    if errors:
        status = "invalid"
    else:
        status = "valid"

    result = FileValidationResult(
        asset_class=expected_file.asset_class,
        source=expected_file.source,
        symbol=expected_file.symbol,
        path=expected_file.path,
        status=status,
        file_size_bytes=file_size,
        errors=errors,
    )

    return result


def _safe_symbol(symbol):
    safe_symbol = symbol.replace("/", "_")
    return safe_symbol


def parse_args():
    parser = argparse.ArgumentParser(description="Run file-level validation for Bronze JSON files.")
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Bronze run date partition to validate, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--quarantine",
        action="store_true",
        help="Move invalid files from data/bronze to data/rejected after validation.",
    )
    parser.add_argument(
        "--failure-threshold",
        type=float,
        default=VALIDATION_FAILURE_THRESHOLD,
        help="Fail validation if the invalid file rate is greater than this number. Default is 0.25.",
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    run_date = date.fromisoformat(args.run_date)
    results = validate_bronze_file_level(run_date)
    print_validation_results(results)
    summary = build_validation_summary(results, args.failure_threshold)
    print_validation_summary(summary)

    if args.quarantine:
        quarantined_paths = quarantine_invalid_files(results)
        print(f"Quarantined {len(quarantined_paths)} invalid files")

    if summary.should_fail_run:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
