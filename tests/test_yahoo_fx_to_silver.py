from datetime import date

from src.transformation import yahoo_fx_to_silver
from src.transformation.schemas import fx_rate_schema, yahoo_fx_bronze_schema
from src.transformation.spark_session import build_spark_session, stop_spark_session


def test_fx_rate_schema_has_expected_columns():
    schema = fx_rate_schema()
    actual_columns = []

    for field in schema.fields:
        actual_columns.append(field.name)

    expected_columns = [
        "symbol",
        "source",
        "rate_date",
        "base_currency",
        "quote_currency",
        "rate",
        "ingestion_run_date",
        "bronze_file_path",
    ]

    assert actual_columns == expected_columns


def test_locate_yahoo_fx_bronze_paths_returns_json_files(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    partition_path = bronze_dir / "fx" / "source=yahoo" / "date=2026-06-16"
    partition_path.mkdir(parents=True)
    first_path = partition_path / "AUDUSD=X.json"
    ignored_path = partition_path / "notes.txt"

    first_path.write_text("{}", encoding="utf-8")
    ignored_path.write_text("ignore me", encoding="utf-8")

    monkeypatch.setattr(yahoo_fx_to_silver, "BRONZE_DIR", bronze_dir)

    paths = yahoo_fx_to_silver.locate_yahoo_fx_bronze_paths(date(2026, 6, 16))

    assert paths == [str(first_path)]


def test_transform_yahoo_fx_bronze_to_fx_rates_writes_parquet(tmp_path):
    spark = build_spark_session("test-yahoo-fx-to-silver", "local[1]")

    try:
        data = [
            {
                "source": "yahoo",
                "symbol": "AUDUSD=X",
                "records": [
                    {
                        "Date": "2026-06-01T00:00:00+00:00",
                        "Open": 0.65,
                        "High": 0.66,
                        "Low": 0.64,
                        "Close": 0.655,
                        "Volume": 0.0,
                    },
                    {
                        "Date": "2026-06-02T00:00:00+00:00",
                        "Open": 0.65,
                        "High": 0.66,
                        "Low": 0.64,
                        "Close": -0.1,
                        "Volume": 0.0,
                    },
                ],
            }
        ]

        bronze_df = spark.createDataFrame(data, yahoo_fx_bronze_schema())
        fx_rates_df = yahoo_fx_to_silver.transform_yahoo_fx_bronze_to_fx_rates(
            bronze_df,
            date(2026, 6, 16),
        )

        output_path = tmp_path / "silver" / "fx_rates"
        yahoo_fx_to_silver.write_fx_rates(fx_rates_df, output_path)
        read_df = spark.read.parquet(str(output_path))
        rows = read_df.collect()
        row = rows[0]

        assert len(rows) == 1
        assert row.symbol == "AUDUSD=X"
        assert row.source == "yahoo"
        assert row.rate_date == date(2026, 6, 1)
        assert row.base_currency == "AUD"
        assert row.quote_currency == "USD"
        assert row.rate == 0.655
        assert row.ingestion_run_date == date(2026, 6, 16)
    finally:
        stop_spark_session(spark)
