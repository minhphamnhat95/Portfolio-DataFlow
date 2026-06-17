from datetime import date

from src.transformation import yahoo_equities_to_silver
from src.transformation.schemas import asset_price_schema, yahoo_equity_bronze_schema
from src.transformation.spark_session import build_spark_session, stop_spark_session


def test_asset_price_schema_has_expected_columns():
    schema = asset_price_schema()
    actual_columns = []

    for field in schema.fields:
        actual_columns.append(field.name)

    expected_columns = [
        "symbol",
        "asset_class",
        "source",
        "price_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "currency",
        "ingestion_run_date",
        "bronze_file_path",
    ]

    assert actual_columns == expected_columns


def test_locate_yahoo_equity_bronze_paths_returns_json_files(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    partition_path = bronze_dir / "equities" / "source=yahoo" / "date=2026-06-16"
    partition_path.mkdir(parents=True)
    first_path = partition_path / "CBA.AX.json"
    second_path = partition_path / "BHP.AX.json"
    ignored_path = partition_path / "notes.txt"

    first_path.write_text("{}", encoding="utf-8")
    second_path.write_text("{}", encoding="utf-8")
    ignored_path.write_text("ignore me", encoding="utf-8")

    monkeypatch.setattr(yahoo_equities_to_silver, "BRONZE_DIR", bronze_dir)

    paths = yahoo_equities_to_silver.locate_yahoo_equity_bronze_paths(date(2026, 6, 16))

    expected_paths = [str(second_path), str(first_path)]
    expected_paths.sort()

    assert paths == expected_paths


def test_transform_yahoo_equity_bronze_to_asset_prices_writes_parquet(tmp_path):
    spark = build_spark_session("test-yahoo-equities-to-silver", "local[1]")

    try:
        data = [
            {
                "source": "yahoo",
                "symbol": "CBA.AX",
                "records": [
                    {
                        "Date": "2026-06-01T00:00:00+10:00",
                        "Open": 100.0,
                        "High": 105.0,
                        "Low": 99.0,
                        "Close": 102.0,
                        "Volume": 1000.0,
                    },
                    {
                        "Date": "2026-06-02T00:00:00+10:00",
                        "Open": 101.0,
                        "High": 106.0,
                        "Low": 100.0,
                        "Close": 103.0,
                        "Volume": -1.0,
                    },
                ],
            }
        ]

        bronze_df = spark.createDataFrame(data, yahoo_equity_bronze_schema())
        asset_prices_df = yahoo_equities_to_silver.transform_yahoo_equity_bronze_to_asset_prices(
            bronze_df,
            date(2026, 6, 16),
        )

        output_path = tmp_path / "silver" / "asset_prices"
        yahoo_equities_to_silver.write_asset_prices(asset_prices_df, output_path)
        read_df = spark.read.parquet(str(output_path))
        rows = read_df.collect()
        row = rows[0]

        assert len(rows) == 1
        assert row.symbol == "CBA.AX"
        assert row.asset_class == "equity"
        assert row.source == "yahoo"
        assert row.price_date == date(2026, 6, 1)
        assert row.open_price == 100.0
        assert row.high_price == 105.0
        assert row.low_price == 99.0
        assert row.close_price == 102.0
        assert row.volume == 1000.0
        assert row.currency == "AUD"
        assert row.ingestion_run_date == date(2026, 6, 16)
    finally:
        stop_spark_session(spark)
