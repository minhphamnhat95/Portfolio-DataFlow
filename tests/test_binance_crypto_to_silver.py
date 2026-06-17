from datetime import date

from src.transformation import binance_crypto_to_silver
from src.transformation.schemas import binance_crypto_bronze_schema
from src.transformation.spark_session import build_spark_session, stop_spark_session


def test_locate_binance_crypto_bronze_paths_returns_json_files(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    partition_path = bronze_dir / "crypto" / "source=binance" / "date=2026-06-16"
    partition_path.mkdir(parents=True)
    first_path = partition_path / "BTCUSDT.json"
    second_path = partition_path / "ETHUSDT.json"
    ignored_path = partition_path / "notes.txt"

    first_path.write_text("{}", encoding="utf-8")
    second_path.write_text("{}", encoding="utf-8")
    ignored_path.write_text("ignore me", encoding="utf-8")

    monkeypatch.setattr(binance_crypto_to_silver, "BRONZE_DIR", bronze_dir)

    paths = binance_crypto_to_silver.locate_binance_crypto_bronze_paths(date(2026, 6, 16))

    expected_paths = [str(second_path), str(first_path)]
    expected_paths.sort()

    assert paths == expected_paths


def test_transform_binance_crypto_bronze_to_asset_prices_writes_parquet(tmp_path):
    spark = build_spark_session("test-binance-crypto-to-silver", "local[1]")

    try:
        data = [
            {
                "source": "binance",
                "symbol": "BTCUSDT",
                "records": [
                    [
                        "1780272000000",
                        "73674.39",
                        "74092.00",
                        "70686.68",
                        "71408.90",
                        "23921.09",
                        "1780358399999",
                        "1723958338.68",
                        "4237773",
                        "11600.30",
                        "835109172.11",
                        "0",
                    ],
                    [
                        "1780358400000",
                        "71408.90",
                        "72000.00",
                        "70000.00",
                        "71000.00",
                        "-1",
                        "1780444799999",
                        "1000",
                        "100",
                        "10",
                        "100",
                        "0",
                    ],
                ],
            }
        ]

        bronze_df = spark.createDataFrame(data, binance_crypto_bronze_schema())
        asset_prices_df = binance_crypto_to_silver.transform_binance_crypto_bronze_to_asset_prices(
            bronze_df,
            date(2026, 6, 16),
        )

        output_path = tmp_path / "silver" / "asset_prices"
        binance_crypto_to_silver.write_asset_prices(asset_prices_df, output_path)
        read_df = spark.read.parquet(str(output_path))
        rows = read_df.collect()
        row = rows[0]

        assert len(rows) == 1
        assert row.symbol == "BTCUSDT"
        assert row.asset_class == "crypto"
        assert row.source == "binance"
        assert row.price_date == date(2026, 6, 1)
        assert row.open_price == 73674.39
        assert row.high_price == 74092.0
        assert row.low_price == 70686.68
        assert row.close_price == 71408.9
        assert row.volume == 23921.09
        assert row.currency == "USDT"
        assert row.ingestion_run_date == date(2026, 6, 16)
    finally:
        stop_spark_session(spark)
