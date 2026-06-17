from src.transformation import assets_to_silver
from src.transformation.schemas import asset_schema
from src.transformation.spark_session import build_spark_session, stop_spark_session


def test_asset_schema_has_expected_columns():
    schema = asset_schema()
    actual_columns = []

    for field in schema.fields:
        actual_columns.append(field.name)

    expected_columns = [
        "symbol",
        "asset_class",
        "source",
        "currency",
        "description",
        "is_active",
    ]

    assert actual_columns == expected_columns


def test_build_asset_rows_uses_configured_symbols(monkeypatch):
    monkeypatch.setattr(assets_to_silver, "EQUITY_SYMBOLS", ["CBA.AX"])
    monkeypatch.setattr(assets_to_silver, "CRYPTO_SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(assets_to_silver, "FX_SYMBOL", "AUDUSD=X")

    rows = assets_to_silver.build_asset_rows()

    assert len(rows) == 3
    assert rows[0]["symbol"] == "CBA.AX"
    assert rows[0]["asset_class"] == "equity"
    assert rows[0]["source"] == "yahoo"
    assert rows[0]["currency"] == "AUD"
    assert rows[0]["is_active"]
    assert rows[1]["symbol"] == "BTCUSDT"
    assert rows[1]["asset_class"] == "crypto"
    assert rows[1]["source"] == "binance"
    assert rows[1]["currency"] == "USDT"
    assert rows[2]["symbol"] == "AUDUSD=X"
    assert rows[2]["asset_class"] == "fx"
    assert rows[2]["source"] == "yahoo"
    assert rows[2]["currency"] == "USD"


def test_transform_assets_to_silver_writes_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(assets_to_silver, "EQUITY_SYMBOLS", ["CBA.AX"])
    monkeypatch.setattr(assets_to_silver, "CRYPTO_SYMBOLS", ["BTCUSDT"])
    monkeypatch.setattr(assets_to_silver, "FX_SYMBOL", "AUDUSD=X")

    spark = build_spark_session("test-assets-to-silver", "local[1]")

    try:
        output_path = tmp_path / "silver" / "assets"
        result = assets_to_silver.transform_assets_to_silver(spark, output_path)
        read_df = spark.read.parquet(str(output_path))
        rows = read_df.orderBy("symbol").collect()

        assert result["table"] == "silver.assets"
        assert result["row_count"] == 3
        assert len(rows) == 3
        assert rows[0].symbol == "AUDUSD=X"
        assert rows[0].asset_class == "fx"
        assert rows[1].symbol == "BTCUSDT"
        assert rows[1].asset_class == "crypto"
        assert rows[2].symbol == "CBA.AX"
        assert rows[2].asset_class == "equity"
    finally:
        stop_spark_session(spark)
