from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
REJECTED_DIR = DATA_DIR / "rejected"

EQUITY_SYMBOLS = ["CBA.AX", "BHP.AX", "CSL.AX", "WOW.AX", "VAS.AX"]
CRYPTO_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
FX_SYMBOL = "AUDUSD=X"
DEFAULT_START_DATE = "2023-01-01"
VALIDATION_FAILURE_THRESHOLD = 0.25


def ensure_data_dirs():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
