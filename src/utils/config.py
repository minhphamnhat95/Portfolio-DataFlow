from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
REJECTED_DIR = DATA_DIR / "rejected"
SPARK_LOCAL_DIR = DATA_DIR / "tmp" / "spark"
SPARK_WAREHOUSE_DIR = DATA_DIR / "tmp" / "spark-warehouse"

EQUITY_SYMBOLS = ["CBA.AX", "BHP.AX", "CSL.AX", "WOW.AX", "VAS.AX"]
CRYPTO_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
FX_SYMBOL = "AUDUSD=X"
DEFAULT_START_DATE = "2023-01-01"
VALIDATION_FAILURE_THRESHOLD = 0.25
TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE = 0.0435
FIXED_PORTFOLIO_WEIGHTS = [
    {"symbol": "VAS.AX", "weight": 0.40},
    {"symbol": "CBA.AX", "weight": 0.25},
    {"symbol": "BHP.AX", "weight": 0.15},
    {"symbol": "BTCUSDT", "weight": 0.10},
    {"symbol": "ETHUSDT", "weight": 0.10},
]


def ensure_data_dirs():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    SPARK_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
