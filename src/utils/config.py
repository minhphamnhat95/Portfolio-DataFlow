from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
REJECTED_DIR = DATA_DIR / "rejected"
SPARK_LOCAL_DIR = DATA_DIR / "tmp" / "spark"
SPARK_WAREHOUSE_DIR = DATA_DIR / "tmp" / "spark-warehouse"


EQUITY_SYMBOLS = [
    # Major Australian banks
    "CBA.AX",
    "WBC.AX",
    "NAB.AX",
    "ANZ.AX",
    "MQG.AX",

    # Mining and resources
    "BHP.AX",
    "RIO.AX",
    "FMG.AX",
    "MIN.AX",
    "S32.AX",
    "WDS.AX",
    "STO.AX",

    # Healthcare
    "CSL.AX",
    "RMD.AX",
    "COH.AX",
    "SHL.AX",

    # Consumer and retail
    "WOW.AX",
    "COL.AX",
    "WES.AX",
    "JBH.AX",

    # Technology and telecommunications
    "XRO.AX",
    "REA.AX",
    "CAR.AX",
    "TLS.AX",

    # Infrastructure and industrials
    "TCL.AX",
    "QAN.AX",
    "ALL.AX",

    # Australian ETFs
    "VAS.AX",
    "A200.AX",
    "VGS.AX",
]

CRYPTO_SYMBOLS = [
    # Large-cap cryptocurrencies
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "TRXUSDT",
    "AVAXUSDT",
    "LINKUSDT",

    # Layer 1 and Layer 2
    "DOTUSDT",
    "NEARUSDT",
    "ATOMUSDT",
    "SUIUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "POLUSDT",

    # DeFi
    "UNIUSDT",
    "AAVEUSDT",
    "MKRUSDT",
    "LDOUSDT",
    "INJUSDT",

    # Infrastructure and data
    "FILUSDT",
    "ICPUSDT",
    "GRTUSDT",

    # Other widely traded assets
    "LTCUSDT",
    "BCHUSDT",
    "ETCUSDT",
    "XLMUSDT",
]

FX_SYMBOL = "AUDUSD=X"
DEFAULT_START_DATE = "2020-01-01"
VALIDATION_FAILURE_THRESHOLD = 0.25
TRADING_DAYS_PER_YEAR = 252
CALENDAR_DAYS_PER_YEAR = 365
MIN_ANNUALIZATION_OBSERVATIONS = 30
RISK_FREE_RATE = 0.0435
FIXED_PORTFOLIO_NAME = "fixed_demo"
OPTIMIZED_PORTFOLIO_NAME = "max_sharpe_calendar"
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


DEFAULT_POSTGRES_HOST = "127.0.0.1"
DEFAULT_POSTGRES_PORT = "5432"
DEFAULT_POSTGRES_DATABASE = "finance_data_market"
DEFAULT_POSTGRES_USER = "postgres"
DEFAULT_POSTGRES_PASSWORD = ""
