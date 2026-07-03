import os
import sys

from pyspark.sql import SparkSession

from src.utils.config import SPARK_LOCAL_DIR, SPARK_WAREHOUSE_DIR


DEFAULT_SPARK_APP_NAME = "finance-market-data"
DEFAULT_SPARK_MASTER = "local[*]"


def build_spark_session(
    app_name=DEFAULT_SPARK_APP_NAME,
    master=DEFAULT_SPARK_MASTER,
    enable_ui=True,
    ui_port=4040,
):
    builder = SparkSession.builder
    builder = builder.appName(app_name)
    builder = builder.master(master)
    builder = apply_default_spark_config(builder, enable_ui, ui_port)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def apply_default_spark_config(builder, enable_ui=False, ui_port=4040):
    SPARK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    SPARK_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.shuffle.partitions", "4")
    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
    builder = builder.config("spark.local.dir", str(SPARK_LOCAL_DIR))
    builder = builder.config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR.as_uri())
    builder = builder.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    builder = builder.config("spark.pyspark.python", python_executable)
    builder = builder.config("spark.pyspark.driver.python", python_executable)

    if enable_ui:
        builder = builder.config("spark.ui.enabled", "true")
        builder = builder.config("spark.ui.port", str(ui_port))
    else:
        builder = builder.config("spark.ui.enabled", "false")

    return builder


def stop_spark_session(spark):
    if spark is None:
        return

    spark.stop()
