from src.transformation import spark_session


class FakeSparkContext:
    def __init__(self):
        self.log_level = None

    def setLogLevel(self, log_level):
        self.log_level = log_level


class FakeSpark:
    def __init__(self):
        self.sparkContext = FakeSparkContext()
        self.was_stopped = False

    def stop(self):
        self.was_stopped = True


class FakeBuilder:
    def __init__(self):
        self.app_name = None
        self.master_value = None
        self.config_values = {}
        self.spark = FakeSpark()

    def appName(self, app_name):
        self.app_name = app_name
        return self

    def master(self, master_value):
        self.master_value = master_value
        return self

    def config(self, key, value):
        self.config_values[key] = value
        return self

    def getOrCreate(self):
        return self.spark


class FakeSparkSession:
    builder = FakeBuilder()


def test_build_spark_session_applies_local_defaults(monkeypatch):
    fake_spark_session = FakeSparkSession()
    fake_spark_session.builder = FakeBuilder()
    spark_local_dir = spark_session.SPARK_LOCAL_DIR
    spark_warehouse_dir = spark_session.SPARK_WAREHOUSE_DIR

    monkeypatch.setattr(spark_session, "SparkSession", fake_spark_session)

    spark = spark_session.build_spark_session("test-app", "local[1]")
    builder = fake_spark_session.builder

    assert builder.app_name == "test-app"
    assert builder.master_value == "local[1]"
    assert builder.config_values["spark.sql.session.timeZone"] == "UTC"
    assert builder.config_values["spark.sql.shuffle.partitions"] == "4"
    assert builder.config_values["spark.driver.bindAddress"] == "127.0.0.1"
    assert builder.config_values["spark.ui.enabled"] == "false"
    assert builder.config_values["spark.local.dir"] == str(spark_local_dir)
    assert builder.config_values["spark.sql.warehouse.dir"] == spark_warehouse_dir.as_uri()
    assert "spark.pyspark.python" in builder.config_values
    assert "spark.pyspark.driver.python" in builder.config_values
    assert spark.sparkContext.log_level == "WARN"


def test_build_spark_session_can_enable_spark_ui(monkeypatch):
    fake_spark_session = FakeSparkSession()
    fake_spark_session.builder = FakeBuilder()

    monkeypatch.setattr(spark_session, "SparkSession", fake_spark_session)

    spark_session.build_spark_session("test-app", "local[1]", True, 4050)
    builder = fake_spark_session.builder

    assert builder.config_values["spark.ui.enabled"] == "true"
    assert builder.config_values["spark.ui.port"] == "4050"


def test_stop_spark_session_stops_existing_session():
    spark = FakeSpark()

    spark_session.stop_spark_session(spark)

    assert spark.was_stopped


def test_stop_spark_session_accepts_none():
    spark_session.stop_spark_session(None)
