import argparse
import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.transformation.spark_session import build_spark_session, stop_spark_session


def slow_partition(values, task_seconds):
    count = 0

    time.sleep(task_seconds)

    for value in values:
        count = count + 1

    yield count


def run_demo(master, port, partitions, task_seconds, keep_alive_seconds):
    spark = build_spark_session(
        app_name="finance-market-data-spark-ui-demo",
        master=master,
        enable_ui=True,
        ui_port=port,
    )

    print(f"Spark version: {spark.version}", flush=True)
    print(f"Spark UI: http://localhost:{port}", flush=True)
    print(f"Starting slow Spark job for about {task_seconds} seconds", flush=True)

    try:
        values = []
        value = 0

        while value < partitions:
            values.append(value)
            value = value + 1

        rdd = spark.sparkContext.parallelize(values, partitions)
        result = rdd.mapPartitions(lambda partition_values: slow_partition(partition_values, task_seconds)).collect()

        print(f"Slow Spark job finished with partition counts: {result}", flush=True)
        print(f"Keeping Spark UI alive for {keep_alive_seconds} more seconds", flush=True)
        time.sleep(keep_alive_seconds)
    finally:
        stop_spark_session(spark)
        print("Spark UI demo stopped", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Run a long Spark job so the Spark UI can be inspected.")
    parser.add_argument("--master", default="local[2]", help="Spark master value.")
    parser.add_argument("--port", type=int, default=4040, help="Spark UI port.")
    parser.add_argument("--partitions", type=int, default=2, help="Number of partitions for the slow job.")
    parser.add_argument("--task-seconds", type=int, default=900, help="Seconds each partition should sleep.")
    parser.add_argument("--keep-alive-seconds", type=int, default=300, help="Seconds to keep UI alive after the job.")

    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    run_demo(
        master=args.master,
        port=args.port,
        partitions=args.partitions,
        task_seconds=args.task_seconds,
        keep_alive_seconds=args.keep_alive_seconds,
    )


if __name__ == "__main__":
    main()
