#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pyspark.sql import SparkSession  # noqa: E402

from car_pipeline.transformations import (  # noqa: E402
    build_marts,
    build_model,
    build_staging,
    read_source,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build car analytics warehouse and marts")
    parser.add_argument("--input", required=True, help="CSV path supported by Spark")
    parser.add_argument("--output", required=True, help="Output lake path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("car-analytics-pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = read_source(spark, args.input)
        staging = build_staging(raw)
        model = build_model(spark, staging)
        marts = build_marts(model)

        staging.write.mode("overwrite").parquet(f"{args.output}/staging/cars")
        for name, frame in model.items():
            frame.write.mode("overwrite").parquet(f"{args.output}/core/{name}")
        for name, frame in marts.items():
            frame.write.mode("overwrite").option("header", True).parquet(f"{args.output}/marts/{name}")
        print(f"Pipeline completed: {staging.count()} valid rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
