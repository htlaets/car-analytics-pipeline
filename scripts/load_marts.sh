#!/usr/bin/env bash
set -euo pipefail

: "${PGHOST:=postgres}"
: "${PGPORT:=5432}"
: "${PGDATABASE:=cars}"
: "${PGUSER:=cars}"
: "${PGPASSWORD:=cars}"

export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD

for spec in \
  "mart_avg_metrics_by_year:model_year,cars_count,avg_mpg_index,avg_weight_index,avg_acceleration_index" \
  "mart_avg_metrics_by_origin:origin_name,cars_count,avg_mpg_index,avg_weight_index,avg_hp_per_weight" \
  "mart_avg_metrics_by_cylinders:cylinders,cars_count,avg_mpg,avg_hp_per_weight,avg_displacement_per_weight" \
  "mart_top_power_to_weight:car_name,model_year,horsepower_per_weight,mpg_index,weight_index"
do
  table="${spec%%:*}"
  columns="${spec#*:}"
  parquet_dir="/opt/airflow/project/build/lake/marts/${table}"
  csv_file="/tmp/${table}.csv"

  python - "$parquet_dir" "$csv_file" <<'PY'
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("export-mart").getOrCreate()
spark.read.parquet(sys.argv[1]).coalesce(1).write.mode("overwrite").option("header", True).csv(sys.argv[2] + ".dir")
import glob, shutil
shutil.copy(glob.glob(sys.argv[2] + ".dir/part-*.csv")[0], sys.argv[2])
spark.stop()
PY

  psql -v ON_ERROR_STOP=1 <<SQL
BEGIN;
TRUNCATE TABLE car_analytics.${table};
\copy car_analytics.${table} (${columns}) FROM '${csv_file}' WITH (FORMAT csv, HEADER true);
COMMIT;
SQL
done
