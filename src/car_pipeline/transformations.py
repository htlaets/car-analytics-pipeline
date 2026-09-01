from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from car_pipeline.quality import validate_clean_data, validate_source_schema, validate_unique_key

SOURCE_SCHEMA = StructType([
    StructField("car_name", StringType(), True),
    StructField("model", IntegerType(), True),
    StructField("origin", IntegerType(), True),
    StructField("mpg", DoubleType(), True),
    StructField("cylinders", IntegerType(), True),
    StructField("displacement", DoubleType(), True),
    StructField("horsepower", DoubleType(), True),
    StructField("weight", DoubleType(), True),
    StructField("acceleration", DoubleType(), True),
])


def read_source(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.option("header", True).schema(SOURCE_SCHEMA).csv(path)
    validate_source_schema(df)
    return df


def build_staging(raw: DataFrame) -> DataFrame:
    staged = raw.select(
        F.trim("car_name").alias("car_name"),
        F.col("model").cast("int"),
        F.col("origin").cast("int"),
        *[F.col(c).cast("double") for c in [
            "mpg", "displacement", "horsepower", "weight", "acceleration"
        ]],
        F.col("cylinders").cast("int"),
    ).withColumn(
        "model_year", F.when(F.col("model") < 100, F.col("model") + 1900).otherwise(F.col("model"))
    )

    clean = staged.dropna(subset=list(staged.columns)).filter(
        (F.col("car_name") != "")
        & F.col("origin").isin(1, 2, 3)
        & (F.col("mpg") > 0)
        & (F.col("cylinders") > 0)
        & (F.col("displacement") > 0)
        & (F.col("horsepower") > 0)
        & (F.col("weight") > 0)
        & (F.col("acceleration") > 0)
    )
    validate_clean_data(clean)
    return clean


def build_model(spark: SparkSession, clean: DataFrame) -> Mapping[str, DataFrame]:
    origin = spark.createDataFrame([(1, "USA"), (2, "Europe"), (3, "Japan")], ["origin_id", "origin_name"])

    cars = clean.withColumn(
        "car_id", F.xxhash64("car_name", "model_year", "origin", "cylinders", "horsepower")
    )
    engine = cars.select("cylinders", "displacement", "horsepower").dropDuplicates().withColumn(
        "engine_id", F.xxhash64("cylinders", "displacement", "horsepower")
    )
    dim_car = cars.select("car_id", "car_name", "model_year", F.col("origin").alias("origin_id"))
    dim_engine = engine.select("engine_id", "cylinders", "displacement", "horsepower")

    averages = cars.agg(*[
        F.avg(c).alias(c) for c in ["mpg", "weight", "acceleration"]
    ]).first().asDict()

    fact = cars.join(engine, ["cylinders", "displacement", "horsepower"], "left").select(
        "car_id", "engine_id", "mpg", "weight", "acceleration",
        F.round(F.col("horsepower") / F.col("weight"), 6).alias("horsepower_per_weight"),
        F.round(F.col("displacement") / F.col("weight"), 6).alias("displacement_per_weight"),
        F.round(F.col("horsepower") / F.col("displacement"), 6).alias("horsepower_per_displacement"),
        F.round(F.col("mpg") / F.lit(averages["mpg"]), 6).alias("mpg_index"),
        F.round(F.col("weight") / F.lit(averages["weight"]), 6).alias("weight_index"),
        F.round(F.col("acceleration") / F.lit(averages["acceleration"]), 6).alias("acceleration_index"),
    )

    validate_unique_key(dim_car, "car_id")
    validate_unique_key(dim_engine, "engine_id")
    return {"dim_origin": origin, "dim_car": dim_car, "dim_engine": dim_engine, "fact_car_metrics": fact}


def build_marts(model: Mapping[str, DataFrame]) -> Mapping[str, DataFrame]:
    car, origin, engine, fact = (
        model["dim_car"], model["dim_origin"], model["dim_engine"], model["fact_car_metrics"]
    )
    by_year = fact.join(car, "car_id").groupBy("model_year").agg(
        F.count("*").alias("cars_count"),
        F.round(F.avg("mpg_index"), 4).alias("avg_mpg_index"),
        F.round(F.avg("weight_index"), 4).alias("avg_weight_index"),
        F.round(F.avg("acceleration_index"), 4).alias("avg_acceleration_index"),
    )
    by_origin = fact.join(car, "car_id").join(origin, "origin_id").groupBy("origin_name").agg(
        F.count("*").alias("cars_count"),
        F.round(F.avg("mpg_index"), 4).alias("avg_mpg_index"),
        F.round(F.avg("weight_index"), 4).alias("avg_weight_index"),
        F.round(F.avg("horsepower_per_weight"), 6).alias("avg_hp_per_weight"),
    )
    by_cylinders = fact.join(engine, "engine_id").groupBy("cylinders").agg(
        F.count("*").alias("cars_count"),
        F.round(F.avg("mpg"), 4).alias("avg_mpg"),
        F.round(F.avg("horsepower_per_weight"), 6).alias("avg_hp_per_weight"),
        F.round(F.avg("displacement_per_weight"), 6).alias("avg_displacement_per_weight"),
    )
    top_power = fact.join(car, "car_id").select(
        "car_name", "model_year", "horsepower_per_weight", "mpg_index", "weight_index"
    ).orderBy(F.desc("horsepower_per_weight")).limit(10)
    return {
        "mart_avg_metrics_by_year": by_year,
        "mart_avg_metrics_by_origin": by_origin,
        "mart_avg_metrics_by_cylinders": by_cylinders,
        "mart_top_power_to_weight": top_power,
    }
