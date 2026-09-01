from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {
    "car_name", "model", "origin", "mpg", "cylinders",
    "displacement", "horsepower", "weight", "acceleration",
}


class DataQualityError(ValueError):
    """Raised when source or transformed data violates a quality rule."""


def validate_source_schema(df: DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise DataQualityError(f"Missing required columns: {sorted(missing)}")


def validate_clean_data(df: DataFrame) -> None:
    if df.limit(1).count() == 0:
        raise DataQualityError("No valid rows remain after cleaning")

    invalid = df.filter(
        F.col("car_name").isNull()
        | ~F.col("origin").isin(1, 2, 3)
        | (F.col("mpg") <= 0)
        | (F.col("cylinders") <= 0)
        | (F.col("displacement") <= 0)
        | (F.col("horsepower") <= 0)
        | (F.col("weight") <= 0)
        | (F.col("acceleration") <= 0)
    )
    if invalid.limit(1).count() > 0:
        raise DataQualityError("Clean layer contains rows outside business constraints")


def validate_unique_key(df: DataFrame, key: str) -> None:
    duplicates = df.groupBy(key).count().filter(F.col("count") > 1)
    if duplicates.limit(1).count() > 0:
        raise DataQualityError(f"Duplicate values found in key: {key}")
