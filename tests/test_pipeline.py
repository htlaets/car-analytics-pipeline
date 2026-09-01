import pytest

from car_pipeline.quality import DataQualityError, validate_source_schema
from car_pipeline.transformations import build_marts, build_model, build_staging


def test_missing_source_column_fails(spark):
    df = spark.createDataFrame([("car",)], ["car_name"])
    with pytest.raises(DataQualityError, match="Missing required columns"):
        validate_source_schema(df)


def test_pipeline_builds_deterministic_model_and_marts(spark):
    raw = spark.createDataFrame([
        ("car a", 70, 1, 18.0, 8, 307.0, 130.0, 3504.0, 12.0),
        ("car b", 71, 3, 30.0, 4, 97.0, 88.0, 2100.0, 15.0),
    ], [
        "car_name", "model", "origin", "mpg", "cylinders",
        "displacement", "horsepower", "weight", "acceleration",
    ])

    clean = build_staging(raw)
    model = build_model(spark, clean)
    marts = build_marts(model)

    assert clean.count() == 2
    assert model["dim_car"].select("car_id").distinct().count() == 2
    assert model["fact_car_metrics"].count() == 2
    assert marts["mart_avg_metrics_by_origin"].count() == 2
    assert marts["mart_top_power_to_weight"].first()["car_name"] == "car b"


def test_invalid_business_values_are_removed(spark):
    raw = spark.createDataFrame([
        ("valid", 70, 1, 18.0, 8, 307.0, 130.0, 3504.0, 12.0),
        ("invalid", 70, 9, -1.0, 8, 307.0, 130.0, 3504.0, 12.0),
    ], [
        "car_name", "model", "origin", "mpg", "cylinders",
        "displacement", "horsepower", "weight", "acceleration",
    ])
    assert build_staging(raw).count() == 1
