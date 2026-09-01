CREATE SCHEMA IF NOT EXISTS car_analytics;

CREATE TABLE IF NOT EXISTS car_analytics.mart_avg_metrics_by_year (
    model_year integer PRIMARY KEY,
    cars_count bigint NOT NULL CHECK (cars_count > 0),
    avg_mpg_index numeric(12, 4) NOT NULL,
    avg_weight_index numeric(12, 4) NOT NULL,
    avg_acceleration_index numeric(12, 4) NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS car_analytics.mart_avg_metrics_by_origin (
    origin_name text PRIMARY KEY,
    cars_count bigint NOT NULL CHECK (cars_count > 0),
    avg_mpg_index numeric(12, 4) NOT NULL,
    avg_weight_index numeric(12, 4) NOT NULL,
    avg_hp_per_weight numeric(12, 6) NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS car_analytics.mart_avg_metrics_by_cylinders (
    cylinders integer PRIMARY KEY CHECK (cylinders > 0),
    cars_count bigint NOT NULL CHECK (cars_count > 0),
    avg_mpg numeric(12, 4) NOT NULL,
    avg_hp_per_weight numeric(12, 6) NOT NULL,
    avg_displacement_per_weight numeric(12, 6) NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS car_analytics.mart_top_power_to_weight (
    car_name text NOT NULL,
    model_year integer NOT NULL,
    horsepower_per_weight numeric(12, 6) NOT NULL,
    mpg_index numeric(12, 4) NOT NULL,
    weight_index numeric(12, 4) NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (car_name, model_year)
);
