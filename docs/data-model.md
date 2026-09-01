# Модель данных

```mermaid
erDiagram
    DIM_ORIGIN ||--o{ DIM_CAR : classifies
    DIM_CAR ||--|| FACT_CAR_METRICS : describes
    DIM_ENGINE ||--o{ FACT_CAR_METRICS : powers

    DIM_ORIGIN {
        int origin_id PK
        string origin_name
    }
    DIM_CAR {
        bigint car_id PK
        string car_name
        int model_year
        int origin_id FK
    }
    DIM_ENGINE {
        bigint engine_id PK
        int cylinders
        double displacement
        double horsepower
    }
    FACT_CAR_METRICS {
        bigint car_id FK
        bigint engine_id FK
        double mpg
        double weight
        double acceleration
        double horsepower_per_weight
        double displacement_per_weight
        double horsepower_per_displacement
        double mpg_index
        double weight_index
        double acceleration_index
    }
```

## Ключи

`car_id` и `engine_id` вычисляются функцией `xxhash64` из натуральных атрибутов. В отличие от `monotonically_increasing_id`, результат не зависит от числа Spark-партиций и остаётся стабильным при повторной обработке одинакового набора данных.

## Стратегия загрузки

Учебный датасет является полным снимком, поэтому Parquet-слои атомарно перезаписываются. Витрины загружаются в транзакции `TRUNCATE + COPY`: потребитель видит либо предыдущую, либо новую полную версию данных.

Для промышленного варианта следующая итерация — хранение `source_updated_at`, watermark в Airflow и `MERGE` по детерминированному ключу.
