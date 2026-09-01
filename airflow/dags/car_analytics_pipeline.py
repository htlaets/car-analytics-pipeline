from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="car_analytics_pipeline",
    description="CSV -> PySpark -> analytical marts -> PostgreSQL/Greenplum",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "pyspark", "cars"],
) as dag:
    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="cars_warehouse",
        sql="sql/01_schema.sql",
    )

    run_pyspark = BashOperator(
        task_id="run_pyspark",
        bash_command=(
            "python /opt/airflow/project/jobs/cars_etl.py "
            "--input /opt/airflow/project/data/sample/cars.csv "
            "--output /opt/airflow/project/build/lake"
        ),
    )

    load_marts = BashOperator(
        task_id="load_marts",
        bash_command="bash /opt/airflow/project/scripts/load_marts.sh",
    )

    validate_warehouse = SQLExecuteQueryOperator(
        task_id="validate_warehouse",
        conn_id="cars_warehouse",
        sql="sql/02_quality_checks.sql",
    )

    create_schema >> run_pyspark >> load_marts >> validate_warehouse
