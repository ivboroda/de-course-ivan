"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile.

Контейнер Airflow має Java, pyspark, JDBC-драйвер і dbt (в окремому venv, див.
docker/Dockerfile.airflow). Проєкт змонтовано в /opt/airflow/project.

DAG навмисно НЕ використовує data_interval_start/end: стан «що вже оброблено» живе в даних
(`_source_file` у Bronze, `_ingested_at` у dbt-моделях), а не в розкладі.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"  # dbt у окремому venv
# target/ і logs/ пишемо в /tmp контейнера, щоб не смітити у змонтованому проєкті.
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="rides_medallion",
    description="Bronze (Spark) -> Silver -> Gold -> reconcile (dbt)",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["medallion", "final-project"],
) as dag:
    bronze_spark = BashOperator(
        task_id="bronze_spark",
        bash_command=f"cd {PROJECT} && python bronze_job.py",
    )
    bronze_contract = BashOperator(
        task_id="bronze_contract",
        bash_command=f"{DBT} test --select source:bronze --indirect-selection cautious {DBT_DIRS}",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{DBT} build --selector silver --indirect-selection cautious {DBT_DIRS}",
    )
    gold = BashOperator(
        task_id="gold",
        bash_command=f"{DBT} build --selector gold --indirect-selection cautious {DBT_DIRS}",
    )
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{DBT} test --selector reconcile {DBT_DIRS}",
    )

    bronze_spark >> bronze_contract >> silver >> gold >> reconcile
