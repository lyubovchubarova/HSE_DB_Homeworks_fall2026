from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMMON = {
    "cwd": str(PROJECT_ROOT),
    "append_env": True,
    "env": {"SNAPSHOT_DATE": "{{ ds }}"},
}

with DAG(
    dag_id="trudvsem_vacancy_elt",
    description="Current Работа России vacancies for eight regions",
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.timezone("Europe/Moscow")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["homework", "vacancies"],
) as dag:
    download = BashOperator(
        task_id="download",
        bash_command='make download SNAPSHOT_DATE="$SNAPSHOT_DATE"',
        **COMMON,
    )
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command='make load SNAPSHOT_DATE="$SNAPSHOT_DATE"',
        **COMMON,
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="make dbt-build",
        **COMMON,
    )
    publish = BashOperator(
        task_id="publish",
        bash_command="make publish",
        **COMMON,
    )

    download >> load_raw >> dbt_build >> publish

