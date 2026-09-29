from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = (
    PROJECT_ROOT / "data" / "source" / "kudago_spb_summer_2026.json"
)

common = {
    "cwd": str(PROJECT_ROOT),
    "append_env": True,
    "env": {
        "INPUT_FILE": (
            "{{ dag_run.conf.get('input_file', params.input_file) "
            "if dag_run else params.input_file }}"
        )
    },
}

with DAG(
    dag_id="kudago_spb_elt",
    description="KudaGo events in Saint Petersburg: fixed summer 2026 slice",
    start_date=datetime(2026, 6, 1, tzinfo=pendulum.timezone("Europe/Moscow")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"input_file": str(DEFAULT_INPUT)},
    tags=["homework", "spb", "kudago"],
) as dag:
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command='make load INPUT_FILE="$INPUT_FILE"',
        **common,
    )

    transform_candidate = BashOperator(
        task_id="transform_candidate",
        bash_command="make transform",
        **common,
    )

    quality_tests = BashOperator(
        task_id="quality_tests",
        bash_command="make quality",
        **common,
    )

    publish_mart = BashOperator(
        task_id="publish_mart",
        bash_command="make publish",
        **common,
    )

    load_raw >> transform_candidate >> quality_tests >> publish_mart
