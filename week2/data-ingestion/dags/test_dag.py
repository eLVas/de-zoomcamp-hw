# This is an airflow DAG

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def create_test_dag():
    start_date = datetime.fromisoformat("2019-01-01")

    default_args = {
        "owner": "airflow",
        "start_date": start_date,
        "depends_on_past": False,
        "retries": 1,
    }

    # NOTE: DAG declaration - using a Context Manager (an implicit way)
    with DAG(
            dag_id="test_dag",
            schedule_interval="@monthly",
            default_args=default_args,
            catchup=True,
            max_active_runs=3,
            tags=['dtc-de'],
    ) as dag:
        test_task = BashOperator(
            task_id="test_echo_ds",
            bash_command="echo green_tripdata_{{ dag_run.logical_date.strftime('%Y-%m') }}"
        )

        test_task

        return dag

dag = create_test_dag()
