from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from common.tasks import convert_to_parquet, upload_to_gcs
from common import *


def define_ny_taxi_data_ingestion_dag(dag_id, dataset_name, dataset_base_file_name, default_args):
    dataset_base_file_url = dataset_base_url_trip_data + dataset_base_file_name

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        tags=['dtc-de'],
    )

    # NOTE: DAG declaration - using a Context Manager (an implicit way)
    with dag:
        local_file_path = path_to_local_home + '/' + dataset_base_file_name + date_string_month_template
        local_file_path_csv = local_file_path + ".csv"
        local_file_path_parquet = local_file_path + ".parquet"

        url = dataset_base_file_url + date_string_month_template + ".csv"
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url} > {local_file_path_csv}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=convert_to_parquet,
            op_kwargs={
                "src_file": local_file_path_csv,
            },
        )

        gcs_file_path = "raw/" + dataset_name + \
                        "/{{ dag_run.logical_date.year }}/{{ dag_run.logical_date.strftime('%m') }}.parquet"

        local_to_gcs_task = PythonOperator(
             task_id="local_to_gcs_task",
             python_callable=upload_to_gcs,
             op_kwargs={
                 "bucket": BUCKET,
                 "object_name": gcs_file_path,
                 "local_file": local_file_path_parquet,
             },
        )

        cleanup = BashOperator(
            task_id="cleanup",
            bash_command=f"rm {local_file_path_csv} {local_file_path_parquet}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> cleanup

        return dag