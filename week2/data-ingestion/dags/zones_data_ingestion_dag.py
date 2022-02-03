
# This is an airflow DAG

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import common
from common.tasks import convert_to_parquet, upload_to_gcs

dataset_name = "zones"
base_file_name = "taxi+_zone_lookup"

with DAG(
        dag_id="zones_data_ingestion",
        default_args=common.default_args,
        schedule_interval=None,
        start_date=days_ago(0),
        catchup=False,
        tags=['dtc-de']
    ) as dag:
        local_file_path = common.path_to_local_home + '/' + base_file_name
        local_file_path_csv = local_file_path + ".csv"
        local_file_path_parquet = local_file_path + ".parquet"

        url = common.dataset_base_url_misc + base_file_name + ".csv"
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

        gcs_file_path = "raw/" + dataset_name + "/data.parquet"

        local_to_gcs_task = PythonOperator(
             task_id="local_to_gcs_task",
             python_callable=upload_to_gcs,
             op_kwargs={
                 "bucket": common.BUCKET,
                 "object_name": gcs_file_path,
                 "local_file": local_file_path_parquet,
             },
        )

        cleanup = BashOperator(
            task_id="cleanup",
            bash_command=f"rm {local_file_path_csv} {local_file_path_parquet}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> cleanup