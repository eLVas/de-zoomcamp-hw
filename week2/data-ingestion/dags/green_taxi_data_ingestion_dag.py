# This is an airflow DAG

import common
from common.dags import define_ny_taxi_data_ingestion_dag

dag_id = "green_taxi_data_ingestion"

# green_tripdata_yyyy-mm.csv
dataset_base_file_name = "green_tripdata_"

default_args = {
    "owner": common.USER,
    "start_date": common.start_date,
    "depends_on_past": False,
    "retries": 1,
}

dag = define_ny_taxi_data_ingestion_dag(dag_id, "green_taxi_trips", dataset_base_file_name, default_args)
