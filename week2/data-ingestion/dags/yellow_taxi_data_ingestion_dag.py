# This is an airflow DAG

import common
from common.dags import define_ny_taxi_data_ingestion_dag

dag_id = "yellow_taxi_data_ingestion"

# yellow_tripdata_yyyy-mm.csv
dataset_base_file_name = "yellow_tripdata_"
dataset_name = "yellow_taxi_trips"

dag = define_ny_taxi_data_ingestion_dag(
    dag_id,
    dataset_name,
    dataset_base_file_name,
    common.default_args_rides_ingestion)
