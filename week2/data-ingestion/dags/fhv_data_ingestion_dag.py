# This is an airflow DAG

import common
from common.dags import define_ny_taxi_data_ingestion_dag

dag_id = "fhv_data_ingestion"

# fhv_tripdata_yyyy-mm.csv
dataset_base_file_name = "fhv_tripdata_"
dataset_name = "fhv_trips"

dag = define_ny_taxi_data_ingestion_dag(
    dag_id,
    dataset_name,
    dataset_base_file_name,
    common.default_args_rides_ingestion)
