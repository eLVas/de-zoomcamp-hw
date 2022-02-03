import os

from datetime import datetime

USER = "airflow"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

start_date = datetime.fromisoformat("2019-01-01")

dataset_base_url = "https://s3.amazonaws.com/nyc-tlc/"
dataset_base_url_trip_data = dataset_base_url + "trip+data/"
dataset_base_url_misc = dataset_base_url + "misc/"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

date_string_month_template = "{{ dag_run.logical_date.strftime('%Y-%m') }}"

default_args = {
    "owner": USER,
    "depends_on_past": False,
    "retries": 1,
}

default_args_rides_ingestion = {
    **default_args,
    "start_date": start_date,
    "schedule_interval": "@monthly",
    "catchup": True,
}
