import os

from datetime import datetime

USER = "airflow"

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

start_date = datetime.fromisoformat("2019-01-01")

dataset_base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

date_string_month_template = "{{ dag_run.logical_date.strftime('%Y-%m') }}"
