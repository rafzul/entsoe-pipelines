from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from scripts.extract_raw import extract_raw_data

default_args = {
    "owner": "rafzul",
    "start_date": datetime(2021, 1, 1, 0, 00),
    "end_date": datetime(2021, 1, 1, 1, 00),
    "depends on past": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

tz = "Europe/Berlin"
country_code = "DE_TENNET"


dag = DAG(
    "ingestion-energydata",
    default_args=default_args,
    description="Builds a DAG to ingest energy data from ENTSOE API into data warehouse",
    schedule_interval="0 * * * *",
)

start_operator = DummyOperator(task_id="starting_dag_execution", dag=dag)

extract_generation = PythonOperator(
    task_id="extract_generation",
    dag=dag,
    python_callable=extract_raw_data,
    op_kwargs={
        "metrics_label": "total_generation",
        "start": "{{ data_interval_start.format('YYYYMMDDHHmm') }}",
        "end": "{{ data_interval_end.format('YYYYMMDDHHmm') }}",
        "timezone": tz,
        "country_code": country_code,
    },
)




start_operator >> extract_generation
