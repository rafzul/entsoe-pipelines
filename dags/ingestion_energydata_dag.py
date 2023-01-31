import datetime as dtime
import pandas as pd
import os
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# from plugins.scripts.extract_raw import extract_raw_data
from scripts import extract_raw


default_args = {
    "owner": "rafzul",
    "start_date": pendulum.datetime(2021, 1, 1, 0, 00),
    "end_date": pendulum.datetime(2021, 1, 1, 6, 00),
    "depends on past": True,
    "retries": 3,
    "retry_delay": dtime.timedelta(minutes=5),
    "email_on_retry": False,
}

# instiating variables
interval_start = "{{ data_interval_start.format('YYYYMMDDHHmm') }}"
interval_end = "{{ data_interval_end.format('YYYYMMDDHHmm') }}"
tz = "Europe/Berlin"
country_code = "DE_TENNET"

start_date = default_args["start_date"].format("YYYY-MM-DD HH:mm:ss")
end_date = default_args["end_date"].format("YYYY-MM-DD HH:mm:ss")

dag = DAG(
    "entsoe-energydata-backfill",
    default_args=default_args,
    description="Builds a DAG to ingest energy data from ENTSOE API into data warehouse",
    schedule_interval="0 * * * *",
    catchup=True,
)

start_operator = DummyOperator(task_id="starting_dag_execution", dag=dag)

extract_generation = PythonOperator(
    task_id="extract_generation",
    dag=dag,
    python_callable=extract_raw.extract_raw_data,
    op_kwargs={
        "metrics_label": "total_generation",
        "start": interval_start,
        "end": interval_end,
        "timezone": tz,
        "country_code": country_code,
    },
)

stage_total_generation = SparkSubmitOperator(
    task_id="stage_total_generation",
    dag=dag,
    conn_id="spark_local",
    application="/opt/airflow/plugins/scripts/transform_raw_staging.py",
    application_args=[
        "total_generation",
        interval_start,
        interval_end,
        country_code,
        start_date,
        end_date,
    ],
)


start_operator >> extract_generation >> stage_total_generation
