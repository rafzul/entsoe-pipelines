import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from 

default_args = {
    'owner': 'rafzul',
    'start_date': datetime(2021, 01, 01, 00, 00),
    'end_date': datetime(2021, 01, 01, 06, 00),
    'depends on past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('ingestion-energydata',
          default_args=default_args,
          description='Builds a DAG to ingest energy data from ENTSOE API into data warehouse',
          schedule_interval='0
          * * * *',
          )

start_operator = DummyOperator(task_id='starting dag execution', dag=dag)

extract_generation = PythonOperator(
    task_id='extract_generation',
    dag=dag,
    python_callable=extract_generation,
)