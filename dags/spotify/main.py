from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from custom_operator.spotify_operator import SpotifyOperator


import sys
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Airflow', 
    'depends_on_past' : False, 
    'start_date' : datetime(2020, 3, 1), 
    'end_date' : datetime(2020, 3, 2), 
    'email': ['ariflow@example.com'], 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 0, 
    'retry_delay': timedelta(minutes=10), 
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
      dag_id="spotify", default_args=default_args, schedule_interval=timedelta(days=1)
    )

t0 = DummyOperator(
    task_id='start',
    trigger_rule='dummy',
    dag=dag,
)

t1 = SpotifyOperator(
    task_id="read_credential",
    conf='./dags/spotify/conf/credentials.yml',
    dag=dag
)

t0 >> t1