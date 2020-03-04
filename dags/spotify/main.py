from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from utils.config import get_info
import os
import sys


default_args = {
    'owner': 'Airflow', 
    'depends_on_past' : False, 
    'start_date' : datetime(2015, 6, 1), 
    'email': ['ariflow@example.com'], 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

path = os.getcwd()
dag_name = os.path.basename(path)
dag = DAG(
      dag_name, default_args=default_args, schedule_interval=timedelta(days=1)
    )


t0 = DummyOperator(
    task_id='start',
    trigger_rule='dummy',
    dag=dag,
)

t1 = PythonOperator(
    task_id="read_credential", 
    python_callable=get_info(dag_name),
    dag=dag
)

t0 >> t1