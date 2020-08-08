from airflow import DAG, settings
from airflow.executors.celery_executor import CeleryExecutor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow', 
    'depends_on_past' : False, 
    'start_date' : datetime(2020, 8, 1), 
    # 'end_date' : datetime(2020, 3, 2),
    # 'email': ['ariflow@example.com'], 
    # 'email_on_failure': False, 
    # 'email_on_retry': False, 
    'retries': 0,
    'retry_delay': timedelta(minutes=10), 
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


DAG_NAME = "spotify-etl"

dag = DAG(
      dag_id=DAG_NAME, default_args=default_args, schedule_interval="@once"
    )

t0 = DummyOperator(
    task_id='spotify-etl',
    trigger_rule='dummy',
    dag=dag,
)

t1 = SparkSubmitOperator(
    task_id='spark_submit_job',
    conn_id='_demo',
    application=f'{settings.DAGS_FOLDER}/{DAG_NAME}/demo.py',
    packages="org.apache.hadoop:hadoop-aws:2.7.1",
    dag=dag,
)

t0 >> t1