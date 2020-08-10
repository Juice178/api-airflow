from airflow import DAG, settings
from airflow.executors.celery_executor import CeleryExecutor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from lib.config import read_credential
import json

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



def check_dagrun_conf(**context):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param dict kwargs: Context
    :param context: The execution context
    :type context: dict
    """
    # print("Remotely received value of {} for key=message".
    #       format(kwargs['dag_run'].conf['message']))
    print("Remotely received value of {} for key=message".format(context["dag_run"].conf["s3_path"]))

_config = {
    'master' : 'local',
    'deploy-mode' : 'client'
}

t0 = DummyOperator(
    task_id='spotify-etl',
    trigger_rule='dummy',
    dag=dag,
)

t1 = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=check_dagrun_conf,
    dag=dag,
)

t2 = SparkSubmitOperator(
    task_id='spark-task',
    application=f'{settings.DAGS_FOLDER}/{DAG_NAME}/demo.py',
    packages="org.apache.hadoop:hadoop-aws:2.7.1",
    application_args=[json.dumps(read_credential(f"{settings.PLUGINS_FOLDER}/secrets/aws_access_key.yml")), '{{ dag_run.conf["s3_path"]}}'],
    dag=dag,
    **_config
)

t0 >>t1 >> t2

# t0 >> t1