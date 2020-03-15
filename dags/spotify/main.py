from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from custom_operator.spotify_operator import SpotifyOperator


from apis.spotify import Spotipy
from lib.config import read_credential

import sys
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Airflow', 
    'depends_on_past' : False, 
    'start_date' : datetime(2020, 3, 1), 
    'end_date' : datetime(2020, 3, 2), 
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


def _create_instance(conf, **context):
    print(os.listdir())
    client_credential = read_credential(conf)
    sp_client = Spotipy(client_credential['client_id'], client_credential['client_secret'])
    context['task_instance'].xcom_push(key='sp_client', value = sp_client)
    msg = sp_client.debug()
    print(msg)


def _get_top50(**context):
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    playlist_id = 'spotify:playlist:37i9dQZEVXbMDoHDwVN2tF'
    playlist = sp_client.get_playlist_tracks(playlist_id=playlist_id, limit=50)
    print(playlist)


def _test(**context):
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    print(sp_client.debug())

dag = DAG(
      dag_id="spotify", default_args=default_args, schedule_interval=timedelta(days=1)
    )

t0 = DummyOperator(
    task_id='start',
    trigger_rule='dummy',
    dag=dag,
)

t1 = PythonOperator(
    task_id="create_instance",
    # conf='./dags/spotify/conf/credentials.yml',
    python_callable = _create_instance,
    op_kwargs={'conf': './dags/spotify/conf/credentials.yml'},
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id="get_top50",
    # conf='./dags/spotify/conf/credentials.yml',
    python_callable =  _get_top50,
    provide_context=True,
    dag=dag
)

t0 >> t1 >> t2