from airflow import DAG, settings
from airflow.executors.celery_executor import CeleryExecutor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import pandas as pd
import os
import pendulum
# from custom_operator.spotify_operator import SpotifyOperator


from apis.spotify import Spotipy
from lib.config import read_credential
from lib.s3 import write_df_to_s3
from lib.parameter import get_parameter

import sys
from datetime import datetime, timedelta
import os

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


def _create_instance(**context):
    """
    Create an instance of a wrapper class for Spotify API 
    """
    print(os.listdir())
    env = os.getenv('env', 'stg')
    conf = f'{settings.DAGS_FOLDER}/{DAG_NAME}/conf/{env}/credentials.yml'
    parameter = read_credential(conf)
    sp_client = Spotipy(parameter['client_id'], parameter['client_secret'])
    context['task_instance'].xcom_push(key='sp_client', value = sp_client)
    msg = sp_client.debug()
    print(msg)
    context['task_instance'].xcom_push(key='parameter', value = parameter)


def _get_top50(country, **context):
    """
    Get Spotify catalog information about an artist's top 10 tracks
    """
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    parameter = context['task_instance'].xcom_pull(key='parameter')
    playlist = sp_client.get_playlist_tracks(playlist_id=parameter[f"{country}_top50"], limit=50)
    df = pd.DataFrame(data=playlist)
    print("top 50 songs: ")
    print(df.head())
    # print(playlist)
    context['task_instance'].xcom_push(key=f"{country}_top50_playlist", value = playlist)


def _get_artist_info(country, **context):
    """
    Get the most popular 10 songs for an artist
    """
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    if country == "JP":
        playlist = context['task_instance'].xcom_pull(key='japan_top50_playlist')
    else:
        playlist = context['task_instance'].xcom_pull(key='global_top50_playlist')

    artist_ids = playlist['artist_id']

    for i, artist_id in enumerate(artist_ids):
        tracks = sp_client.get_artist_top_10_tracks(artist_id, country)
        if i == 0:
            df = create_dataframe(artist_id, tracks, country)
        else:
            tmp_df = create_dataframe(artist_id, tracks, country)
            df = df.append(tmp_df, ignore_index=True)

    # print("os.listdir")
    # print(os.listdir())
    # TODO: Use parameter store
    # parameter = read_credential(f"{settings.PLUGINS_FOLDER}/secrets/aws_access_key.yml")
    parameter = get_parameter("airflow-s3")
    execution_date = context['execution_date']
    print(f"Execution date is {execution_date}")
    partition_dt = get_partition_time(execution_date)
    env = os.getenv('env', 'stg')
    s3_bucket = f"data-lake-{env}"
    s3_path = f"/spotify/top50/{partition_dt}"
    file_name = f"/top10_popular_songs_of_artists-{country}.csv"
    outpath = "s3:///" + s3_bucket + s3_path + file_name
    #outpath = f's3:///data-lake-{env}/spotify/top50/{partition_dt}/top10_popular_songs_of_artists-{country}.csv'
    write_df_to_s3(df, outpath, parameter)

    context['task_instance'].xcom_push(key=f"s3_path", value=f"{s3_bucket}{s3_path}")
    print("Succeeded in writing csv file to s3")


def create_dataframe(artist_id, tracks, country):
    """
    Create a dataframe containing information about each artist
    """

    d = {'artist_id': [artist_id] * len(tracks), 'album_name': [], 'song_name': [], 'release_date': [], 'total_tracks': [], 'country': []}
    for track in tracks:
        d['album_name'].append(track['album']['name'])
        d['song_name'].append(track['name'])
        d['release_date'].append(track['album']['release_date'])
        d['total_tracks'].append(track['album']['total_tracks'])
        d['country'].append(country)
    df = pd.DataFrame(data=d)
    return df


def get_partition_time(execution_date):
    dt = f"dt_y={execution_date.format('%Y')}/dt_m={execution_date.format('%Y-%m')}/dt_d={execution_date.format('%Y-%m-%d')}"
    return dt

def _test(**context):
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    print(sp_client.debug())

def trigger(context, dag_run_obj):
    if context['params']['condition_param']:
        dag_run_obj.payload = {"s3_path" : context['task_instance'].xcom_pull(key='s3_path')}
        print(dag_run_obj.payload)
        return dag_run_obj


DAG_NAME = "spotify-collect"

dag = DAG(
      dag_id=DAG_NAME, default_args=default_args, schedule_interval="@daily"
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
    # op_kwargs={'conf': './dags/spotify/conf/credentials.yml'},
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id="get_global_top50",
    # conf='./dags/spotify/conf/credentials.yml',
    python_callable =  _get_top50,
    provide_context=True,
    op_kwargs={'country': 'global'},
    dag=dag
)

t3 = PythonOperator(
    task_id="get_japan_top50",
    # conf='./dags/spotify/conf/credentials.yml',
    python_callable =  _get_top50,
    provide_context=True,
    op_kwargs={'country': 'japan'},
    dag=dag
)

t4 = PythonOperator(
    task_id="top_10_tracks-US",
    # conf='./dags/spotify/conf/credentials.yml',
    python_callable =  _get_artist_info,
    provide_context=True,
    op_kwargs={'country': 'US'},
    dag=dag
)

t5 = PythonOperator(
    task_id="top_10_tracks-JP",
    # conf='./dags/spotify/conf/credentials.yml',
    python_callable =  _get_artist_info,
    provide_context=True,
    op_kwargs={'country': 'JP'},
    dag=dag
)

t6 = TriggerDagRunOperator(
    task_id="trigger_dagrun",
    trigger_dag_id="spotify-etl",  # Ensure this equals the dag_id of the DAG to trigger
    python_callable=trigger,
    params={'condition_param': True},
    dag=dag,
)

t0 >> t1 >> [t2, t3] 
t2 >> t4
t3 >> t5
t4 >> t6
t5 >> t6