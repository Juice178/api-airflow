"""
This dag only runs some tasks to get music information from Spoitfy.
After getting information, trigger two dags to
1.Get master data about an artist
2.Partition data by Spark
"""

from airflow import DAG, settings
from airflow.executors.celery_executor import CeleryExecutor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.subdag_operator import SubDagOperator
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


def _create_instance(ssm_key, **context):
    """
    Create an instance of a wrapper class for Spotify API 

    Parameters
    ----------
    ssm_key: str
        name of ssm key with which a key-value pair is to be fetched.
    """
    # ssm_key = "spotify-key"
    # Get access  a key, password pair as a dictionary
    parameter = get_parameter(ssm_key)
    sp_client = Spotipy(parameter['client_id'], parameter['client_secret'])
    context['task_instance'].xcom_push(key='sp_client', value=sp_client)

def _get_top50(country, **context):
    """
    Get Spotify catalog information about an artist's top 10 tracks in the specified country

    Parameters
    ----------
    country: str
        A country in which top 10 tracks are fetched. 
        Possible value: japan, global.
    """
    # Pull a wrapper class for spotipy to manipulate Spotify information
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    env = os.getenv('env', 'stg')
    # Read playlit ids
    conf = f'{settings.DAGS_FOLDER}/{DAG_NAME}/conf/{env}/credentials.yml'
    parameter = read_credential(conf)
    # Get a playlist information as a dictinary
    # Example: playlist_info = {'rank': [1], 'artist_name': [BTS], 'artist_id': [3Nrfpe0tUJi4K4DXYWgMUX],
    #                           'album_name': [Dynamite (DayTime Version)], 'relase_date': [2020-08-28] }
    playlist = sp_client.get_playlist_tracks(playlist_id=parameter[f"{country}_top50"], limit=50)
    df = pd.DataFrame(data=playlist)
    print("top 50 songs: ")
    print(df.head())
    # print(playlist)
    context['task_instance'].xcom_push(key=f"{country}_top50_playlist", value = playlist)


def _get_artist_info(country, **context):
    """
    Get the most popular 10 songs for an artist at execution point(time).

    Parameters
    ----------
    country: str
        A country in which top 10 songs are fetched. 
        Possible value: US, JP.
    """

    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    if country == "JP":
        playlist = context['task_instance'].xcom_pull(key='japan_top50_playlist')
    else:
        playlist = context['task_instance'].xcom_pull(key='global_top50_playlist')

    artist_ids = set(playlist['artist_id'])
    execution_date = context['execution_date']
    t = execution_date.format('%Y-%m-%d')


    # Get top 10 music for each artist
    for i, artist_id in enumerate(artist_ids):
        tracks = sp_client.get_artist_top_10_tracks(artist_id, country)
        if i == 0:
            df = create_dataframe(artist_id, tracks, country, t)
        else:
            tmp_df = create_dataframe(artist_id, tracks, country, t)
            df = df.append(tmp_df, ignore_index=True)

    parameter = get_parameter("airflow-s3")
    print(f"Execution date is {execution_date}")
    partition_dt = get_partition_time(execution_date)
    env = os.getenv('env', 'stg')
    # TODO: Write s3 path in yaml file.
    s3_bucket = f"data-lake-{env}"
    s3_path = f"/spotify/top50/{partition_dt}"
    file_name = f"/top10_popular_songs_of_artists-{country}.csv"
    outpath = "s3:///" + s3_bucket + s3_path + file_name
    #outpath = f's3:///data-lake-{env}/spotify/top50/{partition_dt}/top10_popular_songs_of_artists-{country}.csv'
    write_df_to_s3(df, outpath, parameter)

    context['task_instance'].xcom_push(key=f"s3_path", value=f"{s3_bucket}{s3_path}")
    print("Succeeded in writing csv file to s3")


def create_dataframe(artist_id, tracks, country, t):
    """
    Create a dataframe containing information about each artist

    Parameters
    ----------
    artist_id: str
    tracks: str
    country: str
        A country in which top 10 songs are fetched. 
        Possible value: US, JP.
    t: str
        Execution time.
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
    """
    Format execution data as string date

    Parameters
    ----------
    execution_date: class:`pendulum.pendulum.Pendulum` object
        Execution date
    """

    print(type(execution_date))
    dt = f"dt_y={execution_date.format('%Y')}/dt_m={execution_date.format('%Y-%m')}/dt_d={execution_date.format('%Y-%m-%d')}"
    return dt

def _trigger_spotify_artist(context, dag_run_obj):
    """
    Trigger a DAG to get an artist informaiton(master data).
    """
    if context['params']['condition_param']:
        dag_run_obj.payload = {"artist" : "Zara Larsson"}
        print(f"context: {context}")
        print(dag_run_obj.payload)
        return dag_run_obj

def trigger(context, dag_run_obj):
    """
    Trigger a DAG to partition data by release date by using Spark.
    """
    if context['params']['condition_param']:
        dag_run_obj.payload = {"s3_path" : context['task_instance'].xcom_pull(key='s3_path')}
        print(f"context: {context}")
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
    op_kwargs={'ssm_key': 'spotify-key'},
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
    task_id="trigger_spotify-artist",
    trigger_dag_id="spotify-artist",  # Ensure this equals the dag_id of the DAG to trigger
    python_callable=_trigger_spotify_artist,
    params={'condition_param': True},
    dag=dag,
)

t7 = TriggerDagRunOperator(
    task_id="trigger_spotify-etl",
    trigger_dag_id="spotify-etl",  # Ensure this equals the dag_id of the DAG to trigger
    python_callable=trigger,
    params={'condition_param': True},
    dag=dag,
)

t0 >> t1 >> [t2, t3] 
t2 >> t4
t3 >> t5
t4 >> [t6, t7]
t5 >> [t6, t7]