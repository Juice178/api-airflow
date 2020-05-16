from airflow import DAG
from airflow.executors.celery_executor import CeleryExecutor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
import pandas as pd
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
    parameter = read_credential(conf)
    sp_client = Spotipy(parameter['client_id'], parameter['client_secret'])
    context['task_instance'].xcom_push(key='sp_client', value = sp_client)
    msg = sp_client.debug()
    print(msg)
    context['task_instance'].xcom_push(key='parameter', value = parameter)


def _get_top50(country, **context):
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    parameter = context['task_instance'].xcom_pull(key='parameter')
    playlist = sp_client.get_playlist_tracks(playlist_id=parameter[f"{country}_top50"], limit=50)
    context['task_instance'].xcom_push(key=f"{country}_top50_playlist", value = playlist)


def _get_artist_info(country, **context):
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    if country == "JP":
        playlist = context['task_instance'].xcom_pull(key='japan_top50_playlist')
    else:
        playlist = context['task_instance'].xcom_pull(key='global_top50_playlist')

    artist_ids = playlist['artist_id']

    for i, artist_id in enumerate(artist_ids):
        tracks = sp_client.get_artist_top_10_tracks(artist_id, country)
        if i == 0:
            df = create_dataframe(artist_id, tracks)
        else:
            tmp_df = create_dataframe(artist_id, tracks)
            df = df.append(tmp_df, ignore_index=True)


def create_dataframe(artist_id, tracks):
    d = {'artist_id': [artist_id] * len(tracks), 'album_name': [], 'song_name': [], 'release_date': [], 'total_tracks': []}
    for track in tracks:
        d['album_name'].append(track['album']['name'])
        d['song_name'].append(track['name'])
        d['release_date'].append(track['album']['release_date'])
        d['total_tracks'].append(track['album']['total_tracks'])
    df = pd.DataFrame(data=d)
    return df

def subdag(parent_dag_name, child_dag_name, args, t2, **context):
    """ 各idに対して実行する処理フローを記述したDAGを返す """
    #sp_client = context['task_instance'].xcom_pull(key='sp_client')
    sub_dag = DAG(dag_id=f"{parent_dag_name}.{child_dag_name}", default_args=args)
    #sub_dag = DAG(dag_id="{}.{}".format(parent_dag_name, child_dag_name), default_args=args)
    print("-- test end --")
    for country in ['US', 'JPN']:
        t3 = PythonOperator(
            task_id='{}-task-1'.format(country),
            # conf='./dags/spotify/conf/credentials.yml',
            python_callable = _test,
            provide_context=True,
            dag=sub_dag
        )
        # t2 >> t3

    return sub_dag

def _test(**context):
    sp_client = context['task_instance'].xcom_pull(key='sp_client')
    print(sp_client.debug())


DAG_NAME = "spotify"

dag = DAG(
      dag_id=DAG_NAME, default_args=default_args, schedule_interval=timedelta(days=1)
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

# t3 = SubDagOperator(
#     task_id='subdag',
#     # executor=CeleryExecutor(),  # デフォルトはSequentialExecutorで並列実行されない
#     subdag=subdag(DAG_NAME, 'subdag', default_args, t2),
#     default_args=default_args,
#     provide_context=True,
#     dag=dag,
# )

t0 >> t1 >> [t2, t3] 
t2 >> t4
t3 >> t5