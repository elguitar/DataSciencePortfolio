from io import StringIO
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks import S3_hook
from cachier import cachier
import pandas as pd
from airflow.operators.python_operator import PythonOperator

from operators.multiline_bash_operator import MultilineBashOperator

BUCKET_NAME="elguitar-data-engineering-demo-bucket"

def get_s3_object(check=False):
    """Simple wrapper, which ensures that the bucket exists if wanted"""
    sthree = S3_hook.S3Hook()
    if check and not sthree.check_for_bucket(BUCKET_NAME):
        sthree.create_bucket(BUCKET_NAME)
    return sthree

def save_to_s3(data, key):
    sthree = get_s3_object()
    return sthree.load_string(data, key, BUCKET_NAME, replace=True)

@cachier()
def read_csv(key):
    return pd.read_csv("s3://" + BUCKET_NAME + "/" + key)

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020,11,7)
}

with DAG(
    'daily_spotify',
    default_args=default_args,
    description="Do the daily tasks to spotify data",
    schedule_interval=timedelta(days=1),
) as dag:

    def listens_of_the_day(**context):
        keys = context['task_instance'].xcom_pull(task_ids='list_s3_objects')
        # We need only the track listing
        keys = [key for key in keys if key.startswith('track_list_')]
        dfs = []
        if keys == []:
            return
        for key in keys:
            dfs.append(read_csv(key))
        # Concat the reversed list of dfs. Drop duplicates and reset the index
        # WATCH OUT FOR THE TIME COMPLEXITY OF THIS!!!!
        df = pd.concat(dfs[::-1]).drop_duplicates().reset_index(drop=True)
        df.played_at = pd.to_datetime(df.played_at)
        today_df = df[(df.played_at > context.get('ds')) & (df.played_at < context.get('tomorrow_ds'))]
        key = "daily_listened_tracks_" + context.get('ds') + ".csv"
        csv_contents = StringIO()
        today_df.to_csv(csv_contents)
        save_to_s3(csv_contents.getvalue(), key)
        print(today_df)
        return key

    def count_top_artists(**context):
        keys = context['task_instance'].xcom_pull(task_ids='list_s3_objects')
        # We need only the track listing
        keys = [key for key in keys if key.startswith('track_list_')]
        dfs = []
        if keys == []:
            return
        for key in keys:
            dfs.append(read_csv(key))
        # Concat the reversed list of dfs. Drop duplicates and reset the index
        # WATCH OUT FOR THE TIME COMPLEXITY OF THIS!!!!
        df = pd.concat(dfs[::-1]).drop_duplicates().reset_index(drop=True)
        df['n'] = 1
        top_artist_df = df.groupby('artist').n.sum()
        csv_contents = StringIO()
        top_artist_df.to_csv(csv_contents)
        key = "top_artists_" + context.get('ds') + ".csv"
        save_to_s3(csv_contents.getvalue(), key)
        return key


    # Using a custom MultilineBashOperator just to prove that I am aware of
    # operators other than PythonOperator :D
    list_s3_objects = MultilineBashOperator(
        task_id = "list_s3_objects",
        bash_command = "aws s3 ls s3://" + BUCKET_NAME + " | awk '{print $4}'",
        xcom_push = True,
    )

    daily_listens = PythonOperator(
        task_id = 'listens_of_the_day',
        python_callable = listens_of_the_day,
        provide_context = True,
    )

    top_artists = PythonOperator(
        task_id = 'top_artists',
        python_callable = count_top_artists,
        provide_context = True,
    )

    list_s3_objects >> [daily_listens, top_artists]
