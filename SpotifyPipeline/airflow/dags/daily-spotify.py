from datetime import datetime, timedelta

from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator

from operators.multiline_bash_operator import MultilineBashOperator

BUCKET_NAME="elguitar-data-engineering-demo-bucket"

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
        for key in keys:
            dfs.append(pd.read_csv("s3://" + BUCKET_NAME + "/" + key))
        # Concat the reversed list of dfs. Drop duplicates and reset the index
        # WATCH OUT FOR THE TIME COMPLEXITY OF THIS!!!!
        df = pd.concat(dfs[::-1]).drop_duplicates().reset_index(drop=True)
        df.played_at = pd.to_datetime(df.played_at)
        today_df = df[(df.played_at > context.get('ds')) & (df.played_at < context.get('tomorrow_ds'))]
        print(today_df)

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

    list_s3_objects >> daily_listens
