from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.aws_upload_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': 'ACHRAF ZHAR',
    'start_date': datetime.now() - timedelta(hours=1)
}

file_postfix = datetime.now(pytz.timezone('UTC')).strftime("%Y%m%d_%H%M%S")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['etl', 'Reddit-Palestine', 'PowerBI', 'AWS']
)

extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'Palestine',
        'time_filter': 'day',
        'limit': 500
    },
    dag=dag
)

upload_s3 = PythonOperator(
    task_id='upload_s3',
    python_callable=upload_s3_pipeline,
    dag=dag
)

extract >> upload_s3
