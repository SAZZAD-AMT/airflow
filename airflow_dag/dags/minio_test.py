from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_minio():
    hook = S3Hook(aws_conn_id='minio_default')
    hook.load_string(
        string_data="Hello from Airflow!",
        key='test_file.txt',
        bucket_name='test'
    )

with DAG(
    'minio_example',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio
    )