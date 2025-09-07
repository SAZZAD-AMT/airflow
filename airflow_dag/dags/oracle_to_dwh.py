from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
import boto3
import os

# ---------- DAG DEFAULTS ----------
default_args = {
    'owner': 'sazzad',
    'depends_on_past': False,
    'retries': 1,
}

# ---------- CONNECTION SETTINGS ----------
ORACLE_CONN = "oracle+cx_oracle://username:password@host:port/SID"
MINIO_BUCKET = "dwh-bucket"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_FILE_KEY = "oracle_data/employee.parquet"

# ---------- FUNCTIONS ----------
def extract_from_oracle(**kwargs):
    """Extract data from Oracle table to Pandas DataFrame"""
    engine = create_engine(ORACLE_CONN)
    query = "SELECT * FROM employee"  # Change table name here
    df = pd.read_sql(query, engine)
    # Save locally as Parquet
    local_file = "/tmp/employee.parquet"
    df.to_parquet(local_file, index=False)
    # Push file path to XCom for next task
    kwargs['ti'].xcom_push(key='file_path', value=local_file)

def load_to_minio(**kwargs):
    """Upload local Parquet file to MinIO"""
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='extract_oracle')
    
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    s3_client.upload_file(file_path, MINIO_BUCKET, MINIO_FILE_KEY)
    print(f"Uploaded {file_path} to s3://{MINIO_BUCKET}/{MINIO_FILE_KEY}")

# ---------- DAG DEFINITION ----------
with DAG(
    'oracle_to_minio',
    default_args=default_args,
    description='Extract from Oracle and load to MinIO DWH',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['dwh', 'oracle', 'minio'],
) as dag:

    extract_oracle = PythonOperator(
        task_id='extract_oracle',
        python_callable=extract_from_oracle,
        provide_context=True
    )

    load_minio = PythonOperator(
        task_id='load_minio',
        python_callable=load_to_minio,
        provide_context=True
    )

    extract_oracle >> load_minio
