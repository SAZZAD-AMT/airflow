from airflow import DAG
from airflow.operators.empty import DummyOperator
from datetime import datetime

with DAG(
    'test_dag',
    start_date=datetime(2023,1,1),
    schedule=None
) as dag:
    DummyOperator(task_id='dummy')