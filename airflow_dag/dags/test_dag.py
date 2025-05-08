from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_message():
    print("✅ Hello from the Airflow task!")

with DAG(
    dag_id='test_dag',
    start_date=datetime(2025, 5, 5),
    schedule='@daily',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    print_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_message
    )

    end = EmptyOperator(task_id='end')

    start >> print_task >> end
