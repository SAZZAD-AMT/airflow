from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta  # Modern datetime import

def notification_email(**kwargs):
    if kwargs['ti'].xcom_pull(task_ids='check_data'):
        return 'dummy_task_true'
    else:
        return 'dummy_task'

def always_true():
    return True

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 8),  # Explicit start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'branch_example',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 5, 8),  # Explicit start date
    catchup=False,
    tags=['example'],
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    check_data = PythonOperator(
        task_id='check_data',
        python_callable=always_true,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=notification_email,
    )

    dummy_task_true = EmptyOperator(task_id='dummy_task_true')
    dummy_task = EmptyOperator(task_id='dummy_task')
    dummy_task_final = EmptyOperator(
        task_id='dummy_task_final',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    start_task >> check_data >> branch_task >> [dummy_task_true, dummy_task] >> dummy_task_final