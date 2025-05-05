from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


def notification_email(**kwargs):
    if kwargs['ti'].xcom_pull(task_ids='check_data'):
        return 'dummy_task_true'
    else:
        return 'dummy_task'

def always_true():
    return True
    

        
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'branch_example',
    default_args=default_args,
    schedule_interval=None,
)

start_task = DummyOperator(task_id='start_task', dag=dag)


check_data = PythonOperator(
    task_id='check_data',
    python_callable=always_true,
    dag=dag,
)

# Updated to use BranchPythonOperator
branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=notification_email,
    provide_context=True,
    dag=dag,
)


dummy_task_true = DummyOperator(task_id='dummy_task_true', dag=dag)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task_final = DummyOperator(task_id='dummy_task_final', trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

start_task >> check_data >> branch_task >> [dummy_task_true, dummy_task]>>dummy_task_final 


