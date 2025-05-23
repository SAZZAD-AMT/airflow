import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.settings import AIRFLOW_HOME
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from datetime import timedelta


# Define default arguments
default_args = {
    'owner': 'sazzad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Dhaka")
now = pendulum.now("Asia/Dhaka")

dag = DAG(
    dag_id='amarawaz_api_call',
    schedule='0 7 * * *',  
    start_date=datetime(2024, 1, 2, tzinfo=local_tz),  
    catchup=False,
    )

# Function to login and get the session token
def login_to_api(username, password, **kwargs):
    login_url = "https://walton-amar-awaz-prod.com/api/admin/login"

    try:
        # Sending login request
        response = requests.post(login_url, json={"username": username, "password": password}, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes

        # Assuming the response contains a token
        data = response.json()
        token = data.get("token")  # Adjust this depending on the API response structure
        if not token:
            raise ValueError("Login failed, no token received.")
        
        print("Login successful, token:", token)
        return token
    except requests.exceptions.RequestException as e:
        print("Login request failed:", e)
        raise

def fetch_admin_auto_approve_product(**kwargs):
    print("Fetching Admin Auto Approve Product")
    print(f"Fetching Without Warrenty products from {startDate} to {endDate}")
    ti = kwargs['ti']
    token = ti.xcom_pull(task_ids='login_to_api_task')

    if not token:
        raise ValueError("Token not found in XCom.")
    api_url = "https://walton-amar-awaz-prod.com/api/admin/autoapproveproducts"
    
    headers = {
        "User-Agent": "PostmanRuntime/7.32.2",
        "Authorization": f"Bearer {token}",  
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }
    params = {}
    
    try:
        response = requests.post(api_url, headers=headers, json=params, timeout=100)
        response.raise_for_status()
        
        if response.status_code == 200:
            data = response.json()
            print("API Response (Auto Approve Products):", data)
            
            # Correct counting logic based on actual response structure
            if 'data' in data and isinstance(data['data'], list):
                product_count = len(data['data'])
                print(f"Number of auto-approved products: {product_count}")
            else:
                product_count = 0
                print("No products found in response")
            
            return {
                "api_response": data,
                "count": product_count
            }
        else: 
            print(f"API call failed with status {response.status_code}: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

# Function to call the first API with the session token
def fetch_removed_products(startDate, endDate, **kwargs):
    print(f"Fetching Without Warrenty products from {startDate} to {endDate}")
    ti = kwargs['ti']
    token = ti.xcom_pull(task_ids='login_to_api_task')  # Retrieve token from XCom
    if not token:
        raise ValueError("Token not found in XCom.")
    
    api_url = "https://walton-amar-awaz-prod.com/api/admin/automaticRemovedProducts"
    headers = {
        "User-Agent": "PostmanRuntime/7.32.2",
        "Authorization": f"Bearer {token}",  
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }
    params = {
        "startDate": startDate,
        "endDate": endDate
    }
    try:
        response = requests.post(api_url, headers=headers, json=params, timeout=100)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            print("API Response (Removed Without Warrenty Card Products):", data)
            print(f"Products found: {len(data.get('product', []))}")
            return data
        else: 
            print(f"API call Removed Without Warrenty Card Products failed with status {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error in fetch_removed_products: {e}")
        return None

# Function to call the second API with the session token
def fetch_removed_low_size_image_products(startDate, endDate, **kwargs):
    print(f"Fetching Low Size IMG from {startDate} to {endDate}")
    
    ti = kwargs['ti']
    token = ti.xcom_pull(task_ids='login_to_api_task')  # Retrieve token from XCom
    if not token:
        raise ValueError("Token not found in XCom.")

    api_url = "https://walton-amar-awaz-prod.com/api/admin/automaticRemovedLowSizeImageProducts"
    headers = {
        "User-Agent": "PostmanRuntime/7.32.2",
        "Authorization": f"Bearer {token}",  
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }
    params = {
        "startDate": startDate,
        "endDate": endDate
    }
    try:
        response = requests.post(api_url, headers=headers, json=params, timeout=100)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            print("API Response (Removed Low Size Image Products):", data)
            print(f"Products found: {len(data.get('product', []))}")
            return data
        else: 
            print(f"API call Removed Low Size IMG Products failed with status {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error in fetch_removed_low_size_image_products: {e}")
        return None


# variable declare
global_var = Variable.get("amarawaz_api_call_var", deserialize_json=True)
username = global_var['username']
password = global_var['password']

endDate = local_tz.datetime(now.year, now.month, now.day, 7, 0, 0)
startDate = endDate - timedelta(days=10)

# Convert to string format if needed
startDate = startDate.strftime("%Y-%m-%d")  # Format: YYYY-MM-DD
endDate = endDate.strftime("%Y-%m-%d")      # Format: YYYY-MM-DD
# function call
login_to_api_task = PythonOperator(
        task_id="login_to_api_task",
        python_callable=login_to_api,
        dag=dag,
        op_kwargs={
            "username": username,
            "password": password,
        },
        executor_config={
        "pod_template_file": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "kubernetes/pod_templates/default_template.yaml"),
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"app": "airflow-spark-driver"}),
            spec=k8s.V1PodSpec(
                service_account_name="spark-driver",
                containers=[
                    k8s.V1Container(
                        name="base",
                        ports=[
                            k8s.V1ContainerPort(
                                container_port=20020,
                                name="spark-driver",
                            ),
                        ],
                        resources=k8s.V1ResourceRequirements(
                            requests={
                                "cpu": "1",
                                "memory": "4Gi"
                            },
                            limits={
                                "cpu": "6",
                                "memory": "18Gi"
                            }
                        ),
                    ),
                ],
            )
        ),
    },
)

fetch_admin_auto_approve_product_task = PythonOperator(
        task_id="fetch_admin_auto_approve_product_task",
        python_callable=fetch_admin_auto_approve_product, 
        op_kwargs={
            "startDate": startDate,
            "endDate": endDate,
        },
        dag=dag,
        executor_config={
        "pod_template_file": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "kubernetes/pod_templates/default_template.yaml"),
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"app": "airflow-spark-driver"}),
            spec=k8s.V1PodSpec(
                service_account_name="spark-driver",
                containers=[
                    k8s.V1Container(
                        name="base",
                        ports=[
                            k8s.V1ContainerPort(
                                container_port=20020,
                                name="spark-driver",
                            ),
                        ],
                        resources=k8s.V1ResourceRequirements(
                            requests={
                                "cpu": "1",
                                "memory": "4Gi"
                            },
                            limits={
                                "cpu": "6",
                                "memory": "18Gi"
                            }
                        ),
                    ),
                ],
            )
        ),
    },
)
    
fetch_removed_products_task = PythonOperator(
        task_id="fetch_removed_products_task",
        python_callable=fetch_removed_products, 
        op_kwargs={
            "startDate": startDate,
            "endDate": endDate,
        },
        dag=dag,
        executor_config={
        "pod_template_file": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "kubernetes/pod_templates/default_template.yaml"),
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"app": "airflow-spark-driver"}),
            spec=k8s.V1PodSpec(
                service_account_name="spark-driver",
                containers=[
                    k8s.V1Container(
                        name="base",
                        ports=[
                            k8s.V1ContainerPort(
                                container_port=20020,
                                name="spark-driver",
                            ),
                        ],
                        resources=k8s.V1ResourceRequirements(
                            requests={
                                "cpu": "1",
                                "memory": "4Gi"
                            },
                            limits={
                                "cpu": "6",
                                "memory": "18Gi"
                            }
                        ),
                    ),
                ],
            )
        ),
    },
)

fetch_removed_low_size_image_products_task = PythonOperator(
        task_id="fetch_removed_low_size_image_products_task",
        python_callable=fetch_removed_low_size_image_products, 
        op_kwargs={
            "startDate": startDate,
            "endDate": endDate,
        },
        dag=dag,
        executor_config={
        "pod_template_file": os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "kubernetes/pod_templates/default_template.yaml"),
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels={"app": "airflow-spark-driver"}),
            spec=k8s.V1PodSpec(
                service_account_name="spark-driver",
                containers=[
                    k8s.V1Container(
                        name="base",
                        ports=[
                            k8s.V1ContainerPort(
                                container_port=20020,
                                name="spark-driver",
                            ),
                        ],
                        resources=k8s.V1ResourceRequirements(
                            requests={
                                "cpu": "1",
                                "memory": "4Gi"
                            },
                            limits={
                                "cpu": "6",
                                "memory": "18Gi"
                            }
                        ),
                    ),
                ],
            )
        ),
    },
)

login_to_api_task >> fetch_admin_auto_approve_product_task >> fetch_removed_products_task >> fetch_removed_low_size_image_products_task