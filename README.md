airflow standalone start airflow


pkill -9 -f airflow   
kill all post


airflow scheduler
airflow webserver

# Check Airflow can see it
airflow dags list
airflow connections get minio_default

# Dag Path Fix
airflow config get-value core dags_folder
export AIRFLOW__CORE__DAGS_FOLDER="/workspaces/airflow/airflow_dag/dags"

# remove Unnecassary cache filr
rm -rf /workspaces/airflow/airflow_dag/dags/__pycache__/

# db update
airflow db migrate
airflow db check
airflow db reset
airflow db upgrade


# Minio Hosting Guide

mkdir -p ~/minio_data

docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -v ~/minio_data:/data \
  quay.io/minio/minio server /data --console-address ":9001"


  {
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin",
  "host": "http://localhost:9000",
  "endpoint_url": "http://localhost:9000"
}

airflow connections delete minio_default

airflow connections add 'minio_default' \
    --conn-type 'aws' \
    --conn-extra '{"aws_access_key_id": "minioadmin", "aws_secret_access_key": "minioadmin", "host": "http://localhost:9000", "endpoint_url": "http://localhost:9000"}'

airflow dags trigger minio_example

# airflow dag log history
export AIRFLOW__LOGGING__REMOTE_LOGGING=True
export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://airflow-log/
export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=minio_default
export AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=False

export base_log_folder=/workspaces/airflow/airflow_dag/logs




