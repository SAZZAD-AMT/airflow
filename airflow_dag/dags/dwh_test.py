from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql import SparkSession
import os

def spark_upload_employee():
    # 1. Clean environment setup
    os.environ['JAVA_HOME'] = '/usr/local/sdkman/candidates/java/current'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
    os.environ['SPARK_LOCAL_IP'] = '10.0.3.29'  # From your netstat output
    
    # 2. Configure Spark with port conflict avoidance
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("Upload Employee Data to MinIO") \
            .config("spark.ui.port", "4043") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.driver.extraJavaOptions", 
                   "-Dfs.s3a.connection.timeout=60000 " +
                   "-Dfs.s3a.socket.timeout=60000 " +
                   "-Djava.net.preferIPv4Stack=true") \
            .getOrCreate()

        # 3. Data processing
        data = [
            (101, "Sazzad", "IT", 60000),
            (102, "Arafat", "HR", 55000),
            (103, "Rafi", "Finance", 58000)
        ]
        df = spark.createDataFrame(data, ["emp_id", "name", "department", "salary"])
        
        # 4. Write to MinIO (bucket exists as per your mc ls output)
        df.write.mode("overwrite") \
           .option("header", "true") \
           .csv("s3a://dwh/employee/employee_data")
        
        print("SUCCESS: Data uploaded to MinIO")
        return True
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
        print("Spark session stopped")

with DAG(
    'spark_upload_employee_to_minio',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'minio']
) as dag:
    
    upload_task = PythonOperator(
        task_id='spark_upload_employee',
        python_callable=spark_upload_employee,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )