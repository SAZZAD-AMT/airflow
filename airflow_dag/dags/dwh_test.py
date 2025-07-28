from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql import SparkSession
import os

def spark_upload_employee():

    os.environ['JAVA_HOME'] = '/usr/local/sdkman/candidates/java/current'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
    
    # Initialize Spark with comprehensive MinIO configuration
    spark = SparkSession.builder \
        .appName("Upload Employee Data to MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5") \
        .config("spark.hadoop.fs.s3a.retry.limit", "5") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.driver.extraJavaOptions", 
               "-Dfs.s3a.connection.timeout=60000 " +
               "-Dfs.s3a.socket.timeout=60000") \
        .config("spark.executorEnv.JAVA_HOME", os.environ['JAVA_HOME']) \
        .getOrCreate()

    try:
        # Verify/create the employee directory
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.org.apache.hadoop.fs.URI("s3a://dwh"),
            hadoop_conf
        )
        employee_path = spark._jvm.org.apache.hadoop.fs.Path("s3a://dwh/employee")
        if not fs.exists(employee_path):
            fs.mkdirs(employee_path)
        
        # Create and save DataFrame
        data = [(101, "Sazzad", "IT", 60000), (102, "Arafat", "HR", 55000), (103, "Rafi", "Finance", 58000)]
        df = spark.createDataFrame(data, ["emp_id", "name", "department", "salary"])
        
        df.write.mode("overwrite") \
           .option("header", "true") \
           .csv("s3a://dwh/employee/employee_data")
        
        print("Data successfully uploaded to MinIO")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()

with DAG(
    'spark_upload_employee_to_minio',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    upload_task = PythonOperator(
        task_id='spark_upload_employee',
        python_callable=spark_upload_employee
    )