from pyspark.sql import SparkSession
import time

def spark_upload_employee():
    spark = SparkSession.builder \
        .appName("Upload Employee Data to MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    print("✅ Spark is running. Visit http://localhost:4040 to view the Spark UI.")

    # Dummy data (you can replace this with your actual logic)
    df = spark.createDataFrame([
        (1, "Sazzad", "Dhaka"),
        (2, "Hossen", "Naogaon")
    ], ["id", "name", "location"])

    df.show()

    input("Press Enter to stop...")  # Keeps Spark UI open
    spark.stop()

if __name__ == "__main__":
    spark_upload_employee()
