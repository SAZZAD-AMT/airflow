airflow standalone start airflow


pkill -9 -f airflow   kill all post

# Confirm your DAGs folder location
airflow config get-value core dags_folder

airflow scheduler
airflow webserver

# Check Airflow can see it
airflow dags list
