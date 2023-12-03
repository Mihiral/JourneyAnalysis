from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import dataIngest
import eventTransform

# Set default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG
dag = DAG(
    's3_to_api',
    default_args=default_args,
    description='PySpark project to ingest data from AWS S3 and post to an external API',
    schedule_interval='@daily',  # Adjust the schedule as needed
)

# Define PySpark task

# Create PySpark task
pyspark_task = PythonOperator(
    task_id='run_pyspark_job',
    python_callable=dataIngest.run_pyspark_job(),
    provide_context=True,
    dag=dag,
)

# Create API post task
api_post_task = PythonOperator(
    task_id='post_to_api',
    python_callable=eventTransform.post_to_api(),
    provide_context=True,
    dag=dag,
)

# Set task dependencies
pyspark_task >> api_post_task
