import os
from airflow import DAG
from airflow import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from time import sleep

start_date = pendulum.now().subtract(days=1).replace(hour=10, minute=0, second=0, microsecond=0)
default_args = {
    'owner': 'kang',
    'start_date': start_date}


def job(**kwargs):
    print(f"Job {kwargs.get('status', 'undefined')} at {datetime.now()}")


def process_job():
    print("[Start] Processing job...")
    sleep(10)
    print("[Completed] Processing job...")


with DAG(
    'Simple_Workflow',
    default_args=default_args,
    description='Simple ETL Airflow',
    schedule="*/5 * * * *"  # timedelta(days=1),
) as dag:
    job_start = PythonOperator(
        task_id='Job_Start',
        python_callable=job,
        op_kwargs={"status": "Started"}
    )

    job_completed = PythonOperator(
        task_id='Job_End',
        python_callable=job,
        op_kwargs={"status": "Completed"}
    )

    job_processing = PythonOperator(
        task_id='Job_Processing',
        python_callable=process_job
    )

    job_start >> job_processing >> job_completed