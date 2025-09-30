from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# function that writes logs
def log_hello():
    logging.info("Hello from Airflow DAG!")
    logging.info("If you see this, logs are working fine 🚀")
    return "log written"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_log_dag",
    default_args=default_args,
    description="A simple DAG to test Airflow logging",
    schedule_interval="*/5 * * * *",  # run every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test", "logs"],
) as dag:

    task1 = PythonOperator(
        task_id="log_message",
        python_callable=log_hello,
    )

    task1
