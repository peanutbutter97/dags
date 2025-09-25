from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Simple Python functions
def hello_task():
    print("✅ Hello from Airflow DAG!")

def fail_task():
    raise Exception("❌ Intentional failure to test scheduler error handling")

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# DAG definition
with DAG(
    dag_id="test_scheduler_dag_1",
    default_args=default_args,
    description="A test DAG for Airflow Scheduler",
    schedule_interval="0 * * * *",   # every 1 minute
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["test"],
) as dag:

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_task,
    )

    task2 = PythonOperator(
        task_id="force_fail",
        python_callable=fail_task,
    )

    task1 >> task2
