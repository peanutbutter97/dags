import os
from airflow import DAG
from airflow import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from datetime import datetime, timedelta
import pendulum
from time import sleep, time

# subtract(days=1).
start_date = datetime(2025, 10, 9, 7, 35, 00)  # pendulum.now().replace(hour=8, minute=0, second=0, microsecond=0)
default_args = {
    'owner': 'kang',
    'start_date': start_date}


def job(**kwargs):
    print(f"Job {kwargs.get('status', 'undefined')} at {datetime.now()}")


def process_job():
    print("[Start] Processing job...")
    sleep(10)
    print("[Completed] Processing job...")


def get_cronjob_spec():
    hook = KubernetesHook()
    api = hook.batch_v1_client
    cronjob = api.read_namespaced_cron_job("manual-trigger-job", "airflow-cluster")
    return cronjob.spec.job_template.spec.template.spec


pod_config = {
    "KubernetesExecutor": {
        "request_memory": "256Mi",
        "limit_memory": "512Mi",
        "request_cpu": "200m",
        "limit_cpu": "450m",
        "labels": {"type": "python-task"},
        "annotations": {"purpose": "resource-test"},
    }
}

with DAG(
    'Simple_Workflow',
    default_args=default_args,
    description='Simple ETL Airflow',
    schedule=None # "*/10 * * * *"  # timedelta(days=1),
) as dag:
    job_start = PythonOperator(
        task_id='Job_Start',
        python_callable=job,
        op_kwargs={"status": "Started"}
    )

    job_completed = PythonOperator(
        task_id='Job_End',
        python_callable=job,
        op_kwargs={"status": "Completed"},
        op_args=[]
    )

    job_processing = PythonOperator(
        task_id='Job_Processing',
        python_callable=process_job,
        executor_config=pod_config
    )

    trigger_cronjob = KubernetesJobOperator(
        # task_id='Trigger_cronjob',
        # job_name='manual-trigger-job',
        # namespace='airflow-cluster',
        # job_template=get_cronjob_spec(),
        task_id="trigger_manual_cronjob",
        job_name=f"manual-run-{int(time())}",
        body={
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": f"manual-trigger-job", "namespace": "airflow-cluster"},
            "spec": {
                "template": {"spec": get_cronjob_spec()},
            },
        },
)
    # trigger_cronjob = BashOperator(
    #     task_id='Trigger_cronjob',
    #     bash_command='pip list',
    #     # bash_command='kubectl create job --from=cronjob/$job_name $job_name-$(date +%s) -n $namespace',
    #     env={"namespace": "airflow-cluster", "job_name": "manual-trigger-job"}
    # )

    job_start >> job_processing >> trigger_cronjob >> job_completed
    # kubectl describe pod airflow-cluster-triggerer-bd6d789b4-zkvw9 -n airflow-cluster