from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

@task
def run_hello_world_in_k8s():
    hello_pod = KubernetesPodOperator(
        task_id="hello_world_pod",
        name="hello-world-pod",
        namespace="default",
        image="python:3.12-slim",
        cmds=["python", "/scripts/hello.py"],
        volumes=[{
            "name": "hello-src-volume",
            "configMap": {"name": "hello-src"}
        }],
        volume_mounts=[{
            "name": "hello-src-volume",
            "mountPath": "/scripts"
        }],
        is_delete_operator_pod=True,
        get_logs=True,
    )
    # Execute the pod when the task runs
    return hello_pod.execute(context={})

@dag(
    dag_id="taskflow_k8s_hello_task_decorator",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def taskflow_k8s_dag():
    run_hello_world_in_k8s()

taskflow_k8s_dag = taskflow_k8s_dag()