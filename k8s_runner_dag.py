from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import V1Volume, V1VolumeMount, V1ConfigMapVolumeSource

@task
def run_hello_world_in_k8s():
    # Define volume from ConfigMap
    volume = V1Volume(
        name="hello-src-volume",
        config_map=V1ConfigMapVolumeSource(name="hello-src")
    )

    # Define mount path inside pod
    volume_mount = V1VolumeMount(
        name="hello-src-volume",
        mount_path="/scripts"
    )

    hello_pod = KubernetesPodOperator(
        task_id="hello_world_pod",
        name="hello-world-pod",
        namespace="default",
        image="python:3.12-slim",
        cmds=["python", "/scripts/hello.py"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )

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