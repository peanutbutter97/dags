from datetime import datetime
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import V1Volume, V1VolumeMount, V1ConfigMapVolumeSource, V1ResourceRequirements

def create_hello_world_pod():
    volume = V1Volume(
        name="hello-src-volume",
        config_map=V1ConfigMapVolumeSource(name="hello-src")
    )

    volume_mount = V1VolumeMount(
        name="hello-src-volume",
        mount_path="/app"
    )

    resources = V1ResourceRequirements(
        requests={"cpu": "250m", "memory": "256Mi"},
        limits={"cpu": "500m", "memory": "512Mi"}
    )

    hello_pod = KubernetesPodOperator(
        task_id="trigger_hello_cron_job",
        name="trigger-hello-cron",
        namespace="airflow-cluster",
        image="bitnami/kubectl:latest",
        cmds=["kubectl"],
        arguments=[
            "create", "job",
            "--from=cronjob/manual-trigger-job",
            "manual-hello-job", "-n", "airflow-cluster"
        ],
        container_resources=resources,
        is_delete_operator_pod=True,
        get_logs=True,
    )

    # hello_pod = KubernetesPodOperator(
    #         task_id="run-hello-world",
    #         name="hello-world",
    #         namespace="airflow-k8s-task",
    #         image="python:3.12-slim",
    #         cmds=["bash", "-c"],
    #         arguments=[
    #             "pip install --no-cache-dir -r /app/requirements.txt && python /app/hello.py"
    #         ],
    #         container_resources=resources,
    #         volumes=[volume],
    #         volume_mounts=[volume_mount],
    #         is_delete_operator_pod=True,
    #         get_logs=True,
    #     )

    return hello_pod

@dag(
    dag_id="airflow_k8s_hello_task",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def taskflow_k8s_dag():
    hello_pod = create_hello_world_pod()

taskflow_k8s_dag = taskflow_k8s_dag()