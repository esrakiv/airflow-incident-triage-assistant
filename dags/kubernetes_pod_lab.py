from datetime import datetime, timezone

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG


with DAG(
    dag_id="kubernetes_pod_lab",
    description="Start a pod in the local Kubernetes lab and collect its output.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["incident-lab", "kubernetes"],
):
    KubernetesPodOperator(
        task_id="start_demo_pod",
        name="airflow-triage-demo",
        namespace="airflow-triage-lab",
        config_file="/opt/airflow/kubeconfig-triage.json",
        in_cluster=False,
        image="busybox:1.37",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from the Airflow-launched Kubernetes pod'"],
        automount_service_account_token=False,
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=90,
        on_finish_action="keep_pod",
    )


with DAG(
    dag_id="kubernetes_image_pull_lab",
    description="Reproduce an Airflow task failure caused by a missing container image tag.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["incident-lab", "kubernetes", "intentional-failure"],
):
    KubernetesPodOperator(
        task_id="start_missing_image_pod",
        name="airflow-triage-image-pull",
        namespace="airflow-triage-lab",
        config_file="/opt/airflow/kubeconfig-triage.json",
        in_cluster=False,
        image="docker.io/library/busybox:triage-demo-tag-does-not-exist",
        automount_service_account_token=False,
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=35,
        on_finish_action="keep_pod",
        retries=0,
    )
