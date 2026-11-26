import time
from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="airflow_heartbeat_timeout_lab",
    description="Run with the isolated heartbeat timeout settings in the README.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["incident-lab", "heartbeat"],
):
    @task
    def wait_for_heartbeat_timeout() -> None:
        time.sleep(600)

    wait_for_heartbeat_timeout()
