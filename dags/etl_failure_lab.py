import logging
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


logger = logging.getLogger(__name__)


with DAG(
    dag_id="etl_failure_lab",
    description="Generate three intentional ETL failures using local demo data.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["incident-lab", "intentional-failure"],
):

    @task
    def load_customers() -> None:
        with closing(sqlite3.connect(":memory:")) as database:
            database.execute("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY)")
            database.execute("INSERT INTO customers (customer_id) VALUES (?)", (1,))
            logger.info("Loading customer 1 with loyalty_tier into the target table")
            database.execute(
                "UPDATE customers SET loyalty_tier = ? WHERE customer_id = ?",
                ("gold", 1),
            )

    @task
    def list_raw_events() -> None:
        # Model a read-only identity that can fetch known objects but cannot list them.
        allowed_operations = {"GetObject"}
        operation = "ListObjectsV2"
        logger.info("Listing demo events as lab-reader; simulated storage policy")
        if operation not in allowed_operations:
            raise PermissionError("AccessDenied: lab-reader cannot list demo raw events")

    @task(execution_timeout=timedelta(seconds=3))
    def rebuild_fact_sessions() -> None:
        logger.info("Simulating a warehouse operation that takes 15 seconds")
        time.sleep(15)

    @task
    def publish_customer_report() -> None:
        logger.info("Customer report published")

    load_customers() >> publish_customer_report()
    list_raw_events()
    rebuild_fact_sessions()
