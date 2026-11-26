import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


logger = logging.getLogger(__name__)


class DataFreshnessError(RuntimeError):
    """The latest source record is older than the allowed freshness window."""


with DAG(
    dag_id="source_freshness_lab",
    description="Block publishing when a synthetic orders feed is stale.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["incident-lab", "data-quality"],
):

    @task
    def check_orders_freshness() -> None:
        checked_at = datetime.now(timezone.utc)
        max_age = timedelta(minutes=60)

        with closing(sqlite3.connect(":memory:")) as database:
            database.execute("CREATE TABLE raw_orders (updated_at TEXT NOT NULL)")
            database.executemany(
                "INSERT INTO raw_orders (updated_at) VALUES (?)",
                [
                    ((checked_at - timedelta(minutes=age)).isoformat(),)
                    for age in (240, 180)
                ],
            )
            latest_value, = database.execute("SELECT MAX(updated_at) FROM raw_orders").fetchone()

        latest = datetime.fromisoformat(latest_value)
        age = checked_at - latest
        if age > max_age:
            raise DataFreshnessError(
                f"raw_orders is stale: latest updated_at is {latest.isoformat()}, "
                f"{age // timedelta(minutes=1)} minutes old; "
                f"freshness limit is {max_age // timedelta(minutes=1)} minutes."
            )

    @task
    def publish_orders_dashboard() -> None:
        logger.info("Orders dashboard published")

    check_orders_freshness() >> publish_orders_dashboard()
