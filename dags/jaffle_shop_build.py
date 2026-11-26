import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.sdk import DAG, task


JAFFLE_SHOP_SOURCE = Path("/opt/jaffle-shop")
DBT = "/home/airflow/dbt-venv/bin/dbt"


with DAG(
    dag_id="jaffle_shop_build",
    description="Build and test the Jaffle Shop dbt project on DuckDB.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["jaffle-shop", "dbt"],
):

    @task
    def dbt_build() -> None:
        # Each attempt gets a fresh DuckDB file and build directory.
        with TemporaryDirectory(prefix="jaffle-shop-") as directory:
            project = Path(directory)
            for name in ("dbt_project.yml", "profiles.yml"):
                shutil.copy2(JAFFLE_SHOP_SOURCE / name, project / name)
            for name in ("models", "seeds"):
                shutil.copytree(JAFFLE_SHOP_SOURCE / name, project / name)

            # Stream child output through the task's captured stdout.
            with subprocess.Popen(
                [DBT, "--no-use-colors", "build", "--profiles-dir", "."],
                cwd=project,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
            ) as process:
                assert process.stdout is not None
                for line in process.stdout:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                exit_code = process.wait()
            if exit_code:
                raise subprocess.CalledProcessError(exit_code, process.args)

    dbt_build()
