import os
import unittest
from unittest.mock import patch

from airflow_logs import authenticated_get, download_task_log, parse_run_url


class ConfiguredAirflowTest(unittest.TestCase):
    base = "https://airflow.example.org/airflow"

    def test_task_log_uses_configured_base_and_bearer_token(self):
        task_url = f"{self.base}/dags/daily/runs/run%2B1/tasks/load"
        settings = {"AIRFLOW_TRIAGE_URL": f"{self.base}/", "AIRFLOW_TRIAGE_TOKEN": "test-token"}
        with patch.dict(os.environ, settings), patch(
            "airflow_logs.get_with_token", return_value={"content": "task log"}
        ) as get:
            self.assertEqual(download_task_log(task_url, 2), "task log")
        get.assert_called_once_with(
            f"{self.base}/api/v2/dags/daily/dagRuns/run%2B1/taskInstances/load/logs/2"
            "?full_content=true&map_index=-1",
            "test-token",
        )

    def test_run_url_supports_a_configured_path_prefix(self):
        with patch.dict(os.environ, {"AIRFLOW_TRIAGE_URL": self.base}):
            self.assertEqual(parse_run_url(f"{self.base}/dags/daily/runs/run%2B1"), ("daily", "run+1"))

    def test_rejects_other_hosts_and_paths_before_sending_token(self):
        settings = {"AIRFLOW_TRIAGE_URL": self.base, "AIRFLOW_TRIAGE_TOKEN": "test-token"}
        with patch.dict(os.environ, settings), patch("airflow_logs.get_with_token") as get:
            for url in ("https://other.example.org/airflow/api/v2/dags", f"{self.base}-other/api/v2/dags"):
                with self.subTest(url=url), self.assertRaisesRegex(ValueError, "AIRFLOW_TRIAGE_URL"):
                    authenticated_get(url)
        get.assert_not_called()

    def test_configured_instance_requires_token_and_https(self):
        with patch.dict(os.environ, {"AIRFLOW_TRIAGE_URL": self.base, "AIRFLOW_TRIAGE_TOKEN": ""}):
            with self.assertRaisesRegex(ValueError, "AIRFLOW_TRIAGE_TOKEN"):
                authenticated_get(f"{self.base}/api/v2/dags")
        with patch.dict(os.environ, {"AIRFLOW_TRIAGE_URL": "http://airflow.example.org"}):
            with self.assertRaisesRegex(ValueError, "HTTPS"):
                parse_run_url("http://airflow.example.org/dags/daily/runs/run")
