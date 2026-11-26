import io
import json
import runpy
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

class SummaryTest(unittest.TestCase):
    def test_truncated_response_uses_triage_findings(self):
        report = Path("tests/fixtures/heartbeat-report.json").read_text()
        reply = {
            "choices": [
                {"finish_reason": "length", "message": {"content": None}}
            ]
        }
        output = io.StringIO()

        with patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"}), \
             patch("sys.argv", ["llm_summary.py", "wait_for_heartbeat_timeout"]), \
             patch("sys.stdin", io.StringIO(report)), \
             patch("sys.stdout", output), \
             patch("urllib.request.urlopen", return_value=io.StringIO(json.dumps(reply))):
            runpy.run_path("llm_summary.py", run_name="__main__")

        text = output.getvalue()
        self.assertIn("What happened:", text)
        self.assertIn("First check:", text)
        self.assertIn("#task-instance-heartbeat-timeout", text)
    def test_rate_limit_uses_triage_findings(self):
        report = Path("tests/fixtures/heartbeat-report.json").read_text()
        error = HTTPError(
            "https://openrouter.ai/api/v1/chat/completions",
            429,
            "Too Many Requests",
            hdrs=None,
            fp=None,
        )
        output = io.StringIO()

        with patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"}), \
             patch("sys.argv", ["llm_summary.py", "wait_for_heartbeat_timeout"]), \
             patch("sys.stdin", io.StringIO(report)), \
             patch("sys.stdout", output), \
             patch("urllib.request.urlopen", side_effect=error):
            runpy.run_path("llm_summary.py", run_name="__main__")

        text = output.getvalue()
        self.assertIn("What happened:", text)
        self.assertIn("First check:", text)
        self.assertIn("#task-instance-heartbeat-timeout", text)