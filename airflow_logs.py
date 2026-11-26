"""Read runs, task logs and events from Airflow's public API."""

from __future__ import annotations

import json
import os
import re
from getpass import getpass
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


LAB_URL = "http://localhost:8088"
LAB_URLS = (LAB_URL, "http://localhost:8090")
TASK_PATH = re.compile(r"/dags/([^/]+)/runs/([^/]+)/tasks/([^/]+)/?")
RUN_PATH = re.compile(r"/dags/([^/]+)/runs/([^/]+)/?")
TOKEN_CACHE_DIR = Path.home() / ".cache" / "airflow-incident-triage"


def configured_airflow_url() -> str | None:
    value = os.environ.get("AIRFLOW_TRIAGE_URL", "").strip().rstrip("/")
    if not value:
        return None
    parsed = urlsplit(value)
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if (
        not parsed.hostname
        or (parsed.scheme != "https" and not (parsed.scheme == "http" and loopback))
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or any(segment in {".", ".."} for segment in unquote(parsed.path).split("/"))
    ):
        raise ValueError("AIRFLOW_TRIAGE_URL must be an HTTPS base URL or an HTTP loopback URL, without credentials or query parameters.")
    return value


def airflow_base(url: str) -> str:
    parsed = urlsplit(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    configured = configured_airflow_url()
    if configured:
        base = urlsplit(configured)
        if (parsed.scheme, parsed.netloc) == (base.scheme, base.netloc) and (
            parsed.path == base.path or parsed.path.startswith(f"{base.path}/")
        ):
            return configured
    if origin in LAB_URLS:
        return origin
    raise ValueError("Use a local lab URL or set AIRFLOW_TRIAGE_URL to the base URL of your Airflow instance.")


def airflow_path(url: str) -> str:
    base_path = urlsplit(airflow_base(url)).path
    return urlsplit(url).path[len(base_path):]


class RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def api_json(request: Request) -> dict:
    opener = build_opener(ProxyHandler({}), RejectRedirects())
    with opener.open(request, timeout=10) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response from the Airflow API.")
    return payload


def login(origin: str) -> str:
    password = os.environ.get("AIRFLOW_TRIAGE_PASSWORD")
    if not password:
        password = getpass(f"Airflow password for {origin}: ")
    auth_request = Request(
        f"{origin}/auth/token",
        data=json.dumps({"username": "admin", "password": password}).encode(),
        headers={"Content-Type": "application/json"},
    )
    token = api_json(auth_request).get("access_token")
    if not isinstance(token, str) or not token:
        raise ValueError("Airflow did not return an access token.")
    return token


def save_token(token: str, cache_path: Path) -> None:
    cache_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # NamedTemporaryFile starts with owner-only permissions. Replace atomically.
    with NamedTemporaryFile(dir=cache_path.parent, mode="w", encoding="utf-8", delete=False) as file:
        temporary_path = Path(file.name)
        try:
            file.write(token)
            file.close()
            temporary_path.replace(cache_path)
        finally:
            temporary_path.unlink(missing_ok=True)


def get_with_token(url: str, token: str) -> dict:
    return api_json(Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    ))


def authenticated_get(url: str) -> dict:
    origin = airflow_base(url)
    if origin == configured_airflow_url():
        token = os.environ.get("AIRFLOW_TRIAGE_TOKEN", "").strip()
        if not token:
            raise ValueError("Set AIRFLOW_TRIAGE_TOKEN to an API token for AIRFLOW_TRIAGE_URL.")
        return get_with_token(url, token)

    cache_path = TOKEN_CACHE_DIR / f"localhost-{urlsplit(origin).port}.token"
    try:
        token = cache_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        token = ""
    if token:
        try:
            return get_with_token(url, token)
        except HTTPError as error:
            if error.code != 401:
                raise
            error.close()

    token = login(origin)
    response = get_with_token(url, token)
    save_token(token, cache_path)
    return response


def fetch_log_content(
    tasks_url: str, task_id: str, attempt: int, map_index: int = -1
) -> str:
    query = urlencode({"full_content": "true", "map_index": map_index})
    log_url = f"{tasks_url}/{quote(task_id, safe='')}/logs/{attempt}?{query}"
    content = authenticated_get(log_url).get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list) and all(isinstance(line, (str, dict)) for line in content):
        return "\n".join(line if isinstance(line, str) else json.dumps(line) for line in content)
    raise ValueError("Airflow returned an unsupported task log format.")


def parse_task_url(task_url: str) -> tuple[str, str, str]:
    task = TASK_PATH.fullmatch(airflow_path(task_url))
    if task is None:
        raise ValueError("Use an Airflow task Logs URL that includes its DAG run.")
    dag, run, task_id = map(unquote, task.groups())
    return dag, run, task_id


def download_task_log(task_url: str, attempt: int) -> str:
    dag, run, task_id = parse_task_url(task_url)
    if attempt < 1:
        raise ValueError("The attempt number must be at least 1.")
    return fetch_log_content(task_instances_url(dag, run, airflow_base(task_url)), task_id, attempt)


def task_instances_url(dag_id: str, run_id: str, origin: str = LAB_URL) -> str:
    dag, run = (quote(segment, safe="") for segment in (dag_id, run_id))
    return f"{origin}/api/v2/dags/{dag}/dagRuns/{run}/taskInstances"


def parse_run_url(run_url: str) -> tuple[str, str]:
    run = RUN_PATH.fullmatch(airflow_path(run_url))
    if run is None:
        raise ValueError("Use an Airflow DAG run URL ending after the run ID.")
    dag_id, run_id = map(unquote, run.groups())
    return dag_id, run_id


def list_records(url: str, field: str, **filters: str | tuple[str, ...]) -> list[dict]:
    records = []
    offset = 0
    while True:
        query = urlencode({**filters, "limit": 100, "offset": offset}, doseq=True)
        page = authenticated_get(f"{url}?{query}")
        entries = page.get(field)
        total = page.get("total_entries")
        if not isinstance(entries, list) or not isinstance(total, int):
            raise ValueError(f"Airflow returned an unexpected {field} list.")
        records.extend(entries)
        offset += len(entries)
        if not entries or offset >= total:
            return records


def list_task_instances(tasks_url: str) -> list[dict]:
    return list_records(tasks_url, "task_instances")


def list_failed_runs(origin: str, since: str, until: str) -> list[dict]:
    return list_records(
        f"{origin}/api/v2/dags/~/dagRuns",
        "dag_runs",
        state="failed",
        end_date_gte=since,
        end_date_lt=until,
        order_by=("end_date", "id"),
    )


def find_heartbeat_event(
    origin: str, dag_id: str, run_id: str, task_id: str, attempt: int, map_index: int
) -> dict | None:
    query = urlencode({
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "try_number": attempt,
        "map_index": map_index,
        "event": "heartbeat timeout",
        "order_by": "-event_log_id",
        "limit": 1,
    })
    events = authenticated_get(f"{origin}/api/v2/eventLogs?{query}").get("event_logs")
    if not isinstance(events, list):
        raise ValueError("Airflow returned an unexpected event log list.")
    return events[0] if events else None


def list_task_dependencies(dag_id: str, origin: str = LAB_URL) -> dict[str, list[str]]:
    url = f"{origin}/api/v2/dags/{quote(dag_id, safe='')}/tasks"
    definitions = authenticated_get(url).get("tasks")
    if not isinstance(definitions, list):
        raise ValueError("Airflow did not return DAG task definitions.")
    parents: dict[str, list[str]] = {}
    for task in definitions:
        task_id = task["task_id"]
        parents.setdefault(task_id, [])
        for child_id in task["downstream_task_ids"]:
            parents.setdefault(child_id, []).append(task_id)
    return parents
