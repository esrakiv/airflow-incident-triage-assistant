"""Explain a supported failure in one Airflow task log."""

import argparse
import json
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from airflow_logs import (
    LAB_URL,
    LAB_URLS,
    airflow_base,
    configured_airflow_url,
    download_task_log,
    fetch_log_content,
    find_heartbeat_event,
    list_failed_runs,
    list_task_dependencies,
    list_task_instances,
    parse_run_url,
    parse_task_url,
    task_instances_url,
)
from kubernetes_evidence import collect_pod_evidence
from runbook_retrieval import retrieve_sections
from llm_summary import generate_summary

@dataclass(frozen=True)
class Rule:
    pattern: re.Pattern[str]
    category: str
    cause: str
    next_check: str


RULES = (
    Rule(
        re.compile(r"^(?:[\w.]+\.)?DataFreshnessError:\s*\S"),
        "source_freshness",
        "The latest source record is older than the configured freshness limit.",
        "Check the latest source timestamp and the last successful upstream ingestion.",
    ),
    Rule(
        re.compile(r"^(?:[\w.]+\.)?PodLaunchFailedException:\s*Image cannot be pulled\b"),
        "image_pull_failed",
        "Kubernetes could not pull the image before starting the container.",
        "Check the pod events for a missing image, registry authorization, or connectivity error.",
    ),
    Rule(
        re.compile(
            r"^(?:(?:sqlite3\.)?OperationalError:\s*)?no such column:\s*\S"
            r"|^(?:psycopg2\.errors\.)?UndefinedColumn:\s*column\b",
            re.IGNORECASE,
        ),
        "schema_mismatch",
        "The SQL references a column missing from the target schema.",
        "Inspect the target table columns and compare them with the failing SQL.",
    ),
    Rule(
        re.compile(
            r"^PermissionError:\s*(?:AccessDenied|Permission denied)"
            r"|^(?:botocore\.exceptions\.)?ClientError:\s*.*\bAccessDenied\b",
            re.IGNORECASE,
        ),
        "permission_error",
        "The task identity was denied access to the requested operation.",
        "Check the task identity and which permission the attempted operation requires.",
    ),
    Rule(
        re.compile(r"^(?:airflow\.exceptions\.)?AirflowTaskTimeout:\s*\S"),
        "timeout",
        "The task exceeded its Airflow execution timeout.",
        "Compare task duration with its execution timeout and inspect where it stalled.",
    ),
    Rule(
        re.compile(
            r"\bDetected a task instance without a heartbeat:"
            r"|\bTask did not emit heartbeat within time limit\b"
            r"|^heartbeat timeout$",
            re.IGNORECASE,
        ),
        "heartbeat_timeout",
        "The Airflow scheduler detected a task attempt that stopped sending heartbeats.",
        "Check the worker and scheduler logs at the failure timestamp for restarts or resource exhaustion.",
    ),
)

LOG_PREFIX = re.compile(r"^\[\d{4}-\d{2}-\d{2}[^]]*\].*? - ")
FAILURE_PREFIX = re.compile(r"^Task failed with exception\s*:?\s*")
POD_SCHEDULED = re.compile(
    r"^Pod Event: Scheduled - Successfully assigned ([a-z0-9-]+)/([a-z0-9.-]+) to \S+"
)
RUNBOOK_DIR = Path(__file__).resolve().parent / "runbooks"
NO_SUPPORTED_EXCEPTION = "No supported exception found. Check that the full task attempt log was saved."


def evidence_lines(raw_line: str) -> list[str]:
    """Extract candidate text from a plain log line or a structured Airflow record."""
    if not raw_line.lstrip().startswith("{"):
        message = LOG_PREFIX.sub("", raw_line, count=1).strip()
        # Airflow's text download can join the event and exception with no separator.
        return [FAILURE_PREFIX.sub("", message, count=1)]

    try:
        record = json.loads(raw_line)
    except json.JSONDecodeError:
        return []
    if not isinstance(record, dict):
        return []

    candidates = []
    event = record.get("event")
    if isinstance(event, str):
        candidates.extend(event.splitlines())
    for field in ("error_detail", "exception", "exc_info"):
        exceptions = record.get(field)
        if isinstance(exceptions, str):
            candidates.extend(exceptions.splitlines())
        elif isinstance(exceptions, list):
            for error in exceptions:
                if isinstance(error, dict):
                    name, message = error.get("exc_type"), error.get("exc_value")
                    if isinstance(name, str) and isinstance(message, str):
                        candidates.append(f"{name}: {message}")
    return [FAILURE_PREFIX.sub("", candidate.strip(), count=1) for candidate in candidates]


def diagnose(log: str) -> dict:
    matches = {}
    for line_number, raw_line in enumerate(log.splitlines(), start=1):
        for evidence in evidence_lines(raw_line):
            for rule in RULES:
                if rule.category not in matches and rule.pattern.search(evidence):
                    matches[rule.category] = (rule, line_number, evidence)

    if len(matches) != 1:
        return {
            "status": "insufficient_evidence",
            "reason": (
                "Multiple failure categories matched. Review the task attempt and exception chain."
                if matches
                else NO_SUPPORTED_EXCEPTION
            ),
        }

    rule, line_number, evidence = next(iter(matches.values()))
    return {
        "status": "diagnosed",
        "category": rule.category,
        "likely_cause": rule.cause,
        "evidence": {"line": line_number, "text": evidence},
        "next_check": rule.next_check,
    }


def diagnose_with_runbook(log: str, include_runbook: bool, retrieve: bool = False) -> dict:
    result = diagnose(log)
    # KubernetesPodOperator persists pod events in the Airflow task log on failure.
    pull_event_prefix = "Pod Event: Failed - Failed to pull image"
    scheduled_pods = {}
    for line_number, raw_line in enumerate(log.splitlines(), start=1):
        for event in evidence_lines(raw_line):
            if event.startswith(pull_event_prefix) and "airflow_pod_event" not in result:
                result["airflow_pod_event"] = {"line": line_number, "text": event}
            scheduled = POD_SCHEDULED.match(event)
            if scheduled:
                scheduled_pods.setdefault(scheduled.groups(), line_number)
    if len(scheduled_pods) == 1:
        (namespace, name), line_number = next(iter(scheduled_pods.items()))
        result["pod"] = {
            "name": name,
            "namespace": namespace,
            "source": "airflow_task_log",
            "line": line_number,
        }
    if result.get("category") == "timeout":
        result["cause_limit"] = "The log confirms the timeout. It does not establish why the task stalled."
    if result.get("category") == "heartbeat_timeout":
        result["cause_limit"] = "The scheduler detected a missed heartbeat. The evidence does not identify why it stopped."
    if result.get("category") == "source_freshness":
        result["cause_limit"] = "The check confirms stale data. The evidence does not explain why newer records are missing."
    if include_runbook and result["status"] == "diagnosed":
        runbook_path = RUNBOOK_DIR / f"{result['category']}.json"
        try:
            runbook = json.loads(runbook_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            raise ValueError(f"Could not load the runbook: {error}") from error
        result["runbook"] = {
            "source": f"runbooks/{runbook_path.name}",
            "title": runbook["title"],
            "steps": runbook["steps"],
        }
    if retrieve:
        query = [
            result[key]["text"]
            for key in ("evidence", "airflow_pod_event")
            if key in result
        ]
        result["retrieved_sections"] = retrieve_sections(query)
    return result


def add_kubernetes_evidence(result: dict, evidence: dict) -> None:
    result["kubernetes"] = evidence
    pod_metadata = evidence
    if evidence.get("status") == "pod_not_found":
        pod_metadata = evidence.get("saved_evidence", {}).get("evidence", {})
    if pod_metadata.get("status") != "pod_found":
        return

    name, namespace = pod_metadata["pod"], pod_metadata["namespace"]
    pod = result.setdefault("pod", {
        "name": name,
        "namespace": namespace,
        "source": "kubernetes" if pod_metadata is evidence else "saved_kubernetes_evidence",
    })
    if (pod["name"], pod["namespace"]) == (name, namespace) and pod_metadata.get("pod_uid"):
        pod["uid"] = pod_metadata["pod_uid"]


def upstream_failure_candidates(
    task_id: str, parents: dict[str, list[str]], failed: set[str], blocked: set[str]
) -> list[str]:
    pending = list(parents.get(task_id, ()))
    visited = {task_id}
    candidates = set()
    while pending:
        parent = pending.pop()
        if parent in visited:
            continue
        visited.add(parent)
        if parent in failed:
            candidates.add(parent)
        if parent in blocked:
            pending.extend(parents.get(parent, ()))
    return sorted(candidates)


def diagnose_run(run_url: str, include_runbook: bool, retrieve: bool = False) -> dict:
    dag_id, run_id = parse_run_url(run_url)
    return diagnose_run_by_id(airflow_base(run_url), dag_id, run_id, include_runbook, retrieve)


def diagnose_run_by_id(
    origin: str, dag_id: str, run_id: str, include_runbook: bool, retrieve: bool
) -> dict:
    tasks_url = task_instances_url(dag_id, run_id, origin)
    results = []
    instances = list_task_instances(tasks_url)
    failed = [task for task in instances if task.get("state") == "failed"]
    blocked = [task for task in instances if task.get("state") == "upstream_failed"]
    for task in failed:
        task_id = task["task_id"]
        attempt = task.get("try_number")
        map_index = task.get("map_index", -1)
        try:
            if not isinstance(attempt, int) or attempt < 1:
                raise ValueError("Airflow did not report a completed attempt.")
            log = fetch_log_content(tasks_url, task_id, attempt, map_index)
            if not log.strip():
                raise ValueError("Airflow returned an empty task log.")
        except (OSError, ValueError) as error:
            result = {"status": "log_unavailable", "reason": str(error)}
        else:
            result = diagnose_with_runbook(log, include_runbook, retrieve)
        if isinstance(attempt, int) and attempt > 0 and (
            result["status"] == "log_unavailable" or result.get("reason") == NO_SUPPORTED_EXCEPTION
        ):
            try:
                event = find_heartbeat_event(origin, dag_id, run_id, task_id, attempt, map_index)
            except (OSError, ValueError) as error:
                result["heartbeat_lookup"] = {"status": "unavailable", "reason": str(error)}
            else:
                if event:
                    task_log_status = result["status"]
                    result = diagnose_with_runbook(event["event"], include_runbook, retrieve)
                    result["task_log_status"] = task_log_status
                    result["evidence"] = {
                        "source": "airflow_event_log",
                        "event_log_id": event["event_log_id"],
                        "when": event["when"],
                        "text": event.get("extra") or event["event"],
                    }
        task_report = {
            "task_id": task_id,
            "attempt": attempt,
            "map_index": map_index,
            **result,
        }

        if result["status"] == "diagnosed":
            task_report["llm_input"] = {
                "task": task_id,
                "diagnosis": result["category"],
                "evidence": result["evidence"]["text"],
                "next_check": result["next_check"],
                "finding": result["likely_cause"],
                "cause_limit": result.get("cause_limit"),
                "retrieved_context": [
                {
                    "title": section["title"],
                    "summary": section.get("reference", {}).get("summary"),
                    "steps": section["steps"],
                }
                for section in result.get("retrieved_sections", [])[:1]
            ],
            }

        results.append(task_report)
    blocked_reports = []
    dependencies = {}
    dependency_source = "not_needed"
    if blocked:
        try:
            dependencies = list_task_dependencies(dag_id, origin)
        except (OSError, ValueError, KeyError, TypeError):
            dependency_source = "unavailable"
        else:
            dependency_source = "current_dag_definition"
    failed_ids = {task["task_id"] for task in failed}
    blocked_ids = {task["task_id"] for task in blocked}
    for task in blocked:
        task_id = task["task_id"]
        blocked_reports.append({
            "task_id": task_id,
            "map_index": task.get("map_index", -1),
            "state": "upstream_failed",
            "candidate_failed_upstream_task_ids": upstream_failure_candidates(
                task_id, dependencies, failed_ids, blocked_ids
            ),
        })
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "failed_task_count": len(results),
        "tasks": results,
        "blocked_task_count": len(blocked_reports),
        "blocked_tasks": blocked_reports,
        "dependency_source": dependency_source,
    }


def scan_failed_runs(
    origin: str, hours: int, include_runbook: bool, retrieve: bool, history: Optional[sqlite3.Connection] = None
) -> dict:
    until = datetime.now(timezone.utc)
    since = until - timedelta(hours=hours)
    results = []
    runs = list_failed_runs(origin, since.isoformat(), until.isoformat())
    deferred = []
    for run in runs:
        dag_id, run_id = run["dag_id"], run["dag_run_id"]
        ended_at = run["end_date"]
        key = (origin, dag_id, run_id, ended_at)
        if history is not None and history.execute(
            "SELECT 1 FROM reports WHERE origin = ? AND dag_id = ? AND run_id = ? AND ended_at = ?",
            key,
        ).fetchone():
            continue

        run_url = f"{origin}/dags/{quote(dag_id, safe='')}/runs/{quote(run_id, safe='')}"
        try:
            report = diagnose_run_by_id(origin, dag_id, run_id, include_runbook, retrieve)
        except (OSError, ValueError) as error:
            report = {"dag_id": dag_id, "run_id": run_id, "status": "unavailable", "reason": str(error)}
        report = {"run_url": run_url, "ended_at": ended_at, **report}
        if history is not None:
            if report.get("status") == "unavailable" or report.get("dependency_source") == "unavailable" or any(
                task["status"] == "log_unavailable" or "heartbeat_lookup" in task
                for task in report.get("tasks", [])
            ):
                deferred.append(report)
                continue
            with history:
                saved = history.execute(
                    """INSERT INTO reports (origin, dag_id, run_id, ended_at, report)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT (origin, dag_id, run_id, ended_at) DO NOTHING""",
                    (*key, json.dumps(report, ensure_ascii=False)),
                )
            if not saved.rowcount:
                continue
        results.append(report)

    output = {
        "airflow_url": origin,
        "window_start": since.isoformat(),
        "window_end": until.isoformat(),
        "failed_run_count": len(runs),
        "runs": results,
    }
    if history is not None:
        output.update(new_run_count=len(results), deferred_runs=deferred)
    return output


def watch_failed_runs(
    origin: str, hours: int, include_runbook: bool, retrieve: bool, interval: int, history_path: Path
) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    # Reports can contain task log excerpts. Give a new database owner-only permissions.
    history_path.touch(mode=0o600, exist_ok=True)
    with closing(sqlite3.connect(history_path)) as history:
        history.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                origin TEXT NOT NULL,
                dag_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                report TEXT NOT NULL,
                PRIMARY KEY (origin, dag_id, run_id, ended_at)
            )
        """)
        while True:
            try:
                report = scan_failed_runs(origin, hours, include_runbook, retrieve, history)
            except OSError as error:
                print(f"Airflow scan failed; retrying in {interval}s: {error}", file=sys.stderr)
            else:
                print(json.dumps(report, ensure_ascii=False), flush=True)
            time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("log_file", type=Path, nargs="?", help="Path to one task attempt log")
    source.add_argument("--airflow-task", metavar="URL", help="Airflow task Logs URL")
    source.add_argument("--airflow-run", metavar="URL", help="Airflow DAG run URL")
    source.add_argument("--scan-failed", action="store_true", help="Find and diagnose recently failed Airflow runs")
    parser.add_argument("--airflow-url", help=f"Base URL for --scan-failed (default: AIRFLOW_TRIAGE_URL or {LAB_URL})")
    parser.add_argument("--hours", type=int, help="Lookback for --scan-failed (default: 24)")
    parser.add_argument("--watch-seconds", type=int, help="Repeat --scan-failed after this many seconds")
    parser.add_argument("--state-db", type=Path, help="Watch report database (default: logs/triage-watch.sqlite3)")
    parser.add_argument("--attempt", type=int, help="Attempt for --airflow-task (default: 1)")
    parser.add_argument("--runbook", action="store_true", help="Include the matching runbook")
    parser.add_argument("--retrieve", action="store_true", help="Rank runbook sections using log evidence")
    parser.add_argument("--kubernetes", action="store_true", help="Read pod evidence for one Airflow task")
    parser.add_argument("--llm-summary",metavar="TASK_ID",help="Add a model-generated summary for one failed task",)
    args = parser.parse_args()
    if args.llm_summary and not args.airflow_run:
        parser.error("--llm-summary requires --airflow-run")
    if args.attempt is not None and not args.airflow_task:
        parser.error("--attempt requires --airflow-task")
    if args.kubernetes and not args.airflow_task:
        parser.error("--kubernetes requires --airflow-task")
    if (args.airflow_url or args.hours is not None or args.watch_seconds is not None) and not args.scan_failed:
        parser.error("--airflow-url, --hours and --watch-seconds require --scan-failed")
    if args.state_db is not None and args.watch_seconds is None:
        parser.error("--state-db requires --watch-seconds")
    if args.hours is not None and args.hours < 1:
        parser.error("--hours must be at least 1")
    if args.watch_seconds is not None and args.watch_seconds < 1:
        parser.error("--watch-seconds must be at least 1")
    try:
        if args.scan_failed:
            selected_url = (args.airflow_url or configured_airflow_url() or LAB_URL).rstrip("/")
            origin = airflow_base(selected_url)
            if origin != selected_url:
                raise ValueError("--airflow-url requires an Airflow base URL rather than a run or task URL.")
            if args.watch_seconds is not None:
                watch_failed_runs(
                    origin, args.hours or 24, args.runbook, args.retrieve,
                    args.watch_seconds, args.state_db or Path("logs/triage-watch.sqlite3"),
                )
                return
            result = scan_failed_runs(origin, args.hours or 24, args.runbook, args.retrieve)
        elif args.airflow_run:
            result = diagnose_run(args.airflow_run, args.runbook, args.retrieve)

            if args.llm_summary:
                matches = [
                    task for task in result["tasks"]
                    if task["task_id"] == args.llm_summary
                ]
                if len(matches) != 1 or "llm_input" not in matches[0]:
                    raise ValueError(
                        f"Expected one diagnosed task for {args.llm_summary}."
                    )

                task = matches[0]
                task["llm_summary"] = generate_summary(task["llm_input"])
        else:
            if args.airflow_task:
                attempt = 1 if args.attempt is None else args.attempt
                if args.kubernetes:
                    if airflow_base(args.airflow_task) not in LAB_URLS:
                        raise ValueError("--kubernetes is only supported by the local Docker Desktop lab.")
                    dag_id, run_id, task_id = parse_task_url(args.airflow_task)
                    if attempt < 1:
                        raise ValueError("The attempt number must be at least 1.")
                    try:
                        log = download_task_log(args.airflow_task, attempt)
                    except (EOFError, OSError, ValueError) as error:
                        result = {"status": "log_unavailable", "reason": str(error) or "Airflow login required."}
                    else:
                        result = diagnose_with_runbook(log, args.runbook, args.retrieve)
                    try:
                        add_kubernetes_evidence(result, collect_pod_evidence(dag_id, run_id, task_id, attempt))
                    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                        result["kubernetes"] = {"status": "unavailable", "reason": str(error)}
                else:
                    log = download_task_log(args.airflow_task, attempt)
                    result = diagnose_with_runbook(log, args.runbook, args.retrieve)
            else:
                log = args.log_file.read_text(encoding="utf-8", errors="replace")
                result = diagnose_with_runbook(log, args.runbook, args.retrieve)
    except KeyboardInterrupt:
        if args.watch_seconds is not None:
            return
        raise
    except (EOFError, OSError, ValueError, OverflowError, sqlite3.Error) as error:
        parser.exit(2, f"Could not complete triage: {str(error) or 'Airflow password input is unavailable.'}\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
