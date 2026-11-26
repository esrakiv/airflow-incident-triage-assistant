"""Read pod evidence for an Airflow task attempt in the local Kubernetes lab."""

import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile


NAMESPACE = "airflow-triage-lab"
CONTEXT = "docker-desktop"
KUBECTL = ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, "--request-timeout=8s"]
SNAPSHOT_DIR = Path(__file__).resolve().parent / ".airflow" / "kubernetes-evidence"


def airflow_label(value: str) -> str:
    # Match the label format emitted by Airflow's KubernetesPodOperator.
    label = re.sub(r"[^a-zA-Z0-9_.-]", "", value).strip("_.-")
    if label == value and len(label) <= 63:
        return label
    suffix = hashlib.md5(value.encode(), usedforsecurity=False).hexdigest()[:9]
    return f"{label[:53]}-{suffix}"


def kubectl_list(resource: str, selector: str, field: bool = False) -> list[dict]:
    flag = "--field-selector" if field else "--selector"
    completed = subprocess.run(
        [*KUBECTL, "get", resource, flag, selector, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=12,
    )
    if completed.returncode:
        raise ValueError(completed.stderr.strip() or f"Could not list {resource}.")
    return json.loads(completed.stdout)["items"]


def read_pod_evidence(dag_id: str, run_id: str, task_id: str, attempt: int) -> dict:
    selector = ",".join((
        "kubernetes_pod_operator=True",
        f"dag_id={airflow_label(dag_id)}",
        f"run_id={airflow_label(run_id)}",
        f"task_id={airflow_label(task_id)}",
        f"try_number={attempt}",
        "!map_index",
    ))
    pods = kubectl_list("pods", selector)
    if len(pods) != 1:
        return {
            "status": "pod_not_found" if not pods else "ambiguous_pod",
            "matching_pod_count": len(pods),
        }

    pod = pods[0]
    metadata = pod["metadata"]
    status = pod["status"]
    waiting = [
        container["state"]["waiting"]["reason"]
        for container in status.get("containerStatuses", [])
        if container.get("state", {}).get("waiting", {}).get("reason")
    ]
    evidence = {
        "status": "pod_found",
        "pod": metadata["name"],
        "pod_uid": metadata["uid"],
        "namespace": NAMESPACE,
        "phase": status.get("phase"),
        "waiting_reasons": waiting,
    }
    try:
        events = kubectl_list("events", f"involvedObject.uid={metadata['uid']}", field=True)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        return {**evidence, "events_unavailable": str(error)}
    for event in events:
        message = event.get("message", "")
        if event.get("reason") == "Failed" and message.startswith("Failed to pull image"):
            evidence["image_pull_event"] = {"reason": event["reason"], "message": message}
            if re.search(r"\b(?:not found|manifest unknown)\b", message, re.I):
                evidence["next_check"] = "Verify that the configured image name and tag exist in the registry."
                break
    return evidence


def save_snapshot(path: Path, snapshot: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with NamedTemporaryFile(dir=path.parent, mode="w", encoding="utf-8", delete=False) as file:
        temporary_path = Path(file.name)
        try:
            json.dump(snapshot, file, indent=2)
            file.write("\n")
            file.close()
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)


def collect_pod_evidence(dag_id: str, run_id: str, task_id: str, attempt: int) -> dict:
    identity = {
        "context": CONTEXT,
        "namespace": NAMESPACE,
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "attempt": attempt,
    }
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    path = SNAPSHOT_DIR / f"{key}.json"
    snapshot_error = None
    try:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        if snapshot["identity"] != identity:
            raise ValueError("Stored evidence belongs to a different task attempt.")
        if not isinstance(snapshot["evidence"], dict) or not isinstance(snapshot["captured_at"], str):
            raise ValueError("Stored evidence has an invalid format.")
    except FileNotFoundError:
        snapshot = None
    except (OSError, ValueError, KeyError, TypeError) as error:
        snapshot = None
        snapshot_error = str(error)

    try:
        live = read_pod_evidence(dag_id, run_id, task_id, attempt)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        live = {"status": "unavailable", "reason": str(error)}

    if live["status"] == "pod_found" and (
        snapshot is None or live.get("image_pull_event")
    ):
        captured_at = datetime.now(timezone.utc).isoformat()
        try:
            save_snapshot(path, {"identity": identity, "captured_at": captured_at, "evidence": live})
        except OSError as error:
            return {**live, "snapshot_error": str(error)}
        return {**live, "source": "live", "captured_at": captured_at, "snapshot_path": str(path)}

    if snapshot is not None:
        return {
            **live,
            "saved_evidence": {
                "captured_at": snapshot["captured_at"],
                "snapshot_path": str(path),
                "evidence": snapshot["evidence"],
            },
        }
    if snapshot_error:
        live["snapshot_error"] = snapshot_error
    return live
