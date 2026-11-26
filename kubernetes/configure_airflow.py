"""Set up the Docker Desktop lab identity in the running Airflow container."""

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
KUBECTL = ["kubectl", "--context", "docker-desktop", "--request-timeout=10s"]
AIRFLOW_EXEC = ["docker", "compose", "exec", "-T", "airflow"]
NAMESPACE = "airflow-triage-lab"
ACCOUNT = "airflow-pod-launcher"


def main() -> None:
    subprocess.run([*AIRFLOW_EXEC, "true"], cwd=ROOT, check=True)
    subprocess.run([*KUBECTL, "apply", "-f", str(ROOT / "kubernetes/rbac.yaml")], check=True)
    ca = subprocess.check_output(
        [*KUBECTL, "config", "view", "--minify", "--flatten", "--raw",
         "-o", "jsonpath={.clusters[0].cluster.certificate-authority-data}"],
        text=True,
    ).strip()
    if not ca:
        raise SystemExit("Docker Desktop kubeconfig does not contain a cluster CA.")
    token = subprocess.check_output(
        [*KUBECTL, "-n", NAMESPACE, "create", "token", ACCOUNT, "--duration=1h"],
        text=True,
    ).strip()
    config = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{
            "name": "local-lab",
            "cluster": {
                "server": "https://kubernetes.docker.internal:6443",
                "certificate-authority-data": ca,
            },
        }],
        "users": [{"name": ACCOUNT, "user": {"token": token}}],
        "contexts": [{
            "name": "local-lab",
            "context": {"cluster": "local-lab", "user": ACCOUNT, "namespace": NAMESPACE},
        }],
        "current-context": "local-lab",
    }
    subprocess.run(
        [*AIRFLOW_EXEC, "sh", "-c",
         'set -eu; umask 077; tmp=$(mktemp /opt/airflow/.triage-kubeconfig.XXXXXX); '
         'trap \'rm -f "$tmp"\' EXIT; cat > "$tmp"; '
         'mv "$tmp" /opt/airflow/kubeconfig-triage.json'],
        cwd=ROOT,
        input=json.dumps(config),
        text=True,
        check=True,
    )
    subprocess.run(
        [*AIRFLOW_EXEC, "python", "-c",
         "from kubernetes import client, config; "
         "config.load_kube_config('/opt/airflow/kubeconfig-triage.json'); "
         "client.CoreV1Api().list_namespaced_pod('airflow-triage-lab', limit=1, _request_timeout=8)"],
        cwd=ROOT,
        check=True,
    )
    print("Verified that the Airflow container can reach the Kubernetes lab.")


if __name__ == "__main__":
    main()
