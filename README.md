# Airflow Incident Triage Assistant

An Airflow incident triage CLI that connects task failures to log evidence, blocked downstream tasks, and relevant runbooks. It can also generate an English model summary for a selected failed task.

The demo uses synthetic data. The CLI reads from the local Airflow labs and supplies findings for an operator to review.

## Quick start

Run both commands from the repository root with Python 3.9 or newer.

```sh
python3 triage.py tests/fixtures/source-freshness.log --runbook --retrieve
python3 -m unittest discover -s tests
```

The first command reports `source_freshness`. The newest `raw_orders` record is **180 minutes old** against a **60-minute limit**. The retrieved runbook points to the source timestamp, ingestion watermark, and last successful load. The second command checks how the CLI handles truncated model responses and HTTP 429. Both commands run without Airflow, Docker, or an API key.

The [captured Airflow report](examples/source-freshness-report.json) shows the same incident in a full DAG run, including the blocked dashboard task and an English model summary.

## Try a live incident

Start the local Airflow lab with Docker Compose.

```sh
docker compose up --build
```

1. Open [Airflow](http://localhost:8088) and sign in as `admin` with the generated password shown in the Compose terminal.
2. Unpause and trigger `source_freshness_lab`. The freshness check should fail and `publish_orders_dashboard` should show `upstream_failed`.
3. Open the failed DAG **run** and copy its URL. The URL ends after the run ID.

In a second terminal, replace the placeholder with your run URL.

```sh
RUN_URL='<paste your Airflow DAG run URL>'
python3 triage.py --airflow-run "$RUN_URL" --runbook --retrieve
```

The report connects the blocked dashboard task to `check_orders_freshness`. The freshness DAG uses SQLite with its own synthetic rows.

### Add the model summary

If you already exported `OPENROUTER_API_KEY` in this terminal, skip the first four lines. In Bash or zsh, the prompt accepts the key without echoing it.

```sh
printf 'OpenRouter API key: '
read -rs OPENROUTER_API_KEY
printf '\n'
export OPENROUTER_API_KEY

python3 triage.py --airflow-run "$RUN_URL" --runbook --retrieve \
  --llm-summary check_orders_freshness
```

The request sends the selected task's evidence and retrieved runbook context to OpenRouter. Use synthetic or redacted logs for model calls. The lab uses a fixed free-tier model ID whose availability depends on OpenRouter. If a complete response is unavailable, `llm_summary` is `null` and the evidence and runbook remain in the report. Stop the lab with `docker compose down`.

## Additional lab scenarios

The example workload is [dbt Labs' Jaffle Shop for DuckDB](https://github.com/dbt-labs/jaffle_shop_duckdb/tree/20cc904370818ceab08cf19f8fd196e9e81def88), pinned at commit `20cc904370818ceab08cf19f8fd196e9e81def88`. Its models and seed data provide a repeatable workload for the incident experiments.

### Optional Jaffle Shop dbt DAG

With Docker Desktop running, open a terminal in this repository and run `docker compose up --build`. Compose uses `~/jaffle_shop_duckdb` by default. If you keep the checkout elsewhere, set `JAFFLE_SHOP_DIR` to its absolute path before starting the lab. Use `docker compose down` when you want to stop the lab.

Open [Airflow](http://localhost:8088). Sign in as `admin` with the generated password shown in the terminal on first startup. In the DAG list, open `jaffle_shop_build`, unpause it if necessary, and trigger a run. Open the `dbt_build` task log to see the dbt summary.

The Docker Compose lab runs Airflow standalone. The DAG copies the mounted Jaffle Shop models and seeds into a fresh working directory for each attempt and streams dbt output to the Airflow task log.

### Kubernetes provider

The Airflow image installs the Kubernetes provider using the constraints bundled with the base image. Rebuild the Airflow service and check that the operator is available:

```sh
docker compose up -d --build airflow
docker compose exec airflow python -c 'from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator; print(KubernetesPodOperator.__name__)'
```

With the Docker Desktop cluster ready, configure the running Airflow container:

```sh
python3 kubernetes/configure_airflow.py
```

The script creates the `airflow-triage-lab` namespace and grants a dedicated service account the permissions to manage the demo pods there. It requests a one-hour service account token, writes the kubeconfig to the Airflow volume with owner-only permissions, and checks API access from inside Airflow. Run the script again if the token expires. The demo workload pod does not receive the launcher token.

In Airflow, trigger the `kubernetes_pod_lab` DAG and open the log for `start_demo_pod`. Expected output: `Hello from the Airflow-launched Kubernetes pod`. The operator keeps the completed pod so its status and events can be inspected:

```sh
kubectl --context docker-desktop -n airflow-triage-lab get pods
```

Trigger `kubernetes_image_pull_lab` to compare with a missing-image failure. The pod cannot start because its image tag does not exist. The task records pod events on failure and keeps the pod for inspection. In the Airflow task log and Kubernetes events, look for the image pull error that precedes the task failure.

The `log_events_on_failure=True` option in `dags/kubernetes_pod_lab.py` writes pod events into the Airflow task log. The CLI returns the image-pull event as `airflow_pod_event`, with its log line number. The `pod` field includes the name and namespace when the log identifies a single scheduled pod. With `--kubernetes`, it also includes the pod UID when a matching live or saved record provides it. Run the CLI without `--kubernetes` to verify that the evidence remains available from Airflow alone after the pod is deleted.

To collect both sources, copy the failed task's Airflow URL:

```sh
python3 triage.py --airflow-task '<task Logs URL>' --kubernetes --runbook
```

The Kubernetes lookup is read-only and matches the DAG, run, task and attempt labels for an unmapped task in the Docker Desktop lab namespace. It queries events by pod UID. When the command finds a matching pod, it saves the observed evidence and capture time in `.airflow/kubernetes-evidence/`. A later run can return that copy under `saved_evidence` when the pod or its image pull event is unavailable. Run the command while the evidence still exists. This lab has no background collector.

### Generate incident logs

Trigger `etl_failure_lab` from the Airflow DAG list. Three tasks are designed to fail. The `publish_customer_report` task depends on `load_customers` and should have the state `upstream_failed`. Open each failed task's Logs tab to capture evidence. Keep a successful `jaffle_shop_build` run as a control.

| Task | Expected evidence | Failure category |
| --- | --- | --- |
| `load_customers` | SQLite rejects an update to the missing `loyalty_tier` column | Schema mismatch |
| `list_raw_events` | The simulated storage policy denies the list operation for `lab-reader` | Permission error |
| `rebuild_fact_sessions` | Airflow terminates the task after its three-second execution timeout | Timeout |

The scenarios are inspired by the example logs in [ETL Failure Investigator](https://huggingface.co/spaces/build-small-hackathon/etl-failure-investigator/tree/b109b2c607e1cf9f14f7e76decbe987605e96590/sample_logs). The upstream Space provides static logs. This DAG generates independent logs from local task runs using SQLite, a simulated storage policy, and an Airflow timeout.

### Diagnose a saved log

Open the failed task's **Logs** tab in Airflow and download the log for one attempt. Run this command from the repository root with Python 3.9 or newer.

```sh
python3 triage.py '/path/to/task-attempt.log'
```

The command reports a category, the likely cause, the evidence and its line number in the input file, and a next check. It reads plain logs or Airflow JSON lines. It reports insufficient evidence when no known signature matches or when different categories match the same input.

Add `--runbook` to include ordered troubleshooting steps and the source file. The command looks up the runbook by the diagnosed category. It skips the lookup when evidence is insufficient.

```sh
python3 triage.py '/path/to/task-attempt.log' --runbook
```

### Read a log directly from Airflow

Open a task instance's **Logs** tab, copy its browser URL, and pass it to `--airflow-task`. Enter the local Airflow admin password at the terminal prompt. The client authenticates with the lab at `localhost:8088` and reads only the requested task log.

The CLI caches the lab access token in `~/.cache/airflow-incident-triage/localhost-8088.token` with owner-only file permissions. It asks for the password on the first API call and when Airflow rejects the cached token as unauthorized. The password is never saved.

```sh
python3 triage.py --airflow-task '<task Logs URL>' --runbook
```

The default is attempt 1. Add `--attempt 2` for the second attempt. When reading from the API, the evidence line is the position in the fetched log. It can differ from the exported text file because the browser formats its download.

### Retrieve runbook sections from evidence

Use `--retrieve` to search the sections in `runbooks/sections.json` using the extracted exception and pod event text. A section must match a phrase that distinguishes it from the other sections. Results show the matched phrases, source and next checks. Combine `--runbook` and `--retrieve` to compare the category lookup with evidence-based retrieval.

```sh
python3 triage.py --airflow-task '<task Logs URL>' --runbook --retrieve
```

The `task-execution-timeout` section includes a summary and versioned link to the official Airflow Tasks / Timeouts documentation. The CLI reads this reference from the local corpus and returns it as `retrieved_sections[].reference`. To see it in the lab, open the logs for `rebuild_fact_sessions` in a failed `etl_failure_lab` run and use that task URL.

### Reproduce a task heartbeat timeout

The [Airflow heartbeat timeout recipe](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/tasks.html#reproducing-task-instance-heartbeat-timeouts-locally) changes scheduler settings for the entire instance. Run it in a separate Compose project on port 8090:

```sh
docker compose -p airflow-heartbeat-lab run --rm --build \
  --name airflow-heartbeat-lab -p 127.0.0.1:8090:8080 \
  -e AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_SEC=600 \
  -e AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT=2 \
  -e AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT_DETECTION_INTERVAL=5 \
  airflow
```

Open [the isolated Airflow instance](http://localhost:8090), sign in with the credentials from its terminal, and unpause and trigger only `airflow_heartbeat_timeout_lab`. The scheduler should mark the task as failed when the heartbeat expires. In a second terminal, capture the scheduler event and pass it to the triage CLI:

```sh
docker logs airflow-heartbeat-lab 2>&1 | rg 'Detected a task instance without a heartbeat' | rg 'airflow_heartbeat_timeout_lab' > /tmp/airflow-heartbeat-lab.log
python3 triage.py /tmp/airflow-heartbeat-lab.log --runbook --retrieve
```

Expect `category: heartbeat_timeout` and the official heartbeat reference. The scheduler event supplies the evidence if the task attempt log ends abruptly. Stop the isolated instance with Ctrl+C in its terminal.

### Diagnose all failures in a DAG run

Copy the URL of a completed DAG run from Airflow. The command lists task instances, selects those in the `failed` state, and reads their latest reported attempts. The report contains one entry for each failed instance, including mapped task instances. If a log cannot be read, its entry shows `log_unavailable`.

```sh
python3 triage.py --airflow-run '<DAG run URL>' --runbook
```

The report also lists `upstream_failed` tasks separately. Failed ancestor candidates are traced through blocked tasks using the current DAG definition. Check the historical graph when the DAG has changed since the run. Candidates are task IDs, so they do not resolve dependencies between individual mapped instances or between DAG runs.

An empty or inaccessible log is reported as `log_unavailable`. An available log with no supported signature returns `insufficient_evidence`. A timeout can establish that a deadline was exceeded while leaving the underlying cause unknown.

### Discover failed runs automatically

Run a one-time scan from the repo terminal. The default scans the last 24 hours on port 8088:

```sh
python3 triage.py --scan-failed --runbook --retrieve
```

For the isolated heartbeat lab:

```sh
python3 triage.py --scan-failed --airflow-url http://localhost:8090 --runbook --retrieve
```

The scan queries DAG runs across all DAGs and selects failed runs by their end time. It uses the run IDs returned by Airflow to inspect failed and blocked task instances. If a failed task log is unavailable or has no supported signature, the CLI looks up a heartbeat timeout event for the same DAG, run, task, attempt and map index. Reports include the original run link. Use `--hours` to change the lookback window.

The CLI stores separate tokens for ports 8088 and 8090. A first scan or expired token can prompt for that instance's password. Each invocation scans its time window once, so overlapping invocations can report the same runs.

### Watch for failed runs

Run the scan every minute in the foreground:

```sh
python3 triage.py --scan-failed --watch-seconds 60 --runbook --retrieve
```

Each scan prints one JSON line with the number of failed runs found and the number of new reports. Completed reports are stored in `logs/triage-watch.sqlite3` by default, so restarting the command with the same database preserves its history. If a run is cleared and fails again with a new end time, it can produce a new report. Runs whose evidence could not be fetched appear in `deferred_runs` and are retried while they remain in the lookback window.

Use `--airflow-url http://localhost:8090` to watch the isolated heartbeat lab or `--state-db` to choose another database. For unattended authentication, supply the local Airflow password through `AIRFLOW_TRIAGE_PASSWORD` in the process environment so the CLI can refresh its API token. Press Ctrl+C to stop the watcher.

## How it works

1. The CLI reads a saved log or queries the Airflow API for failed runs, task attempts, and task logs. For missing heartbeat evidence it can query scheduler events for the same attempt.
2. Deterministic rules extract evidence, preserve its line number or event ID, and identify supported categories. Failed and blocked tasks are reported separately.
3. Keyword retrieval ranks the local corpus. The CLI includes runbook steps and official references where available.
4. With `--llm-summary`, the selected task's evidence and retrieved text go to the model API. The report retains the original evidence and its sources.

## Evaluation notes

| Lab case | Observed result |
| --- | --- |
| Stale source data | The model preserved the 180-minute age and 60-minute limit, left the cause unknown, and suggested comparing the source watermark with the last successful load. |
| Execution timeout | The Airflow log confirmed a task runtime limit. The model recommended the same initial check in one comparison with retrieval on and off. |
| Heartbeat timeout | A scheduler event supplied evidence for a task log with insufficient detail. In one retrieval comparison, the first check became more specific but the model omitted the two-second deadline. |
| Missing SQL column | The model identified `loyalty_tier` and suggested comparing the failing query with the actual table schema. |

These observations come from synthetic lab runs and individual model calls. The automated tests cover preservation of triage findings when the model response is truncated or rate-limited. The rule set and local corpus cover a limited set of errors. Scheduler events and the current DAG definition can supply additional evidence, and operators can verify conclusions against the failed run before acting.
