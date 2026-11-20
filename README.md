# Airflow Incident Triage Assistant

**Status:** Project setup. The first milestone is to capture one healthy and one failed run of the same dbt pipeline in Airflow.

An AI-assisted tool for investigating failed Airflow tasks. It analyzes a task log, identifies the most relevant error evidence, retrieves matching operational guidance, and returns a concise, evidence-grounded diagnosis with safe next steps.

This project is a portfolio application built with synthetic or fully redacted examples. It does not connect to a production Airflow environment or execute remediation actions.

## What the project runs on

The example workload is [dbt Labs' Jaffle Shop for DuckDB](https://github.com/dbt-labs/jaffle_shop_duckdb/tree/20cc904370818ceab08cf19f8fd196e9e81def88), pinned at commit `20cc904370818ceab08cf19f8fd196e9e81def88`. Its models and seed data provide a repeatable workload for the incident experiments.

### First milestone

1. In the existing Jaffle Shop VS Code Dev Container, run `dbt build --profiles-dir .` from the project root. This loads the example seeds, runs the models, and executes their tests.
2. Have Airflow run the same command using an isolated working copy. Record the task log and dbt's `target/run_results.json` from the healthy run.
3. Change one order status to `unknown_status` in a copy of the seed data. Repeat the run in isolation and capture the task log and artifact before another run can overwrite it. The expected failure is a dbt `accepted_values` test. Record the failing test name from the artifact and the matching evidence in the log.

The project creates its own local Airflow learning environment with Docker Compose. A small set of intentionally failing example DAGs produces real Airflow task logs for repeatable scenarios. This means no employer Airflow access, old incident logs, or private data are required. The local Airflow quick-start is for learning and exploration, not production operation.

The assistant will first support local fixture files and then read task logs from the local Airflow REST API using read-only requests. It will not need credentials for any employer system. Example logs and runbooks will be synthetic, labeled, and safe to publish.

## System shape

```text
Local Airflow example DAGs -> Airflow task logs -> read-only log ingestion
                                                -> deterministic parsing
Runbooks -> indexing/retrieval ----------------> diagnosis API -> evidence-backed result
                                                -> evaluation and observability
```

The LLM is not the first component. A deterministic parser and keyword-search baseline come first so the project can measure whether retrieval and model synthesis improve on the existing manual workflow.

## Problem

Airflow task logs can be long and noisy. Engineers often search manually for keywords, then inspect stack traces and related task context to determine the likely failure cause. This assistant aims to reduce that investigation effort while showing the evidence behind its conclusion.

## Intended user and workflow

1. An engineer provides a failed task's log and basic context such as DAG ID, task ID, execution time, and upstream task status.
2. Deterministic parsing extracts error blocks, exception types, timestamps, and line references.
3. Retrieval finds relevant runbook sections and known-incident examples.
4. The model produces a short diagnosis with evidence references, uncertainty, and proposed checks.
5. The engineer reviews the result and decides what to do. The assistant never retries tasks, edits DAGs, or changes infrastructure.

## Example response contract

- **Likely cause:** one concise explanation, or “insufficient evidence”.
- **Evidence:** exact log line ranges and relevant task context.
- **Confidence:** a calibrated category with an explanation, not an unsupported numeric score.
- **Suggested checks:** ordered, reversible investigation steps.
- **Runbook references:** retrieved document and section identifiers.
- **Do not do:** any destructive or production-changing action automatically.

## Portfolio acceptance criteria

The project is considered portfolio-ready only when it can demonstrate all of the following:

- A documented API contract for submitting a log and receiving a diagnosis.
- Log parsing that preserves source line numbers and handles truncated or malformed input.
- Retrieval over a small, versioned set of runbooks with citations to the retrieved sections.
- At least two diagnostic tools with strict input/output schemas and read-only behavior.
- A model API integration with timeout, retry limits, and a clear “insufficient evidence” path.
- A labeled evaluation set of representative failures, including ambiguous and misleading logs.
- Evaluation results for root-cause accuracy, evidence support, useful abstention, and latency/cost where available.
- A comparison against a simple keyword-search baseline so the project demonstrates measured value over manual search.
- Automated checks in CI, containerized local startup, structured logs, and a deployed demo using non-sensitive examples.
- A threat and limitation section covering prompt injection in logs, secrets in logs, unsupported diagnoses, and the synthetic-data limitation.
- A short demo and architecture explanation that make it possible to inspect the evidence behind each answer.

## Initial evaluation scenarios

The first fixture set should cover distinct failure patterns rather than many variations of one error:

- Python exception in task code.
- Upstream task failure that causes a downstream task to be skipped or fail.
- Connection or authentication failure with sensitive values redacted.
- Timeout or worker/resource termination.
- Data-quality assertion failure.
- Truncated log with no reliable root-cause evidence.
- A misleading warning followed by a different fatal exception.

For every fixture, record the expected cause category, the decisive evidence lines, the relevant runbook section, and whether the correct response should abstain.

## Planned milestones

1. **Problem and data contract:** finalize input/output schemas and the first labeled log fixtures.
2. **Reliable baseline:** build a deterministic parser and keyword-search baseline before adding an LLM.
3. **Grounded diagnosis:** add runbook retrieval, evidence citations, and model-generated explanations.
4. **Tool-using workflow:** add read-only diagnostic tools and bounded orchestration.
5. **Evaluation and hardening:** measure against fixtures, test ambiguous cases, add privacy and prompt-injection protections.
6. **Delivery:** add API, CI, container, observability, and a public demo with synthetic data.

## Honest project positioning

A deployed demo demonstrates deployment and operational engineering practices. It is not evidence that the system has served real production users. CV wording must reflect the completed implementation and measured results; do not claim production use unless it actually occurs.
