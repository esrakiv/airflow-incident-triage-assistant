FROM apache/airflow:slim-3.4.0-python3.12

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 dbt-duckdb==1.11.0 duckdb==1.5.5

RUN pip install --no-cache-dir --constraint "${HOME}/constraints.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-cncf-kubernetes
