FROM apache/airflow:3.1.8

USER airflow

COPY infra/runtime/dbt/requirements.txt /tmp/requirements-dbt-runtime.txt

RUN pip install --no-cache-dir -r /tmp/requirements-dbt-runtime.txt
