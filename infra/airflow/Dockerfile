FROM apache/airflow:3.1.8

USER airflow

COPY infra/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
