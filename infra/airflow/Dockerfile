FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/oracle

COPY infra/airflow/oracle/instantclient-basic-linux.x64-23.26.1.0.0/instantclient_23_26 /opt/oracle/instantclient_23_26

RUN ln -sf /opt/oracle/instantclient_23_26/libclntsh.so.23.1 /opt/oracle/instantclient_23_26/libclntsh.so \
    && ln -sf /opt/oracle/instantclient_23_26/libclntshcore.so.23.1 /opt/oracle/instantclient_23_26/libclntshcore.so

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_26:${LD_LIBRARY_PATH}

COPY infra/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
