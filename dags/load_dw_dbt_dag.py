from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from src.utils.airflow_helpers import run_dbt_command


def run_dbt(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    raw_models = None
    if dag_conf:
        raw_models = dag_conf.get("models")

    if isinstance(raw_models, str):
        select_models = [item.strip() for item in raw_models.split(",") if item.strip()]
    elif isinstance(raw_models, list):
        select_models = [str(item).strip() for item in raw_models if str(item).strip()]
    else:
        select_models = ["."]

    reconciliation_mode = str(
        dag_conf.get("reconciliation_mode")
        or context["params"].get("reconciliation_mode")
        or "incremental"
    ).strip().lower()
    if reconciliation_mode not in {"incremental", "full"}:
        reconciliation_mode = "incremental"

    return run_dbt_command(
        dbt_project_dir="/opt/airflow/project/dbt/solix_dbt",
        dbt_profiles_dir="/opt/airflow/project/dbt/solix_dbt",
        select_models=select_models,
        dbt_vars={"sx_estado_d_reconciliation_mode": reconciliation_mode},
    )


with DAG(
    dag_id="load_dw_dbt_dag",
    description="Executa dbt build para a camada DW após os dados estarem carregados em Snowflake DS.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "dw"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
    params={
        "airbyte_connection_id": "",
        "reconciliation_mode": "incremental",
    },
) as dag:
    
    sync_ds_airbyte = AirbyteTriggerSyncOperator(
        task_id="sync_ds_airbyte",
        airbyte_conn_id="airbyte_default",
        connection_id="{{ dag_run.conf.get('airbyte_connection_id', params.airbyte_connection_id) }}",
        asynchronous=False,
        deferrable=True,
        queue="dbt",
    )

    run_dw_dbt = PythonOperator(
        task_id="run_dw_dbt",
        python_callable=run_dbt,
        queue="dbt",
    )

    sync_ds_airbyte >> run_dw_dbt
