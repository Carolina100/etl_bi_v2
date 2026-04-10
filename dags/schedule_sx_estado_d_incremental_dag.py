from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="schedule_sx_estado_d_incremental_dag",
    description="Agenda a execucao incremental da DAG principal para SX_ESTADO_D.",
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["airbyte", "ds", "incremental", "scheduler", "sx_estado_d"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
) as dag:
    trigger_incremental = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt_incremental",
        trigger_dag_id="load_dw_dbt_dag",
        conf={
            "airbyte_connection_id": "00000000-0000-0000-0000-000000000001",
            "models": ["ds_sx_estado_d", "stg_ds__sx_estado_d", "dim_sx_estado_d"],
            "reconciliation_mode": "incremental",
            "wait_for_airbyte": True,
            "airbyte_timeout_seconds": 3600,
            "airbyte_poll_interval_seconds": 15,
        },
    )

    trigger_incremental
