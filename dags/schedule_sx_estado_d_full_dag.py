from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="schedule_sx_estado_d_full_dag",
    description="Agenda a execucao full de reconciliacao da DAG principal para SX_ESTADO_D.",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["airbyte", "ds", "full", "reconciliation", "scheduler", "sx_estado_d"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
) as dag:
    trigger_full = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt_full",
        trigger_dag_id="orchestrate_ds_dw_dag",
        conf={
            "airbyte_connection_id": "00000000-0000-0000-0000-000000000002",
            "models": ["ds_sx_estado_d", "stg_ds__sx_estado_d", "dim_sx_estado_d"],
            "reconciliation_mode": "full",
            "full_refresh": True,
            "dbt_vars": {
                "sx_estado_d_reconciliation_mode": "full"
            },
            "watermark_pipeline_name": "dim_sx_estado_d",
            "watermark_id_clientes": [0]
        },
    )

    trigger_full
