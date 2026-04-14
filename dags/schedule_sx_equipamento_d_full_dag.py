from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="schedule_sx_equipamento_d_full_dag",
    description="Agenda a execucao full de reconciliacao da DAG principal para SX_EQUIPAMENTO_D.",
    start_date=datetime(2025, 1, 1),
    schedule="15 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["airbyte", "ds", "full", "reconciliation", "scheduler", "sx_equipamento_d"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
) as dag:
    trigger_full = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt_full",
        trigger_dag_id="orchestrate_ds_dw_dag",
        conf={
            "airbyte_connection_id": "404f8969-e421-416e-96f6-cd0434047acf",
            "models": [],
            "client_models": ["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"],
            "reconciliation_mode": "full",
            "full_refresh": True,
            "dbt_vars": {},
            "continue_on_client_error": True,
            "watermark_pipeline_name": "dim_sx_equipamento_d",
            "watermark_id_clientes": None,
            "watermark_client_source_table": "SOLIX_BI.DS.SX_CLIENTE_D",
            "watermark_client_id_column": "ID_CLIENTE",
            "watermark_client_active_column": "FL_ATIVO",
            "airbyte_timeout_seconds": 7200,
            "airbyte_poll_interval_seconds": 30
        },
    )

    trigger_full
