from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="reproc_sx_equipamento_d_dag",
    description="Executa reprocessamento full global do pipeline SX_EQUIPAMENTO_D relendo todo o RAW.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airbyte", "ds", "reprocess", "manual", "sx_equipamento_d"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
) as dag:
    trigger_reprocess = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt_reprocess",
        trigger_dag_id="orchestrate_ds_dw_dag",
        conf={
            "airbyte_connection_id": "404f8969-e421-416e-96f6-cd0434047acf",
            "models": ["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"],
            "reconciliation_mode": "full",
            "full_refresh": True,
            "dbt_vars": {},
            "watermark_pipeline_name": "dim_sx_equipamento_d",
            "watermark_id_clientes": [0],
            "airbyte_timeout_seconds": 7200,
            "airbyte_poll_interval_seconds": 30,
        },
        queue="dbt",
    )

    trigger_reprocess
