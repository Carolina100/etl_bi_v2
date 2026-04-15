from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.settings import Session
from airflow.task.trigger_rule import TriggerRule

from src.utils.airflow_helpers import (
    audit_batch_execution_end,
    audit_batch_execution_start,
)


def register_batch_start(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    source_name = "ORCHESTRATION"
    target_name = "DS->DW"
    pipeline_name = dag_conf.get("watermark_pipeline_name") or dag_run.dag_id
    orchestration_type = "MANUAL_TRIGGER" if dag_run.run_type == "manual" else "SCHEDULER"

    audit_batch_execution_start(
        batch_id=dag_run.run_id,
        pipeline_name=pipeline_name,
        source_name=source_name,
        target_name=target_name,
        orchestration_type=orchestration_type,
        id_cliente=resolve_single_client_id(dag_conf.get("watermark_id_clientes")),
    )
    return {"status": "STARTED", "orchestration_type": orchestration_type}


def register_batch_end(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    session = Session()
    task_instances = {}
    try:
        if dag_run is not None:
            rows = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id,
                    TaskInstance.task_id.in_(["trigger_load_ds_airbyte", "trigger_load_dw_dbt"]),
                )
                .all()
            )
            task_instances = {task_instance.task_id: task_instance for task_instance in rows}
    finally:
        session.close()

    task_states = []
    for task_id in ("trigger_load_ds_airbyte", "trigger_load_dw_dbt"):
        ti = task_instances.get(task_id)
        if ti is not None and ti.state is not None:
            task_states.append((task_id, ti.state))

    failed_tasks = [task_id for task_id, state in task_states if state not in {"success", "skipped"}]
    status = "SUCCESS" if not failed_tasks else "FAILED"
    error_message = None
    if failed_tasks:
        error_message = f"tasks failed: {', '.join(failed_tasks)}"

    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    pipeline_name = dag_conf.get("watermark_pipeline_name") or dag_run.dag_id
    source_name = "ORCHESTRATION"
    target_name = "DS->DW"
    
    batch_start_time_seconds = dag_run.start_date.timestamp() if dag_run.start_date else time.time()
    duration_seconds = int(time.time() - batch_start_time_seconds)

    audit_batch_execution_end(
        batch_id=dag_run.run_id,
        pipeline_name=pipeline_name,
        source_name=source_name,
        target_name=target_name,
        status=status,
        error_message=error_message,
        duration_seconds=duration_seconds,
        id_cliente=resolve_single_client_id(dag_conf.get("watermark_id_clientes")),
    )
    return {"status": status}


with DAG(
    dag_id="orchestrate_ds_dw_dag",
    description="Orquestra o fluxo Airbyte -> dbt mantendo produtos separados em DAGs distintas.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "airbyte", "dbt"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
    params={
        "airbyte_connection_id": "",
    },
) as dag:
    trigger_load_ds_airbyte = TriggerDagRunOperator(
        task_id="trigger_load_ds_airbyte",
        trigger_dag_id="load_ds_airbyte_dag",
        conf={
            "airbyte_connection_id": "{{ dag_run.conf.get('airbyte_connection_id', params.airbyte_connection_id) }}",
            "watermark_pipeline_name": "{{ dag_run.conf.get('watermark_pipeline_name', '') }}",
            "watermark_id_clientes": "{{ dag_run.conf.get('watermark_id_clientes') | tojson }}",
            "watermark_client_source_table": "{{ dag_run.conf.get('watermark_client_source_table', '') }}",
            "watermark_client_id_column": "{{ dag_run.conf.get('watermark_client_id_column', 'CD_ID') }}",
            "watermark_client_active_column": "{{ dag_run.conf.get('watermark_client_active_column', 'FG_ATIVO') }}",
            "airbyte_timeout_seconds": "{{ dag_run.conf.get('airbyte_timeout_seconds', 3600) }}",
            "airbyte_poll_interval_seconds": "{{ dag_run.conf.get('airbyte_poll_interval_seconds', 15) }}",
        },
        wait_for_completion=True,
        poke_interval=30,
        queue="dbt",
    )

    trigger_load_dw_dbt = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt",
        trigger_dag_id="load_dw_dbt_dag",
        conf={
            "models": "{{ dag_run.conf.get('models', []) | tojson }}",
            "client_models": "{{ dag_run.conf.get('client_models', []) | tojson }}",
            "reconciliation_mode": "{{ dag_run.conf.get('reconciliation_mode', 'incremental') }}",
            "dbt_vars": "{{ dag_run.conf.get('dbt_vars', {}) | tojson }}",
            "full_refresh": "{{ dag_run.conf.get('full_refresh', false) }}",
            "continue_on_client_error": "{{ dag_run.conf.get('continue_on_client_error', true) }}",
            "watermark_id_clientes": "{{ dag_run.conf.get('watermark_id_clientes') | tojson }}",
            "watermark_client_source_table": "{{ dag_run.conf.get('watermark_client_source_table', '') }}",
            "watermark_client_id_column": "{{ dag_run.conf.get('watermark_client_id_column', 'ID_CLIENTE') }}",
            "watermark_client_active_column": "{{ dag_run.conf.get('watermark_client_active_column', 'FL_ATIVO') }}",
        },
        wait_for_completion=True,
        poke_interval=30,
        queue="dbt",
    )

    register_batch_start_task = PythonOperator(
        task_id="register_batch_start",
        python_callable=register_batch_start,
        queue="dbt",
    )

    register_batch_end_task = PythonOperator(
        task_id="register_batch_end",
        python_callable=register_batch_end,
        queue="dbt",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    register_batch_start_task >> trigger_load_ds_airbyte >> trigger_load_dw_dbt >> register_batch_end_task


def resolve_single_client_id(raw_value: Any) -> int | None:
    if isinstance(raw_value, list) and len(raw_value) == 1:
        return int(raw_value[0])
    if isinstance(raw_value, int):
        return raw_value
    return None
