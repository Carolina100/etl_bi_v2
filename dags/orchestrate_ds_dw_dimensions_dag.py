from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule

from src.utils.airflow_helpers import (
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    audit_batch_execution_end,
    audit_batch_execution_start,
    audit_load_audit,
    execute_snowflake_sql,
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
    )
    return {"status": "STARTED", "orchestration_type": orchestration_type}


def register_batch_end(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    ti_context = context["ti"]
    task_results = {
        task_id: ti_context.xcom_pull(task_ids=task_id)
        for task_id in ("trigger_load_ds_airbyte", "trigger_load_dw_dbt", "cleanup_raw_after_success")
    }

    failed_tasks = []
    for task_id, result in task_results.items():
        if result is None:
            failed_tasks.append(task_id)
            continue
        if isinstance(result, dict):
            normalized_status = str(result.get("status", "")).upper()
            if normalized_status in {"SUCCESS", "SKIPPED"}:
                continue
        failed_tasks.append(task_id)

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
    )
    if status == "FAILED":
        raise AirflowFailException(error_message or "Falha na orquestracao DS->DW.")
    return {"status": status}


def cleanup_raw_after_success(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    cleanup_specs = parse_cleanup_specs(dag_conf.get("cleanup_raw_specs"))

    if not cleanup_specs:
        return {"status": "SKIPPED", "reason": "cleanup_raw_specs nao informado"}

    batch_id = dag_run.run_id
    total_rows_deleted = 0
    executed_targets: list[str] = []

    for execution_order, spec in enumerate(cleanup_specs, start=1):
        step_start_time = time.time()
        started_at = datetime.utcnow()

        audit_load_audit(
            batch_id=batch_id,
            step_name=spec.get("step_name", f"CLEANUP_RAW_{execution_order}"),
            source_name="RETENTION",
            target_name=spec["target_name"],
            status="STARTED",
            details=spec.get("description", "cleanup tecnico do RAW apos sucesso do pipeline"),
            execution_order=execution_order,
            started_at=started_at,
        )

        execution_result = execute_snowflake_sql(sql=spec["sql"])
        rows_affected = execution_result.get("rows_affected")
        if rows_affected is not None:
            total_rows_deleted += int(rows_affected)

        audit_load_audit(
            batch_id=batch_id,
            step_name=spec.get("step_name", f"CLEANUP_RAW_{execution_order}"),
            source_name="RETENTION",
            target_name=spec["target_name"],
            status="SUCCESS",
            details="cleanup tecnico do RAW concluido",
            rows_processed=rows_affected,
            execution_order=execution_order,
            duration_seconds=int(time.time() - step_start_time),
            started_at=started_at,
            ended_at=datetime.utcnow(),
        )
        executed_targets.append(spec["target_name"])

    return {
        "status": "SUCCESS",
        "rows_deleted": total_rows_deleted,
        "targets": executed_targets,
    }


def parse_cleanup_specs(raw_value: Any) -> list[dict[str, str]]:
    if raw_value in (None, "", "null", "None"):
        return []
    if isinstance(raw_value, list):
        return [spec for spec in raw_value if isinstance(spec, dict) and spec.get("sql") and spec.get("target_name")]
    if isinstance(raw_value, str):
        parsed_value = json.loads(raw_value)
        if isinstance(parsed_value, list):
            return [spec for spec in parsed_value if isinstance(spec, dict) and spec.get("sql") and spec.get("target_name")]
    return []


with DAG(
    dag_id="orchestrate_ds_dw_dimensions_dag",
    description="Orquestra a trilha de dimensoes: Airbyte -> dbt -> cleanup tecnico do RAW.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "airbyte", "dbt", "dimensions"],
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "on_failure_callback": airflow_failure_alert_callback,
        "on_retry_callback": airflow_retry_alert_callback,
    },
    on_failure_callback=airflow_failure_alert_callback,
    params={
        "airbyte_connection_id": "",
    },
) as dag:
    trigger_load_ds_airbyte = TriggerDagRunOperator(
        task_id="trigger_load_ds_airbyte",
        trigger_dag_id="load_ds_airbyte_dimensions_dag",
        conf={
            "airbyte_connection_id": "{{ dag_run.conf.get('airbyte_connection_id', params.airbyte_connection_id) }}",
            "watermark_pipeline_name": "{{ dag_run.conf.get('watermark_pipeline_name', '') }}",
            "airbyte_timeout_seconds": "{{ dag_run.conf.get('airbyte_timeout_seconds', 3600) }}",
            "airbyte_poll_interval_seconds": "{{ dag_run.conf.get('airbyte_poll_interval_seconds', 15) }}",
        },
        wait_for_completion=True,
        poke_interval=30,
        queue="dbt",
    )

    trigger_load_dw_dbt = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt",
        trigger_dag_id="load_dw_dbt_dimensions_dag",
        conf={
            "models": "{{ dag_run.conf.get('models', []) | tojson }}",
            "dbt_vars": "{{ dag_run.conf.get('dbt_vars', {}) | tojson }}",
        },
        wait_for_completion=True,
        poke_interval=30,
        queue="dbt",
    )

    cleanup_raw_after_success_task = PythonOperator(
        task_id="cleanup_raw_after_success",
        python_callable=cleanup_raw_after_success,
        queue="dbt",
        pool="retention_cleanup_pool",
        trigger_rule=TriggerRule.ALL_SUCCESS,
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

    register_batch_start_task >> trigger_load_ds_airbyte >> trigger_load_dw_dbt >> cleanup_raw_after_success_task >> register_batch_end_task
