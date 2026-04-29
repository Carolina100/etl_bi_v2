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
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_STARTED,
    AUDIT_STATUS_SUCCESS,
    PIPELINE_STATUS_FAILED,
    PIPELINE_STATUS_SUCCESS,
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    audit_batch_execution_end,
    audit_batch_execution_start,
    audit_load_audit,
    execute_snowflake_sql,
    fetch_batch_row_totals,
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


def task_success_callback(context: dict[str, Any]) -> None:
    """Register task success status via XCom"""
    task_instance = context.get("task_instance")
    if task_instance:
        task_instance.xcom_push(key="task_status", value="SUCCESS")


def task_failure_callback(context: dict[str, Any]) -> None:
    """Register task failure status via XCom"""
    task_instance = context.get("task_instance")
    if task_instance:
        task_instance.xcom_push(key="task_status", value="FAILED")
    airflow_failure_alert_callback(context)


def register_batch_end(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    ti = task_instance

    if dag_run is None:
        raise AirflowFailException("dag_run nao encontrado no contexto")

    target_task_ids = [
        "trigger_load_ds_airbyte",
        "trigger_load_dw_dbt",
        "cleanup_raw_after_success",
    ]

    failed_tasks = []

    # The orchestration is conservative: missing task status is treated as failure.
    for task_id in target_task_ids:
        try:
            task_status = ti.xcom_pull(task_ids=task_id, key="task_status")
        except Exception as exc:
            failed_tasks.append(f"{task_id}:UNKNOWN({exc})")
            continue

        if task_status != "SUCCESS":
            failed_tasks.append(f"{task_id}:{task_status or 'UNKNOWN'}")

    status = PIPELINE_STATUS_SUCCESS if not failed_tasks else PIPELINE_STATUS_FAILED
    error_message = None
    if failed_tasks:
        error_message = f"tasks failed: {', '.join(failed_tasks)}"

    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    pipeline_name = dag_conf.get("watermark_pipeline_name") or dag_run.dag_id
    source_name = "ORCHESTRATION"
    target_name = "DS->DW"

    batch_start_time_seconds = dag_run.start_date.timestamp() if dag_run.start_date else time.time()
    duration_seconds = int(time.time() - batch_start_time_seconds)
    batch_row_totals = fetch_batch_row_totals(dag_run.run_id)

    audit_batch_execution_end(
        batch_id=dag_run.run_id,
        pipeline_name=pipeline_name,
        source_name=source_name,
        target_name=target_name,
        status=status,
        error_message=error_message,
        rows_extracted=batch_row_totals.get("rows_extracted"),
        rows_loaded=batch_row_totals.get("rows_loaded"),
        rows_affected=batch_row_totals.get("rows_affected"),
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
            status=AUDIT_STATUS_STARTED,
            details=spec.get("description", "cleanup tecnico do RAW apos sucesso do pipeline"),
            execution_order=execution_order,
            started_at=started_at,
        )

        try:
            execution_result = execute_snowflake_sql(
                sql=spec["sql"],
                role_env_var="SNOWFLAKE_ROLE_RAW_CLEANUP",
            )
            rows_affected = execution_result.get("rows_affected")
            if rows_affected is not None:
                total_rows_deleted += int(rows_affected)

            audit_load_audit(
                batch_id=batch_id,
                step_name=spec.get("step_name", f"CLEANUP_RAW_{execution_order}"),
                source_name="RETENTION",
                target_name=spec["target_name"],
                status=AUDIT_STATUS_SUCCESS,
                details="cleanup tecnico do RAW concluido",
                rows_processed=rows_affected,
                execution_order=execution_order,
                duration_seconds=int(time.time() - step_start_time),
                started_at=started_at,
                ended_at=datetime.utcnow(),
            )
        except Exception as exc:
            audit_load_audit(
                batch_id=batch_id,
                step_name=spec.get("step_name", f"CLEANUP_RAW_{execution_order}"),
                source_name="RETENTION",
                target_name=spec["target_name"],
                status=AUDIT_STATUS_FAILED,
                details=str(exc),
                execution_order=execution_order,
                duration_seconds=int(time.time() - step_start_time),
                started_at=started_at,
                ended_at=datetime.utcnow(),
            )
            raise
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
            "parent_batch_id": "{{ dag_run.run_id }}",
            "airbyte_connection_id": "{{ dag_run.conf.get('airbyte_connection_id', params.airbyte_connection_id) }}",
            "watermark_pipeline_name": "{{ dag_run.conf.get('watermark_pipeline_name', '') }}",
            "airbyte_timeout_seconds": "{{ dag_run.conf.get('airbyte_timeout_seconds', 3600) }}",
            "airbyte_poll_interval_seconds": "{{ dag_run.conf.get('airbyte_poll_interval_seconds', 15) }}",
        },
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        queue="dbt",
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    trigger_load_dw_dbt = TriggerDagRunOperator(
        task_id="trigger_load_dw_dbt",
        trigger_dag_id="load_dw_dbt_dimensions_dag",
        conf={
            "parent_batch_id": "{{ dag_run.run_id }}",
            "models": "{{ dag_run.conf.get('models', []) | tojson }}",
            "dbt_vars": "{{ dag_run.conf.get('dbt_vars', {}) | tojson }}",
            "dbt_command": "{{ dag_run.conf.get('dbt_command', 'run') }}",
            "watermark_pipeline_name": "{{ dag_run.conf.get('watermark_pipeline_name', '') }}",
        },
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        queue="dbt",
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
    )

    cleanup_raw_after_success_task = PythonOperator(
        task_id="cleanup_raw_after_success",
        python_callable=cleanup_raw_after_success,
        queue="dbt",
        pool="retention_cleanup_pool",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback,
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
