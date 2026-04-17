from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

from src.utils.airflow_helpers import (
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    audit_load_audit,
    run_airbyte_cloud_sync,
    run_extract_watermark_event,
    send_operational_alert,
)


def register_extract_start(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    target_name = "RAW"
    pipeline_name = dag_conf.get("watermark_pipeline_name")

    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_START",
        source_name="AIRBYTE",
        target_name=target_name,
        status="STARTED",
        details=f"pipeline={pipeline_name}",
        execution_order=1,
        started_at=started_at,
    )

    result = run_extract_watermark_event(
        pipeline_name=pipeline_name,
        id_clientes=None,
        event_type="start",
    )

    duration_seconds = int(time.time() - step_start_time)
    ended_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_START",
        source_name="AIRBYTE",
        target_name=target_name,
        status="SUCCESS",
        details=f"event={result.get('event_type')}",
        execution_order=1,
        duration_seconds=duration_seconds,
        started_at=started_at,
        ended_at=ended_at,
    )

    return result


def register_extract_end(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    target_name = "RAW"
    pipeline_name = dag_conf.get("watermark_pipeline_name")

    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_END",
        source_name="AIRBYTE",
        target_name=target_name,
        status="STARTED",
        details=f"pipeline={pipeline_name}",
        execution_order=3,
        started_at=started_at,
    )

    result = run_extract_watermark_event(
        pipeline_name=pipeline_name,
        id_clientes=None,
        event_type="end",
    )

    duration_seconds = int(time.time() - step_start_time)
    ended_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_END",
        source_name="AIRBYTE",
        target_name=target_name,
        status="SUCCESS",
        details=f"event={result.get('event_type')}",
        execution_order=3,
        duration_seconds=duration_seconds,
        started_at=started_at,
        ended_at=ended_at,
    )

    return result


def run_airbyte_sync(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}

    raw_connection_id = dag_conf.get("airbyte_connection_id")
    if isinstance(raw_connection_id, str):
        raw_connection_id = raw_connection_id.strip().strip('"').strip("'")

    connection_id = str(
        raw_connection_id
        or context["params"].get("airbyte_connection_id")
        or ""
    ).strip()

    if not connection_id:
        raise ValueError("airbyte_connection_id nao informado para a DAG de extracao.")

    timeout_seconds = int(
        dag_conf.get("airbyte_timeout_seconds")
        or context["params"].get("airbyte_timeout_seconds")
        or 3600
    )
    poll_interval_seconds = int(
        dag_conf.get("airbyte_poll_interval_seconds")
        or context["params"].get("airbyte_poll_interval_seconds")
        or 15
    )
    target_name = "RAW"
    pipeline_name = dag_conf.get("watermark_pipeline_name")

    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="AIRBYTE_SYNC",
        source_name="AIRBYTE",
        target_name=target_name,
        status="STARTED",
        details=f"pipeline={pipeline_name}",
        execution_order=2,
        started_at=started_at,
    )

    try:
        result = run_airbyte_cloud_sync(
            connection_id=connection_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        rows_processed = result.get("rows_processed")
        details = f"job_id={result.get('job_id')} status={result.get('job_status')}"
        if rows_processed is not None:
            details += f" rows_processed={rows_processed}"

        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()

        audit_load_audit(
            batch_id=dag_run.run_id,
            step_name="AIRBYTE_SYNC",
            source_name="AIRBYTE",
            target_name=target_name,
            status="SUCCESS",
            rows_processed=rows_processed,
            details=details,
            execution_order=2,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        return result
    except Exception as exc:
        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()
        audit_load_audit(
            batch_id=dag_run.run_id,
            step_name="AIRBYTE_SYNC",
            source_name="AIRBYTE",
            target_name=target_name,
            status="FAILED",
            details=str(exc),
            execution_order=2,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        raise


def assert_airbyte_run_success(**context: Any) -> dict[str, Any]:
    ti_context = context["ti"]
    dag_run = context.get("dag_run")

    task_results = {
        "register_extract_start": ti_context.xcom_pull(task_ids="register_extract_start"),
        "sync_ds_airbyte": ti_context.xcom_pull(task_ids="sync_ds_airbyte"),
        "register_extract_end": ti_context.xcom_pull(task_ids="register_extract_end"),
    }

    failed_tasks: list[str] = []
    for task_id, result in task_results.items():
        if result is None:
            failed_tasks.append(task_id)
            continue
        if isinstance(result, dict):
            normalized_status = str(result.get("status", "")).upper()
            if normalized_status == "SUCCESS":
                continue
        failed_tasks.append(task_id)

    if failed_tasks:
        title = f"Airflow DAG FAILURE: load_ds_airbyte_dimensions_dag"
        message = "\n".join(
            [
                f"dag_id=load_ds_airbyte_dimensions_dag",
                f"run_id={dag_run.run_id if dag_run is not None else 'unknown_run'}",
                f"failed_tasks={', '.join(failed_tasks)}",
                "observacao=dag finalizou com falha ou estado inconsistente na etapa de extracao",
            ]
        )
        send_operational_alert(title=title, message=message, severity="ERROR")
        raise AirflowFailException(
            f"Falha na DAG de extracao Airbyte. tasks problemáticas: {', '.join(failed_tasks)}"
        )

    return {"status": "SUCCESS"}

with DAG(
    dag_id="load_ds_airbyte_dimensions_dag",
    description="Executa apenas a extracao Airbyte para a trilha de dimensoes e registra metadados de extracao.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["airbyte", "ds", "dimensions"],
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": airflow_failure_alert_callback,
        "on_retry_callback": airflow_retry_alert_callback,
    },
    params={
        "airbyte_connection_id": "",
        "watermark_pipeline_name": "",
        "airbyte_timeout_seconds": 3600,
        "airbyte_poll_interval_seconds": 15,
    },
    on_failure_callback=airflow_failure_alert_callback,
) as dag:
    register_extract_start_task = PythonOperator(
        task_id="register_extract_start",
        python_callable=register_extract_start,
        queue="dbt",
    )

    sync_ds_airbyte = PythonOperator(
        task_id="sync_ds_airbyte",
        python_callable=run_airbyte_sync,
        queue="dbt",
        pool="airbyte_sync_pool",
    )

    register_extract_end_task = PythonOperator(
        task_id="register_extract_end",
        python_callable=register_extract_end,
        queue="dbt",
    )

    assert_airbyte_run_success_task = PythonOperator(
        task_id="assert_airbyte_run_success",
        python_callable=assert_airbyte_run_success,
        queue="dbt",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    register_extract_start_task >> sync_ds_airbyte >> register_extract_end_task >> assert_airbyte_run_success_task
