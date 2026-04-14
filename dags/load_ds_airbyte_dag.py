from __future__ import annotations

import ast
import json
import time
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.utils.airflow_helpers import (
    audit_load_audit,
    run_airbyte_cloud_sync,
    run_extract_watermark_event,
)


def register_extract_start(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    client_ids = parse_watermark_client_ids(dag_conf.get("watermark_id_clientes"))
    id_cliente = client_ids[0] if client_ids and len(client_ids) == 1 else None
    target_name = (
        dag_conf.get("watermark_client_source_table")
        or context["params"].get("watermark_client_source_table")
        or "UNKNOWN"
    )
    pipeline_name = dag_conf.get("watermark_pipeline_name")

    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_START",
        source_name="AIRBYTE",
        target_name=target_name,
        status="STARTED",
        id_cliente=id_cliente,
        details=f"pipeline={pipeline_name}",
        execution_order=1,
        started_at=started_at,
    )

    result = run_extract_watermark_event(
        pipeline_name=pipeline_name,
        id_clientes=client_ids,
        event_type="start",
        client_source_table=target_name,
        client_id_column=str(
            dag_conf.get("watermark_client_id_column")
            or context["params"].get("watermark_client_id_column")
            or "CD_ID"
        ),
        client_active_column=dag_conf.get("watermark_client_active_column")
        or context["params"].get("watermark_client_active_column"),
    )

    duration_seconds = int(time.time() - step_start_time)
    ended_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_START",
        source_name="AIRBYTE",
        target_name=target_name,
        status="SUCCESS",
        id_cliente=id_cliente,
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
    client_ids = parse_watermark_client_ids(dag_conf.get("watermark_id_clientes"))
    id_cliente = client_ids[0] if client_ids and len(client_ids) == 1 else None
    target_name = (
        dag_conf.get("watermark_client_source_table")
        or context["params"].get("watermark_client_source_table")
        or "UNKNOWN"
    )
    pipeline_name = dag_conf.get("watermark_pipeline_name")

    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_END",
        source_name="AIRBYTE",
        target_name=target_name,
        status="STARTED",
        id_cliente=id_cliente,
        details=f"pipeline={pipeline_name}",
        execution_order=3,
        started_at=started_at,
    )

    result = run_extract_watermark_event(
        pipeline_name=pipeline_name,
        id_clientes=client_ids,
        event_type="end",
        client_source_table=target_name,
        client_id_column=str(
            dag_conf.get("watermark_client_id_column")
            or context["params"].get("watermark_client_id_column")
            or "CD_ID"
        ),
        client_active_column=dag_conf.get("watermark_client_active_column")
        or context["params"].get("watermark_client_active_column"),
    )

    duration_seconds = int(time.time() - step_start_time)
    ended_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="REGISTER_EXTRACT_END",
        source_name="AIRBYTE",
        target_name=target_name,
        status="SUCCESS",
        id_cliente=id_cliente,
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
    target_name = (
        dag_conf.get("watermark_client_source_table")
        or context["params"].get("watermark_client_source_table")
        or "UNKNOWN"
    )
    pipeline_name = dag_conf.get("watermark_pipeline_name")
    id_clientes = parse_watermark_client_ids(dag_conf.get("watermark_id_clientes"))
    id_cliente = id_clientes[0] if id_clientes and len(id_clientes) == 1 else None

    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=dag_run.run_id,
        step_name="AIRBYTE_SYNC",
        source_name="AIRBYTE",
        target_name=target_name,
        status="STARTED",
        id_cliente=id_cliente,
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
            id_cliente=id_cliente,
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
            id_cliente=id_cliente,
            details=str(exc),
            execution_order=2,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        raise


def parse_watermark_client_ids(raw_value: Any) -> list[int]:
    if raw_value is None or raw_value == "":
        return None

    if isinstance(raw_value, str) and raw_value.strip().lower() in {"none", "null", ""}:
        return None

    if isinstance(raw_value, int):
        return [raw_value]

    if isinstance(raw_value, list):
        return [int(item) for item in raw_value]

    if isinstance(raw_value, str):
        parsed_ids = try_parse_json_list(raw_value)
        if parsed_ids is not None:
            return [int(item) for item in parsed_ids]
        parsed_python_ids = try_parse_python_list(raw_value)
        if parsed_python_ids is not None:
            return [int(item) for item in parsed_python_ids]
        return [int(item.strip()) for item in raw_value.split(",") if item.strip()]

    return None


with DAG(
    dag_id="load_ds_airbyte_dag",
    description="Executa apenas a extracao Airbyte para DS e registra metadados de extracao.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["airbyte", "ds"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
    params={
        "airbyte_connection_id": "",
        "watermark_pipeline_name": "",
        "watermark_id_clientes": None,
        "watermark_client_source_table": "",
        "watermark_client_id_column": "CD_ID",
        "watermark_client_active_column": "FG_ATIVO",
        "airbyte_timeout_seconds": 3600,
        "airbyte_poll_interval_seconds": 15,
    },
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

    register_extract_start_task >> sync_ds_airbyte >> register_extract_end_task


def try_parse_json_list(raw_value: str) -> list[Any] | None:
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return None

    return parsed if isinstance(parsed, list) else None


def try_parse_python_list(raw_value: str) -> list[Any] | None:
    try:
        parsed = ast.literal_eval(raw_value)
    except Exception:
        return None

    return parsed if isinstance(parsed, list) else None
