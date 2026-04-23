from __future__ import annotations

import ast
import json
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.utils.airflow_helpers import (
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    audit_load_audit,
    mark_pipeline_watermark_failure,
    run_dbt_command,
)


def get_audit_batch_id(context: dict[str, Any]) -> str:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    return str(dag_conf.get("parent_batch_id") or dag_run.run_id)


def run_dbt(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    models = parse_string_list(dag_conf.get("models"))
    if not models:
        models = ["."]

    raw_dbt_vars = dag_conf.get("dbt_vars")
    if isinstance(raw_dbt_vars, str):
        if raw_dbt_vars.strip().lower() in {"none", "null", ""}:
            dbt_vars = {}
        else:
            parsed_dbt_vars = try_parse_json_dict(raw_dbt_vars)
            if parsed_dbt_vars is not None:
                dbt_vars = parsed_dbt_vars
            else:
                parsed_python_dbt_vars = try_parse_python_dict(raw_dbt_vars)
                dbt_vars = parsed_python_dbt_vars or {}
    elif isinstance(raw_dbt_vars, dict):
        dbt_vars = raw_dbt_vars
    else:
        dbt_vars = {}

    batch_id = get_audit_batch_id(context)
    watermark_pipeline_names = derive_watermark_pipeline_names(models)
    source_name = "DBT"
    details = "mode=incremental"

    execution_summary: dict[str, Any] = {
        "status": "SUCCESS",
        "models": models,
    }

    execution_summary["result"] = execute_dbt_step(
        batch_id=batch_id,
        step_name="DBT_RUN",
        source_name=source_name,
        target_name=",".join(models),
        details=details,
        select_models=models,
        dbt_vars=dbt_vars,
        execution_order=1,
        watermark_pipeline_names=watermark_pipeline_names,
    )

    return execution_summary


with DAG(
    dag_id="load_dw_dbt_dimensions_dag",
    description="Executa apenas dbt para a trilha de dimensoes na carga DS -> DW.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["dbt", "dw", "dimensions"],
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": airflow_failure_alert_callback,
        "on_retry_callback": airflow_retry_alert_callback,
    },
    params={
        "dbt_vars": {},
        "models": [],
    },
    on_failure_callback=airflow_failure_alert_callback,
) as dag:
    run_dw_dbt = PythonOperator(
        task_id="run_dw_dbt",
        python_callable=run_dbt,
        queue="dbt",
        pool="dbt_build_pool",
    )

    run_dw_dbt


def execute_dbt_step(
    *,
    batch_id: str,
    step_name: str,
    source_name: str,
    target_name: str,
    details: str,
    select_models: list[str],
    dbt_vars: dict[str, Any],
    execution_order: int,
    watermark_pipeline_names: list[str] | None = None,
) -> dict[str, Any]:
    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=batch_id,
        step_name=step_name,
        source_name=source_name,
        target_name=target_name,
        status="STARTED",
        details=details,
        execution_order=execution_order,
        started_at=started_at,
    )

    try:
        result = run_dbt_command(
            dbt_project_dir="/opt/airflow/project/dbt/solix_dbt",
            dbt_profiles_dir="/opt/airflow/project/dbt/solix_dbt",
            select_models=select_models,
            dbt_vars=dbt_vars,
            full_refresh=False,
        )
        run_results = result.get("run_results", {})
        rows_processed = int(run_results.get("rows_affected_total") or 0)
        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()

        audit_load_audit(
            batch_id=batch_id,
            step_name=step_name,
            source_name=source_name,
            target_name=target_name,
            status="SUCCESS",
            rows_processed=rows_processed,
            details=f"{details} rows_affected={rows_processed}",
            execution_order=execution_order,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        return {"result": result, "rows_processed": rows_processed}
    except Exception as exc:
        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()
        audit_load_audit(
            batch_id=batch_id,
            step_name=step_name,
            source_name=source_name,
            target_name=target_name,
            status="FAILED",
            details=str(exc),
            execution_order=execution_order,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        for pipeline_name in watermark_pipeline_names or []:
            mark_pipeline_watermark_failure(
                pipeline_name=pipeline_name,
                batch_id=batch_id,
                error_message=str(exc),
            )
        raise


def derive_watermark_pipeline_names(models: list[str]) -> list[str]:
    """
    Converte a lista de modelos informada no dag_run.conf para os pipelines
    dimensionais que precisam ser marcados como FAILED em caso de erro do dbt.

    Exemplo:
    - ds_sx_cliente_d -> dim_sx_cliente_d
    - stg_ds__sx_estado_d -> dim_sx_estado_d
    - dim_sx_equipamento_d -> dim_sx_equipamento_d
    """
    pipeline_names: list[str] = []

    for raw_model in models:
        model = str(raw_model).strip()
        if not model:
            continue

        if model.startswith("dim_"):
            candidate = model
        elif model.startswith("stg_ds__"):
            entity_name = model[len("stg_ds__") :]
            candidate = f"dim_{entity_name}"
        elif model.startswith("ds_"):
            entity_name = model[len("ds_") :]
            candidate = f"dim_{entity_name}"
        else:
            continue

        if candidate not in pipeline_names:
            pipeline_names.append(candidate)

    return pipeline_names


def parse_string_list(raw_value: Any) -> list[str]:
    if isinstance(raw_value, str):
        if raw_value.strip().lower() in {"none", "null", ""}:
            return []
        parsed_models = try_parse_json_list(raw_value)
        if parsed_models is not None:
            return [str(item).strip() for item in parsed_models if str(item).strip()]
        parsed_python_models = try_parse_python_list(raw_value)
        if parsed_python_models is not None:
            return [str(item).strip() for item in parsed_python_models if str(item).strip()]
        return [item.strip() for item in raw_value.split(",") if item.strip()]
    if isinstance(raw_value, list):
        return [str(item).strip() for item in raw_value if str(item).strip()]
    return []


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


def try_parse_json_dict(raw_value: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return None

    return parsed if isinstance(parsed, dict) else None


def try_parse_python_dict(raw_value: str) -> dict[str, Any] | None:
    try:
        parsed = ast.literal_eval(raw_value)
    except Exception:
        return None

    return parsed if isinstance(parsed, dict) else None
