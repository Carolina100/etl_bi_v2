from __future__ import annotations

import ast
import json
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

from src.utils.airflow_helpers import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_STARTED,
    AUDIT_STATUS_SUCCESS,
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    audit_load_audit,
    mark_pipeline_watermark_running,
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
    models = parse_string_list(dag_conf.get("models", context["params"].get("models")))
    if not models:
        raise AirflowFailException(
            "Parametro obrigatorio ausente: models. "
            "Informe explicitamente os modelos dbt para evitar execucao ampla."
        )
    validate_supported_dbt_models(models)

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

    for pipeline_name in watermark_pipeline_names:
        mark_pipeline_watermark_running(
            pipeline_name=pipeline_name,
            batch_id=batch_id,
        )

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
    max_active_runs=1,
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
        status=AUDIT_STATUS_STARTED,
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
        success_details = build_dbt_success_details(
            base_details=details,
            run_results=run_results,
            requested_models=select_models,
        )
        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()

        if result.get("status") != "SUCCESS":
            failed_pipeline_names = derive_failed_watermark_pipeline_names(
                run_results=run_results,
                requested_pipeline_names=watermark_pipeline_names or [],
            )
            failure_message = build_dbt_failure_message(
                result=result,
                failed_pipeline_names=failed_pipeline_names,
            )

            audit_load_audit(
                batch_id=batch_id,
                step_name=step_name,
                source_name=source_name,
                target_name=target_name,
                status=AUDIT_STATUS_FAILED,
                rows_processed=rows_processed,
                details=failure_message,
                execution_order=execution_order,
                duration_seconds=duration_seconds,
                started_at=started_at,
                ended_at=ended_at,
            )

            for pipeline_name in failed_pipeline_names:
                mark_pipeline_watermark_failure(
                    pipeline_name=pipeline_name,
                    batch_id=batch_id,
                    error_message=failure_message,
                )

            raise AirflowFailException(failure_message)

        audit_load_audit(
            batch_id=batch_id,
            step_name=step_name,
            source_name=source_name,
            target_name=target_name,
            status=AUDIT_STATUS_SUCCESS,
            rows_processed=rows_processed,
            details=success_details,
            execution_order=execution_order,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        return {"result": result, "rows_processed": rows_processed}
    except Exception as exc:
        if isinstance(exc, AirflowFailException):
            raise

        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()
        audit_load_audit(
            batch_id=batch_id,
            step_name=step_name,
            source_name=source_name,
            target_name=target_name,
            status=AUDIT_STATUS_FAILED,
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


def derive_failed_watermark_pipeline_names(
    *,
    run_results: dict[str, Any],
    requested_pipeline_names: list[str],
) -> list[str]:
    all_results = run_results.get("all_results") if isinstance(run_results, dict) else None
    if not isinstance(all_results, list):
        return requested_pipeline_names

    failed_pipeline_names: list[str] = []

    for result in all_results:
        if not isinstance(result, dict):
            continue

        status = str(result.get("status") or "").lower()
        if status in {"success", "pass", "skipped"}:
            continue

        unique_id = str(result.get("unique_id") or "")
        matched_pipeline = match_pipeline_name_from_unique_id(
            unique_id=unique_id,
            requested_pipeline_names=requested_pipeline_names,
        )
        if matched_pipeline and matched_pipeline not in failed_pipeline_names:
            failed_pipeline_names.append(matched_pipeline)

    return failed_pipeline_names or requested_pipeline_names


def match_pipeline_name_from_unique_id(
    *,
    unique_id: str,
    requested_pipeline_names: list[str],
) -> str | None:
    unique_id_lower = unique_id.lower()

    for pipeline_name in requested_pipeline_names:
        pipeline_lower = pipeline_name.lower()
        entity_name = pipeline_lower[len("dim_") :] if pipeline_lower.startswith("dim_") else pipeline_lower
        candidates = [
            pipeline_lower,
            f"ds_{entity_name}",
            f"stg_ds__{entity_name}",
        ]

        if any(candidate in unique_id_lower for candidate in candidates):
            return pipeline_name

    return None


def build_dbt_failure_message(
    *,
    result: dict[str, Any],
    failed_pipeline_names: list[str],
) -> str:
    failed_pipelines_text = ",".join(failed_pipeline_names) if failed_pipeline_names else "unknown"
    return (
        "Falha na camada DW. "
        f"returncode={result.get('returncode')}. "
        f"failed_pipelines={failed_pipelines_text}. "
        f"stdout={str(result.get('stdout') or '').strip()} "
        f"stderr={str(result.get('stderr') or '').strip()}"
    )


def build_dbt_success_details(
    *,
    base_details: str,
    run_results: dict[str, Any],
    requested_models: list[str],
) -> str:
    rows_affected_total = int(run_results.get("rows_affected_total") or 0)
    rows_affected_final_total = int(run_results.get("rows_affected_final_total") or 0)
    rows_inserted_total = int(run_results.get("rows_inserted_total") or 0)
    rows_updated_total = int(run_results.get("rows_updated_total") or 0)
    rows_deleted_total = int(run_results.get("rows_deleted_total") or 0)
    rows_inserted_final_total = int(run_results.get("rows_inserted_final_total") or 0)
    rows_updated_final_total = int(run_results.get("rows_updated_final_total") or 0)
    rows_deleted_final_total = int(run_results.get("rows_deleted_final_total") or 0)
    model_results = run_results.get("model_results") if isinstance(run_results, dict) else None
    if not isinstance(model_results, list):
        return (
            f"{base_details} rows_affected_total={rows_affected_total} "
            f"rows_affected_final={rows_affected_final_total}"
        )

    changed_model_rows_summary: list[str] = []
    final_model_rows_summary: list[str] = []
    requested_model_names = {str(model).strip() for model in requested_models if str(model).strip()}

    for model_result in model_results:
        if not isinstance(model_result, dict):
            continue

        unique_id = str(model_result.get("unique_id") or "")
        model_name = unique_id.split(".")[-1] if unique_id else ""
        if requested_model_names and model_name not in requested_model_names:
            continue

        rows_affected = int(model_result.get("rows_affected") or 0)
        rows_inserted = int(model_result.get("rows_inserted") or 0)
        rows_updated = int(model_result.get("rows_updated") or 0)
        rows_deleted = int(model_result.get("rows_deleted") or 0)
        status = model_result.get("status")

        metrics = [
            f"status={status}",
            f"affected={rows_affected}",
            f"inserted={rows_inserted}",
            f"updated={rows_updated}",
            f"deleted={rows_deleted}",
        ]

        model_summary = f"{model_name}({';'.join(metrics)})"
        if any([rows_affected, rows_inserted, rows_updated, rows_deleted]):
            changed_model_rows_summary.append(model_summary)
        if model_name.startswith(("dim_", "fct_")):
            final_model_rows_summary.append(model_summary)

    totals_summary = (
        f"{base_details} rows_affected_total={rows_affected_total} "
        f"rows_inserted_total={rows_inserted_total} "
        f"rows_updated_total={rows_updated_total} "
        f"rows_deleted_total={rows_deleted_total} "
        f"rows_affected_final={rows_affected_final_total} "
        f"rows_inserted_final={rows_inserted_final_total} "
        f"rows_updated_final={rows_updated_final_total} "
        f"rows_deleted_final={rows_deleted_final_total}"
    )

    detail_parts = [totals_summary]
    if changed_model_rows_summary:
        detail_parts.append(f"models_changed={','.join(changed_model_rows_summary)}")
    else:
        detail_parts.append("models_changed=none")

    if not final_model_rows_summary:
        return " ".join(detail_parts)

    detail_parts.append(f"models_final={','.join(final_model_rows_summary)}")
    return " ".join(detail_parts)


def validate_supported_dbt_models(models: list[str]) -> None:
    invalid_models = [
        model for model in models
        if not (
            model.startswith("ds_")
            or model.startswith("stg_ds__")
            or model.startswith("dim_")
        )
    ]

    if invalid_models:
        raise AirflowFailException(
            "Foram informados modelos dbt invalidos para a trilha de dimensoes: "
            f"{', '.join(invalid_models)}. "
            "Apenas prefixos ds_, stg_ds__ e dim_ sao permitidos nesta DAG."
        )


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
