from __future__ import annotations

import ast
import json
import logging
import time
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

from src.utils.airflow_helpers import audit_load_audit, load_all_client_ids, run_dbt_command

LOGGER = logging.getLogger(__name__)


def run_dbt(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}
    shared_models = parse_string_list(dag_conf.get("models"))
    client_models = parse_string_list(dag_conf.get("client_models"))
    if not shared_models and not client_models:
        shared_models = ["."]

    reconciliation_mode = str(
        dag_conf.get("reconciliation_mode")
        or context["params"].get("reconciliation_mode")
        or "incremental"
    ).strip().lower()
    if reconciliation_mode not in {"incremental", "full"}:
        reconciliation_mode = "incremental"

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

    if "reconciliation_mode" not in dbt_vars:
        dbt_vars["reconciliation_mode"] = reconciliation_mode

    full_refresh_raw = dag_conf.get("full_refresh")
    if isinstance(full_refresh_raw, str):
        full_refresh = full_refresh_raw.strip().lower() in {"true", "1", "yes", "y"}
    else:
        full_refresh = bool(full_refresh_raw)

    batch_id = dag_run.run_id
    source_name = "DBT"
    details = f"mode={reconciliation_mode} full_refresh={full_refresh}"
    continue_on_client_error = parse_bool(dag_conf.get("continue_on_client_error"), default=True)

    execution_summary: dict[str, Any] = {
        "status": "SUCCESS",
        "shared_models": shared_models,
        "client_models": client_models,
        "client_results": [],
    }

    if shared_models:
        execution_summary["shared_result"] = execute_dbt_step(
            batch_id=batch_id,
            step_name="DBT_RUN_SHARED",
            source_name=source_name,
            target_name=",".join(shared_models),
            details=details,
            select_models=shared_models,
            dbt_vars=dbt_vars,
            full_refresh=full_refresh,
            execution_order=1,
        )

    if not client_models:
        return execution_summary

    client_ids = resolve_client_ids(dag_conf, context["params"])
    client_failures: list[dict[str, Any]] = []

    for client_id in client_ids:
        client_dbt_vars = dict(dbt_vars)
        client_dbt_vars["id_cliente"] = client_id
        client_details = f"{details} id_cliente={client_id}"

        try:
            result = execute_dbt_step(
                batch_id=batch_id,
                step_name="DBT_RUN_CLIENT",
                source_name=source_name,
                target_name=",".join(client_models),
                details=client_details,
                select_models=client_models,
                dbt_vars=client_dbt_vars,
                full_refresh=full_refresh,
                execution_order=2,
                id_cliente=client_id,
            )
            execution_summary["client_results"].append(
                {"id_cliente": client_id, "status": "SUCCESS", "rows_processed": result["rows_processed"]}
            )
        except Exception as exc:
            LOGGER.exception("Falha no dbt por cliente. id_cliente=%s", client_id)
            execution_summary["client_results"].append(
                {"id_cliente": client_id, "status": "FAILED", "error": str(exc)}
            )
            client_failures.append({"id_cliente": client_id, "error": str(exc)})
            if not continue_on_client_error:
                raise

    if client_failures:
        execution_summary["status"] = "PARTIAL_FAILURE"
        failed_clients = ", ".join(str(item["id_cliente"]) for item in client_failures)
        raise AirflowFailException(
            "Processamento dbt finalizado com falhas por cliente. "
            f"clientes_falhos={failed_clients}"
        )

    return execution_summary


with DAG(
    dag_id="load_dw_dbt_dag",
    description="Executa apenas dbt para a carga DS -> DW.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["dbt", "dw"],
    default_args={"email_on_failure": False, "email_on_retry": False, "retries": 0},
    params={
        "reconciliation_mode": "incremental",
        "full_refresh": False,
        "dbt_vars": {},
        "models": [],
        "client_models": [],
        "continue_on_client_error": True,
        "watermark_id_clientes": None,
        "watermark_client_source_table": "",
        "watermark_client_id_column": "ID_CLIENTE",
        "watermark_client_active_column": "FL_ATIVO",
    },
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
    full_refresh: bool,
    execution_order: int,
    id_cliente: int | None = None,
) -> dict[str, Any]:
    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=batch_id,
        step_name=step_name,
        source_name=source_name,
        target_name=target_name,
        status="STARTED",
        id_cliente=id_cliente,
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
            full_refresh=full_refresh,
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
            id_cliente=id_cliente,
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
            id_cliente=id_cliente,
            details=str(exc),
            execution_order=execution_order,
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        raise


def resolve_client_ids(dag_conf: dict[str, Any], params: dict[str, Any]) -> list[int]:
    raw_client_ids = dag_conf.get("watermark_id_clientes")
    client_ids = parse_int_list(raw_client_ids)
    if client_ids:
        return client_ids

    client_source_table = dag_conf.get("watermark_client_source_table") or params.get("watermark_client_source_table")
    client_id_column = str(dag_conf.get("watermark_client_id_column") or params.get("watermark_client_id_column") or "ID_CLIENTE")
    client_active_column = dag_conf.get("watermark_client_active_column") or params.get("watermark_client_active_column")

    return load_all_client_ids(
        client_source_table=client_source_table,
        client_id_column=client_id_column,
        client_active_column=client_active_column,
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


def parse_int_list(raw_value: Any) -> list[int]:
    if raw_value is None or raw_value == "":
        return []
    if isinstance(raw_value, str) and raw_value.strip().lower() in {"none", "null", ""}:
        return []
    if isinstance(raw_value, int):
        return [raw_value]
    if isinstance(raw_value, list):
        return [int(item) for item in raw_value]
    if isinstance(raw_value, str):
        parsed_json = try_parse_json_list(raw_value)
        if parsed_json is not None:
            return [int(item) for item in parsed_json]
        parsed_python = try_parse_python_list(raw_value)
        if parsed_python is not None:
            return [int(item) for item in parsed_python]
        return [int(item.strip()) for item in raw_value.split(",") if item.strip()]
    return []


def parse_bool(raw_value: Any, *, default: bool) -> bool:
    if raw_value is None:
        return default
    if isinstance(raw_value, str):
        normalized = raw_value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
        return default
    return bool(raw_value)


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
