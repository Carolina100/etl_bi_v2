from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException


def ensure_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise AirflowFailException(
            f"Variavel de ambiente obrigatoria ausente para a DAG: {name}"
        )
    return value


def normalize_client_conf(
    raw_conf: dict[str, Any],
    *,
    allow_missing_client: bool = False,
) -> dict[str, Any]:
    id_clientes = raw_conf.get("id_clientes")
    id_cliente = raw_conf.get("id_cliente")

    if id_clientes and id_cliente:
        raise AirflowFailException(
            "Informe apenas um entre 'id_cliente' ou 'id_clientes'."
        )

    if id_clientes:
        if not isinstance(id_clientes, list) or not id_clientes:
            raise AirflowFailException("'id_clientes' deve ser uma lista nao vazia.")
        normalized_ids = [int(item) for item in id_clientes]
    elif id_cliente is not None:
        normalized_ids = [int(id_cliente)]
    else:
        if allow_missing_client:
            normalized_ids = []
        else:
            raise AirflowFailException(
                "Informe 'id_cliente' ou 'id_clientes' no dag_run.conf."
            )

    data_inicio = raw_conf.get("data_inicio")
    data_fim = raw_conf.get("data_fim")
    manual_window_informed = data_inicio is not None or data_fim is not None
    if manual_window_informed and not (data_inicio and data_fim):
        raise AirflowFailException(
            "Informe 'data_inicio' e 'data_fim' juntos para backfill manual."
        )

    load_mode = "MANUAL_BACKFILL" if manual_window_informed else "INCREMENTAL_WATERMARK"

    return {
        "id_clientes": normalized_ids,
        "data_inicio": str(data_inicio) if data_inicio else None,
        "data_fim": str(data_fim) if data_fim else None,
        "load_mode": load_mode,
    }


def run_dbt_command(
    *,
    dbt_project_dir: str,
    dbt_profiles_dir: str,
    select_models: list[str],
) -> dict[str, Any]:
    ensure_env_var("SNOWFLAKE_ACCOUNT")
    ensure_env_var("SNOWFLAKE_USER")
    ensure_env_var("SNOWFLAKE_WAREHOUSE")
    ensure_env_var("SNOWFLAKE_ROLE_DBT")
    ensure_env_var("SNOWFLAKE_PRIVATE_KEY_PATH")

    target_dir = Path(dbt_project_dir) / "target"
    if target_dir.exists():
        shutil.rmtree(target_dir)

    command = ["dbt", "build", "--no-partial-parse", "--select", *select_models]
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_profiles_dir

    completed = subprocess.run(
        command,
        cwd=dbt_project_dir,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.returncode != 0:
        raise AirflowFailException(
            "Falha na camada DW. "
            f"returncode={completed.returncode}. "
            f"stdout={completed.stdout.strip()} "
            f"stderr={completed.stderr.strip()}"
        )

    run_results_path = Path(dbt_project_dir) / "target" / "run_results.json"
    run_results = load_dbt_run_results(run_results_path)

    return {
        "status": "SUCCESS",
        "dbt_command": " ".join(command),
        "profiles_dir": dbt_profiles_dir,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "run_results": run_results,
    }


def load_dbt_run_results(run_results_path: Path) -> dict[str, Any]:
    if not run_results_path.exists():
        raise AirflowFailException(
            f"Arquivo de resultado do dbt nao encontrado: {run_results_path}"
        )

    with run_results_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    results = payload.get("results", [])
    model_results: list[dict[str, Any]] = []
    test_results: list[dict[str, Any]] = []

    for result in results:
        unique_id = str(result.get("unique_id", ""))
        resource_type = unique_id.split(".", maxsplit=1)[0] if "." in unique_id else unique_id
        adapter_response = result.get("adapter_response") or {}

        parsed_result = {
            "unique_id": unique_id,
            "resource_type": resource_type,
            "status": result.get("status"),
            "message": result.get("message"),
            "execution_time": result.get("execution_time"),
            "rows_affected": int(adapter_response.get("rows_affected") or 0),
            "rows_inserted": int(adapter_response.get("rows_inserted") or 0),
            "rows_updated": int(adapter_response.get("rows_updated") or 0),
            "rows_deleted": int(adapter_response.get("rows_deleted") or 0),
            "query_id": adapter_response.get("query_id"),
        }

        if resource_type == "model":
            model_results.append(parsed_result)
        elif resource_type == "test":
            test_results.append(parsed_result)

    return {
        "generated_at": payload.get("metadata", {}).get("generated_at"),
        "dbt_version": payload.get("metadata", {}).get("dbt_version"),
        "model_results": model_results,
        "test_results": test_results,
        "model_count": len(model_results),
        "test_count": len(test_results),
        "rows_inserted_total": sum(result["rows_inserted"] for result in model_results),
        "rows_updated_total": sum(result["rows_updated"] for result in model_results),
        "rows_deleted_total": sum(result["rows_deleted"] for result in model_results),
        "rows_affected_total": sum(result["rows_affected"] for result in model_results),
    }
