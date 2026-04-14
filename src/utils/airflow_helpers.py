from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import snowflake.connector
from airflow.exceptions import AirflowFailException
from cryptography.hazmat.primitives import serialization

LOGGER = logging.getLogger(__name__)


def ensure_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise AirflowFailException(
            f"Variavel de ambiente obrigatoria ausente para a DAG: {name}"
        )
    return value


def run_dbt_command(
    *,
    dbt_project_dir: str,
    dbt_profiles_dir: str,
    select_models: list[str],
    dbt_vars: dict[str, Any] | None = None,
    full_refresh: bool = False,
) -> dict[str, Any]:
    ensure_env_var("SNOWFLAKE_ACCOUNT")
    ensure_env_var("SNOWFLAKE_USER")
    ensure_env_var("SNOWFLAKE_WAREHOUSE")
    ensure_env_var("SNOWFLAKE_ROLE_DBT")
    ensure_env_var("SNOWFLAKE_PRIVATE_KEY_PATH")

    target_dir = Path(dbt_project_dir) / f"target_airflow_{uuid.uuid4().hex[:8]}"
    target_dir.mkdir(parents=True, exist_ok=True)

    command = ["dbt", "build", "--no-partial-parse", "--select", *select_models]
    if full_refresh:
        command.append("--full-refresh")
    if dbt_vars:
        command.extend(["--vars", json.dumps(dbt_vars)])
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_profiles_dir
    env["DBT_TARGET_PATH"] = str(target_dir)

    try:
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

        run_results_path = target_dir / "run_results.json"
        run_results = load_dbt_run_results(run_results_path)

        return {
            "status": "SUCCESS",
            "dbt_command": " ".join(command),
            "profiles_dir": dbt_profiles_dir,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "run_results": run_results,
        }
    finally:
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)


def run_extract_watermark_event(
    *,
    pipeline_name: str | None,
    id_clientes: list[int] | None,
    event_type: str,
    client_source_table: str | None = None,
    client_id_column: str = "CD_ID",
    client_active_column: str | None = "FG_ATIVO",
) -> dict[str, Any]:
    if not pipeline_name:
        return {
            "status": "SKIPPED",
            "reason": "watermark_pipeline_name nao informado",
            "event_type": event_type,
        }

    if event_type not in {"start", "end"}:
        raise AirflowFailException(f"Tipo de evento de extracao invalido: {event_type}")

    client_ids = (
        id_clientes
        if id_clientes is not None
        else load_all_client_ids(
            client_source_table=client_source_table,
            client_id_column=client_id_column,
            client_active_column=client_active_column,
        )
    )

    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        for id_cliente in client_ids:
            cursor.execute(build_extract_watermark_merge_sql(
                pipeline_name=pipeline_name,
                id_cliente=int(id_cliente),
                event_type=event_type,
            ))

        connection.commit()

        return {
            "status": "SUCCESS",
            "pipeline_name": pipeline_name,
            "id_clientes": client_ids,
            "event_type": event_type,
        }
    except Exception as exc:  # pragma: no cover - erro operacional
        raise AirflowFailException(
            f"Falha ao registrar watermark de extracao. "
            f"pipeline_name={pipeline_name} event_type={event_type} erro={exc}"
        ) from exc
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def run_airbyte_cloud_sync(
    *,
    connection_id: str,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 15,
) -> dict[str, Any]:
    client_id = ensure_env_var("AIRBYTE_CLOUD_CLIENT_ID")
    client_secret = ensure_env_var("AIRBYTE_CLOUD_CLIENT_SECRET")
    api_base_url = os.getenv("AIRBYTE_CLOUD_API_URL", "https://api.airbyte.com/v1").rstrip("/")

    access_token = get_airbyte_cloud_access_token(
        api_base_url=api_base_url,
        client_id=client_id,
        client_secret=client_secret,
    )

    job_id = trigger_airbyte_cloud_sync(
        api_base_url=api_base_url,
        access_token=access_token,
        connection_id=connection_id,
    )

    job_result = wait_for_airbyte_cloud_job(
        api_base_url=api_base_url,
        access_token=access_token,
        job_id=job_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )

    final_status = job_result.get("status")
    if final_status not in {"succeeded", "completed"}:
        raise AirflowFailException(
            f"Sync do Airbyte Cloud finalizou com status invalido. "
            f"connection_id={connection_id} job_id={job_id} status={final_status}"
        )

    return {
        "status": "SUCCESS",
        "connection_id": connection_id,
        "job_id": job_id,
        "job_status": final_status,
        "rows_processed": job_result.get("rows_processed"),
        "rows_synced": job_result.get("rows_synced"),
        "rows_emitted": job_result.get("rows_emitted"),
    }


def open_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    account = ensure_env_var("SNOWFLAKE_ACCOUNT")
    user = ensure_env_var("SNOWFLAKE_USER")
    warehouse = ensure_env_var("SNOWFLAKE_WAREHOUSE")
    role = ensure_env_var("SNOWFLAKE_ROLE_DBT")
    private_key_path = ensure_env_var("SNOWFLAKE_PRIVATE_KEY_PATH")
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")

    with open(private_key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account=account,
        user=user,
        private_key=pkb,
        warehouse=warehouse,
        role=role,
        database="SOLIX_BI",
        schema="DS",
        session_parameters={"TIMEZONE": "UTC"},
    )


def audit_load_audit(
    *,
    batch_id: str,
    step_name: str,
    source_name: str,
    target_name: str,
    status: str,
    rows_processed: int | None = None,
    details: str | None = None,
    id_cliente: int | None = None,
    execution_order: int | None = None,
    duration_seconds: int | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> None:
    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        escaped_step_name = step_name.replace("'", "''")
        escaped_source_name = source_name.replace("'", "''")
        escaped_target_name = target_name.replace("'", "''")
        escaped_status = status.replace("'", "''")
        escaped_details = details.replace("'", "''") if details else None
        dt_fim_value = "null" if status == "STARTED" else "current_date()"

        started_at_sql = f"'{started_at.isoformat()}'::timestamp_ntz" if started_at else "convert_timezone('UTC', current_timestamp())::timestamp_ntz"
        ended_at_sql = f"'{ended_at.isoformat()}'::timestamp_ntz" if ended_at else ("null" if status == "STARTED" else "convert_timezone('UTC', current_timestamp())::timestamp_ntz")

        cursor.execute(f"""
insert into SOLIX_BI.DS.CTL_LOAD_AUDIT (
    BATCH_ID,
    STEP_NAME,
    SOURCE_NAME,
    TARGET_NAME,
    STATUS,
    ROWS_PROCESSED,
    EVENT_TIME,
    DETAILS,
    ID_CLIENTE,
    DT_INICIO,
    DT_FIM,
    EXECUTION_ORDER,
    DURATION_SECONDS,
    STARTED_AT,
    ENDED_AT
) values (
    '{batch_id}',
    '{escaped_step_name}',
    '{escaped_source_name}',
    '{escaped_target_name}',
    '{escaped_status}',
    {rows_processed if rows_processed is not None else 'null'},
    convert_timezone('UTC', current_timestamp())::timestamp_ntz,
    {f"'{escaped_details}'" if escaped_details is not None else 'null'},
    {id_cliente if id_cliente is not None else 'null'},
    current_date(),
    {dt_fim_value},
    {execution_order if execution_order is not None else 'null'},
    {duration_seconds if duration_seconds is not None else 'null'},
    {started_at_sql},
    {ended_at_sql}
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao registrar auditoria de carga (CTL_LOAD_AUDIT).")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def audit_batch_execution_start(
    *,
    batch_id: str,
    pipeline_name: str,
    source_name: str,
    target_name: str,
    orchestration_type: str | None = None,
    id_cliente: int | None = None,
) -> None:
    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        escaped_batch_id = batch_id.replace("'", "''")
        escaped_pipeline_name = pipeline_name.replace("'", "''")
        escaped_source_name = source_name.replace("'", "''")
        escaped_target_name = target_name.replace("'", "''")
        escaped_orchestration_type = orchestration_type.replace("'", "''") if orchestration_type else None

        cursor.execute(f"""
merge into SOLIX_BI.DS.CTL_BATCH_EXECUTION as tgt
using (
    select
        '{escaped_batch_id}' as BATCH_ID,
        '{escaped_pipeline_name}' as PIPELINE_NAME,
        '{escaped_source_name}' as SOURCE_NAME,
        '{escaped_target_name}' as TARGET_NAME,
        'RUNNING' as STATUS,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as STARTED_AT,
        null as ENDED_AT,
        null as ROWS_EXTRACTED,
        null as ROWS_LOADED,
        null as ERROR_MESSAGE,
        {id_cliente if id_cliente is not None else 'null'} as ID_CLIENTE,
        current_date() as DT_INICIO,
        null as DT_FIM,
        null as DURATION_SECONDS,
        {f"'{escaped_orchestration_type}'" if escaped_orchestration_type else 'null'} as ORCHESTRATION_TYPE
) as src
on tgt.BATCH_ID = src.BATCH_ID
when matched then update set
    tgt.STATUS = src.STATUS,
    tgt.STARTED_AT = src.STARTED_AT,
    tgt.ERROR_MESSAGE = src.ERROR_MESSAGE,
    tgt.ID_CLIENTE = src.ID_CLIENTE,
    tgt.DT_INICIO = src.DT_INICIO,
    tgt.ORCHESTRATION_TYPE = src.ORCHESTRATION_TYPE
when not matched then insert (
    BATCH_ID,
    PIPELINE_NAME,
    SOURCE_NAME,
    TARGET_NAME,
    STATUS,
    STARTED_AT,
    ENDED_AT,
    ROWS_EXTRACTED,
    ROWS_LOADED,
    ERROR_MESSAGE,
    ID_CLIENTE,
    DT_INICIO,
    DT_FIM,
    DURATION_SECONDS,
    ORCHESTRATION_TYPE
) values (
    src.BATCH_ID,
    src.PIPELINE_NAME,
    src.SOURCE_NAME,
    src.TARGET_NAME,
    src.STATUS,
    src.STARTED_AT,
    src.ENDED_AT,
    src.ROWS_EXTRACTED,
    src.ROWS_LOADED,
    src.ERROR_MESSAGE,
    src.ID_CLIENTE,
    src.DT_INICIO,
    src.DT_FIM,
    src.DURATION_SECONDS,
    src.ORCHESTRATION_TYPE
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao registrar batch start (CTL_BATCH_EXECUTION).")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def audit_batch_execution_end(
    *,
    batch_id: str,
    pipeline_name: str,
    source_name: str,
    target_name: str,
    status: str,
    error_message: str | None = None,
    rows_extracted: int | None = None,
    rows_loaded: int | None = None,
    duration_seconds: int | None = None,
    id_cliente: int | None = None,
) -> None:
    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        escaped_batch_id = batch_id.replace("'", "''")
        escaped_pipeline_name = pipeline_name.replace("'", "''")
        escaped_source_name = source_name.replace("'", "''")
        escaped_target_name = target_name.replace("'", "''")
        escaped_status = status.replace("'", "''")
        escaped_error_message = error_message.replace("'", "''") if error_message else None

        cursor.execute(f"""
merge into SOLIX_BI.DS.CTL_BATCH_EXECUTION as tgt
using (
    select
        '{escaped_batch_id}' as BATCH_ID,
        '{escaped_pipeline_name}' as PIPELINE_NAME,
        '{escaped_source_name}' as SOURCE_NAME,
        '{escaped_target_name}' as TARGET_NAME,
        '{escaped_status}' as STATUS,
        null as STARTED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as ENDED_AT,
        {rows_extracted if rows_extracted is not None else 'null'} as ROWS_EXTRACTED,
        {rows_loaded if rows_loaded is not None else 'null'} as ROWS_LOADED,
        {f"'{escaped_error_message}'" if escaped_error_message is not None else 'null'} as ERROR_MESSAGE,
        {id_cliente if id_cliente is not None else 'null'} as ID_CLIENTE,
        null as DT_INICIO,
        current_date() as DT_FIM,
        {duration_seconds if duration_seconds is not None else 'null'} as DURATION_SECONDS,
        null as ORCHESTRATION_TYPE
) as src
on tgt.BATCH_ID = src.BATCH_ID
when matched then update set
    tgt.STATUS = src.STATUS,
    tgt.ENDED_AT = src.ENDED_AT,
    tgt.ROWS_EXTRACTED = src.ROWS_EXTRACTED,
    tgt.ROWS_LOADED = src.ROWS_LOADED,
    tgt.ERROR_MESSAGE = src.ERROR_MESSAGE,
    tgt.ID_CLIENTE = src.ID_CLIENTE,
    tgt.DT_FIM = src.DT_FIM,
    tgt.DURATION_SECONDS = src.DURATION_SECONDS
when not matched then insert (
    BATCH_ID,
    PIPELINE_NAME,
    SOURCE_NAME,
    TARGET_NAME,
    STATUS,
    STARTED_AT,
    ENDED_AT,
    ROWS_EXTRACTED,
    ROWS_LOADED,
    ERROR_MESSAGE,
    ID_CLIENTE,
    DT_INICIO,
    DT_FIM,
    DURATION_SECONDS,
    ORCHESTRATION_TYPE
) values (
    src.BATCH_ID,
    src.PIPELINE_NAME,
    src.SOURCE_NAME,
    src.TARGET_NAME,
    src.STATUS,
    src.STARTED_AT,
    src.ENDED_AT,
    src.ROWS_EXTRACTED,
    src.ROWS_LOADED,
    src.ERROR_MESSAGE,
    src.ID_CLIENTE,
    src.DT_INICIO,
    src.DT_FIM,
    src.DURATION_SECONDS,
    src.ORCHESTRATION_TYPE
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao registrar batch end (CTL_BATCH_EXECUTION).")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()



def load_all_client_ids(
    *,
    client_source_table: str | None,
    client_id_column: str,
    client_active_column: str | None,
) -> list[int]:
    if not client_source_table:
        return [0]

    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()
        cursor.execute(build_all_clients_sql(
            client_source_table=client_source_table,
            client_id_column=client_id_column,
            client_active_column=client_active_column,
        ))
        rows = cursor.fetchall()
        client_ids = [int(row[0]) for row in rows if row and row[0] is not None]
        return client_ids or [0]
    except Exception as exc:  # pragma: no cover - erro operacional
        raise AirflowFailException(
            f"Falha ao carregar clientes para watermark. "
            f"table={client_source_table} coluna={client_id_column} erro={exc}"
        ) from exc
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def build_all_clients_sql(
    *,
    client_source_table: str,
    client_id_column: str,
    client_active_column: str | None,
) -> str:
    active_filter = ""
    if client_active_column:
        active_filter = f"""
where upper(cast({client_active_column} as varchar)) in ('1', 'TRUE', 'T', 'ATIVO', 'A')
"""

    return f"""
select distinct {client_id_column}
from {client_source_table}
{active_filter}
order by {client_id_column}
"""


def get_airbyte_cloud_access_token(
    *,
    api_base_url: str,
    client_id: str,
    client_secret: str,
) -> str:
    response = requests.post(
        f"{api_base_url}/applications/token",
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant-type": "client_credentials",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = payload.get("access_token")

    if not access_token:
        raise AirflowFailException("Nao foi possivel obter access_token do Airbyte Cloud.")

    return str(access_token)


def trigger_airbyte_cloud_sync(
    *,
    api_base_url: str,
    access_token: str,
    connection_id: str,
) -> int:
    response = requests.post(
        f"{api_base_url}/jobs",
        headers={"Authorization": f"Bearer {access_token}"},
        json={"connectionId": connection_id, "jobType": "sync"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    job_id = payload.get("jobId") or payload.get("job", {}).get("jobId") or payload.get("job", {}).get("id") or payload.get("id")

    if job_id is None:
        raise AirflowFailException(
            f"Resposta inesperada ao disparar sync do Airbyte Cloud: {payload}"
        )

    return int(job_id)


def wait_for_airbyte_cloud_job(
    *,
    api_base_url: str,
    access_token: str,
    job_id: int,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        response = requests.get(
            f"{api_base_url}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        status = (
            payload.get("status")
            or payload.get("job", {}).get("status")
            or payload.get("job", {}).get("job", {}).get("status")
        )
        normalized_status = str(status or "").strip().lower()

        if normalized_status in {"running", "pending", "incomplete", "queued"}:
            time.sleep(poll_interval_seconds)
            continue

        if normalized_status in {"succeeded", "completed"}:
            details = parse_airbyte_cloud_job_details(payload)
            return {"status": normalized_status, **details}

        if normalized_status in {"failed", "cancelled", "canceled"}:
            raise AirflowFailException(
                f"Job do Airbyte Cloud falhou. job_id={job_id} status={normalized_status}"
            )

        time.sleep(poll_interval_seconds)

    raise AirflowFailException(
        f"Timeout aguardando job do Airbyte Cloud. job_id={job_id} timeout_seconds={timeout_seconds}"
    )


def parse_airbyte_cloud_job_details(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"rows_processed": None, "rows_synced": None, "rows_emitted": None}

    job_payload = payload.get("job") if "job" in payload else payload
    if isinstance(job_payload, dict) and "job" in job_payload:
        job_payload = job_payload.get("job") or job_payload

    attempts = job_payload.get("attempts")
    if isinstance(attempts, dict):
        attempts = [attempts]

    last_attempt = None
    if isinstance(attempts, list) and attempts:
        last_attempt = attempts[-1]
    elif isinstance(job_payload.get("attempt"), dict):
        last_attempt = job_payload.get("attempt")
    else:
        last_attempt = job_payload

    sync_stats = {}
    if isinstance(last_attempt, dict):
        sync_stats = (
            last_attempt.get("syncStats")
            or last_attempt.get("sync_stats")
            or {}
        )

    def parse_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    rows_synced = parse_int(
        sync_stats.get("records_synced")
        or sync_stats.get("recordsSynced")
        or sync_stats.get("records_synced")
    )
    rows_emitted = parse_int(
        sync_stats.get("records_emitted")
        or sync_stats.get("recordsEmitted")
    )
    rows_extracted = parse_int(
        sync_stats.get("records_extracted")
        or sync_stats.get("recordsExtracted")
    )

    rows_processed = rows_synced if rows_synced is not None else rows_emitted if rows_emitted is not None else rows_extracted

    return {
        "rows_processed": rows_processed,
        "rows_synced": rows_synced,
        "rows_emitted": rows_emitted,
        "rows_extracted": rows_extracted,
    }


def build_extract_watermark_merge_sql(
    *,
    pipeline_name: str,
    id_cliente: int,
    event_type: str,
) -> str:
    timestamp_column = (
        "LAST_EXTRACT_STARTED_AT" if event_type == "start" else "LAST_EXTRACT_ENDED_AT"
    )

    escaped_pipeline_name = pipeline_name.replace("'", "''")

    return f"""
merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
using (
    select
        '{escaped_pipeline_name}' as PIPELINE_NAME,
        {id_cliente} as ID_CLIENTE,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as {timestamp_column},
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.PIPELINE_NAME = src.PIPELINE_NAME
and tgt.ID_CLIENTE = src.ID_CLIENTE
when matched then update set
    tgt.{timestamp_column} = src.{timestamp_column},
    tgt.UPDATED_AT = src.UPDATED_AT
when not matched then insert (
    PIPELINE_NAME,
    ID_CLIENTE,
    {timestamp_column},
    UPDATED_AT
) values (
    src.PIPELINE_NAME,
    src.ID_CLIENTE,
    src.{timestamp_column},
    src.UPDATED_AT
)
"""


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
