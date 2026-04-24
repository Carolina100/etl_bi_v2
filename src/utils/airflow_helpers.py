from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import snowflake.connector
from airflow.exceptions import AirflowFailException
from cryptography.hazmat.primitives import serialization

LOGGER = logging.getLogger(__name__)

# ============================================================================
# CONVENCAO DE STATUS
# ----------------------------------------------------------------------------
# Todos os campos de status nas tabelas CTL devem usar exclusivamente
# os valores definidos abaixo. Nao usar strings literais avulsas no codigo.
#
# CTL_PIPELINE_WATERMARK.LAST_RUN_STATUS:
#   PIPELINE_STATUS_RUNNING  -> pipeline em execucao
#   PIPELINE_STATUS_SUCCESS  -> concluido com sucesso (Airflow ou dbt post_hook)
#   PIPELINE_STATUS_FAILED   -> falhou
#
# CTL_BATCH_EXECUTION.STATUS:
#   PIPELINE_STATUS_RUNNING  -> batch registrado e em execucao
#   PIPELINE_STATUS_SUCCESS  -> batch concluido com sucesso
#   PIPELINE_STATUS_FAILED   -> batch concluido com falha
#
# CTL_LOAD_AUDIT.STATUS:
#   AUDIT_STATUS_STARTED -> etapa iniciada (sem ended_at)
#   AUDIT_STATUS_SUCCESS -> etapa concluida com sucesso
#   AUDIT_STATUS_FAILED  -> etapa concluida com falha
# ============================================================================
PIPELINE_STATUS_RUNNING: str = "RUNNING"
PIPELINE_STATUS_SUCCESS: str = "SUCCESS"
PIPELINE_STATUS_FAILED: str = "FAILED"

AUDIT_STATUS_STARTED: str = "STARTED"
AUDIT_STATUS_SUCCESS: str = "SUCCESS"
AUDIT_STATUS_FAILED: str = "FAILED"

# Janela padrao (minutos) apos a qual um pipeline em RUNNING e considerado travado.
DEFAULT_STALL_THRESHOLD_MINUTES: int = 90


def should_fail_on_audit_error() -> bool:
    """
    Modo padrao: fail-closed em falhas de auditoria.

    Para troubleshooting emergencial, e possivel liberar fail-open definindo:
    AIRFLOW_AUDIT_FAIL_OPEN=true
    """
    raw_value = str(os.getenv("AIRFLOW_AUDIT_FAIL_OPEN", "")).strip().lower()
    return raw_value not in {"1", "true", "yes", "on"}


def ensure_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise AirflowFailException(
            f"Variavel de ambiente obrigatoria ausente para a DAG: {name}"
        )
    return value


def send_operational_alert(
    *,
    title: str,
    message: str,
    severity: str = "ERROR",
) -> None:
    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL")
    formatted_message = f"[{severity}] {title}\n{message}"

    if not webhook_url:
        LOGGER.warning("Alerta operacional sem webhook configurado: %s", formatted_message)
        return

    try:
        response = requests.post(
            webhook_url,
            json={"text": formatted_message},
            timeout=15,
        )
        response.raise_for_status()
    except Exception:
        LOGGER.exception("Falha ao enviar alerta operacional para o webhook configurado.")


def build_airflow_alert_message(context: dict[str, Any], *, event_type: str) -> tuple[str, str]:
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = dag.dag_id if dag is not None else context.get("dag_id", "unknown_dag")
    task_id = task_instance.task_id if task_instance is not None else "unknown_task"
    run_id = dag_run.run_id if dag_run is not None else "unknown_run"
    try_number = task_instance.try_number if task_instance is not None else "unknown_try"
    log_url = getattr(task_instance, "log_url", None)

    title = f"Airflow {event_type}: {dag_id}.{task_id}"
    message_lines = [
        f"dag_id={dag_id}",
        f"task_id={task_id}",
        f"run_id={run_id}",
        f"try_number={try_number}",
    ]

    if exception is not None:
        message_lines.append(f"exception={exception}")
    if log_url:
        message_lines.append(f"log_url={log_url}")

    return title, "\n".join(message_lines)


def airflow_failure_alert_callback(context: dict[str, Any]) -> None:
    title, message = build_airflow_alert_message(context, event_type="FAILURE")
    send_operational_alert(title=title, message=message, severity="ERROR")


def airflow_retry_alert_callback(context: dict[str, Any]) -> None:
    title, message = build_airflow_alert_message(context, event_type="RETRY")
    send_operational_alert(title=title, message=message, severity="WARNING")


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

    project_path = Path(dbt_project_dir)
    clear_stale_dbt_artifacts(project_path)

    target_dir = project_path / f"target_airflow_{uuid.uuid4().hex[:8]}"
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

        run_results_path = target_dir / "run_results.json"
        run_results = load_dbt_run_results(run_results_path) if run_results_path.exists() else {}

        return {
            "status": PIPELINE_STATUS_SUCCESS if completed.returncode == 0 else PIPELINE_STATUS_FAILED,
            "returncode": completed.returncode,
            "dbt_command": " ".join(command),
            "profiles_dir": dbt_profiles_dir,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "run_results": run_results,
        }
    finally:
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)


def clear_stale_dbt_artifacts(project_path: Path) -> None:
    """
    Remove apenas artefatos temporarios antigos criados por esta integracao.

    Nao apagamos mais `target/` e `logs/` globais do dbt para evitar condicao de corrida
    quando duas execucoes coexistem no mesmo filesystem de worker.
    """
    now_utc = datetime.now(timezone.utc)
    max_age = timedelta(hours=24)
    for temp_target in project_path.glob("target_airflow_*"):
        try:
            if not temp_target.is_dir():
                continue
            modified_at = datetime.fromtimestamp(temp_target.stat().st_mtime, tz=timezone.utc)
            if now_utc - modified_at > max_age:
                shutil.rmtree(temp_target, ignore_errors=True)
        except Exception:
            LOGGER.warning(
                "Falha ao limpar artefato temporario antigo do dbt: %s",
                temp_target,
                exc_info=True,
            )


def run_airbyte_connection_sync(
    *,
    connection_id: str,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 15,
) -> dict[str, Any]:
    if os.getenv("AIRBYTE_CLOUD_CLIENT_ID") and os.getenv("AIRBYTE_CLOUD_CLIENT_SECRET"):
        return run_airbyte_cloud_sync(
            connection_id=connection_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )

    return run_airbyte_self_hosted_sync(
        connection_id=connection_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


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
        connection_id=connection_id,
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
        "rows_extracted": job_result.get("rows_extracted"),
        "rows_committed": job_result.get("rows_committed"),
        "rows_stream_stats_total": job_result.get("rows_stream_stats_total"),
        "rows_metric_source": job_result.get("rows_metric_source"),
        "rows_emitted": job_result.get("rows_emitted"),
        "stream_stats_summary": job_result.get("stream_stats_summary"),
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
    {execution_order if execution_order is not None else 'null'},
    {duration_seconds if duration_seconds is not None else 'null'},
    {started_at_sql},
    {ended_at_sql}
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao registrar auditoria de carga (CTL_LOAD_AUDIT).")
        if should_fail_on_audit_error():
            raise AirflowFailException(
                "Falha critica ao registrar CTL_LOAD_AUDIT em modo fail-closed."
            ) from exc
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
        '{PIPELINE_STATUS_RUNNING}' as STATUS,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as STARTED_AT,
        null as ENDED_AT,
        null as ROWS_EXTRACTED,
        null as ROWS_LOADED,
        null as ROWS_AFFECTED,
        null as ERROR_MESSAGE,
        null as DURATION_SECONDS,
        {f"'{escaped_orchestration_type}'" if escaped_orchestration_type else 'null'} as ORCHESTRATION_TYPE,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.BATCH_ID = src.BATCH_ID
when matched then update set
    tgt.STATUS = src.STATUS,
    tgt.STARTED_AT = src.STARTED_AT,
    tgt.ERROR_MESSAGE = src.ERROR_MESSAGE,
    tgt.ORCHESTRATION_TYPE = src.ORCHESTRATION_TYPE,
    tgt.UPDATED_AT = src.UPDATED_AT
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
    ROWS_AFFECTED,
    ERROR_MESSAGE,
    DURATION_SECONDS,
    ORCHESTRATION_TYPE,
    UPDATED_AT
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
    src.ROWS_AFFECTED,
    src.ERROR_MESSAGE,
    src.DURATION_SECONDS,
    src.ORCHESTRATION_TYPE,
    src.UPDATED_AT
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao registrar batch start (CTL_BATCH_EXECUTION).")
        if should_fail_on_audit_error():
            raise AirflowFailException(
                "Falha critica ao registrar inicio em CTL_BATCH_EXECUTION em modo fail-closed."
            ) from exc
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
    rows_affected: int | None = None,
    duration_seconds: int | None = None,
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
        {rows_affected if rows_affected is not None else 'null'} as ROWS_AFFECTED,
        {f"'{escaped_error_message}'" if escaped_error_message is not None else 'null'} as ERROR_MESSAGE,
        {duration_seconds if duration_seconds is not None else 'null'} as DURATION_SECONDS,
        null as ORCHESTRATION_TYPE,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.BATCH_ID = src.BATCH_ID
when matched then update set
    tgt.STATUS = src.STATUS,
    tgt.ENDED_AT = src.ENDED_AT,
    tgt.ROWS_EXTRACTED = src.ROWS_EXTRACTED,
    tgt.ROWS_LOADED = src.ROWS_LOADED,
    tgt.ROWS_AFFECTED = src.ROWS_AFFECTED,
    tgt.ERROR_MESSAGE = src.ERROR_MESSAGE,
    tgt.DURATION_SECONDS = src.DURATION_SECONDS,
    tgt.UPDATED_AT = src.UPDATED_AT
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
    ROWS_AFFECTED,
    ERROR_MESSAGE,
    DURATION_SECONDS,
    ORCHESTRATION_TYPE,
    UPDATED_AT
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
    src.ROWS_AFFECTED,
    src.ERROR_MESSAGE,
    src.DURATION_SECONDS,
    src.ORCHESTRATION_TYPE,
    src.UPDATED_AT
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao registrar batch end (CTL_BATCH_EXECUTION).")
        if should_fail_on_audit_error():
            raise AirflowFailException(
                "Falha critica ao registrar fim em CTL_BATCH_EXECUTION em modo fail-closed."
            ) from exc
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def mark_pipeline_watermark_failure(
    *,
    pipeline_name: str | None,
    batch_id: str,
    error_message: str,
) -> None:
    if not pipeline_name:
        return

    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        escaped_pipeline_name = pipeline_name.replace("'", "''")
        escaped_batch_id = batch_id.replace("'", "''")
        escaped_error_message = error_message.replace("'", "''")

        cursor.execute(f"""
merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
using (
    select
        '{escaped_pipeline_name}' as PIPELINE_NAME,
        '{escaped_batch_id}' as LAST_RUN_BATCH_ID,
        '{PIPELINE_STATUS_FAILED}' as LAST_RUN_STATUS,
        '{escaped_error_message}' as LAST_ERROR_MESSAGE,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.PIPELINE_NAME = src.PIPELINE_NAME
when matched then update set
    tgt.LAST_RUN_BATCH_ID = src.LAST_RUN_BATCH_ID,
    tgt.LAST_RUN_STATUS = src.LAST_RUN_STATUS,
    tgt.LAST_ERROR_MESSAGE = src.LAST_ERROR_MESSAGE,
    tgt.UPDATED_AT = src.UPDATED_AT
when not matched then insert (
    PIPELINE_NAME,
    LAST_RUN_BATCH_ID,
    LAST_RUN_STATUS,
    LAST_ERROR_MESSAGE,
    UPDATED_AT
) values (
    src.PIPELINE_NAME,
    src.LAST_RUN_BATCH_ID,
    src.LAST_RUN_STATUS,
    src.LAST_ERROR_MESSAGE,
    src.UPDATED_AT
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao marcar watermark como FAILED.")
        if should_fail_on_audit_error():
            raise AirflowFailException(
                "Falha critica ao marcar CTL_PIPELINE_WATERMARK como FAILED em modo fail-closed."
            ) from exc
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def mark_pipeline_watermark_running(
    *,
    pipeline_name: str | None,
    batch_id: str,
) -> None:
    if not pipeline_name:
        return

    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        escaped_pipeline_name = pipeline_name.replace("'", "''")
        escaped_batch_id = batch_id.replace("'", "''")

        cursor.execute(f"""
merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
using (
    select
        '{escaped_pipeline_name}' as PIPELINE_NAME,
        '{escaped_batch_id}' as LAST_RUN_BATCH_ID,
        '{PIPELINE_STATUS_RUNNING}' as LAST_RUN_STATUS,
        null as LAST_ERROR_MESSAGE,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_RUN_STARTED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
) as src
on tgt.PIPELINE_NAME = src.PIPELINE_NAME
when matched then update set
    tgt.LAST_RUN_BATCH_ID = src.LAST_RUN_BATCH_ID,
    tgt.LAST_RUN_STATUS = src.LAST_RUN_STATUS,
    tgt.LAST_ERROR_MESSAGE = src.LAST_ERROR_MESSAGE,
    tgt.LAST_RUN_STARTED_AT = src.LAST_RUN_STARTED_AT,
    tgt.UPDATED_AT = src.UPDATED_AT
when not matched then insert (
    PIPELINE_NAME,
    LAST_RUN_BATCH_ID,
    LAST_RUN_STATUS,
    LAST_ERROR_MESSAGE,
    LAST_RUN_STARTED_AT,
    UPDATED_AT
) values (
    src.PIPELINE_NAME,
    src.LAST_RUN_BATCH_ID,
    src.LAST_RUN_STATUS,
    src.LAST_ERROR_MESSAGE,
    src.LAST_RUN_STARTED_AT,
    src.UPDATED_AT
)
""")
        connection.commit()
    except Exception as exc:
        LOGGER.exception("Falha ao marcar watermark como RUNNING.")
        if should_fail_on_audit_error():
            raise AirflowFailException(
                "Falha critica ao marcar CTL_PIPELINE_WATERMARK como RUNNING em modo fail-closed."
            ) from exc
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
        return client_ids
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


def run_airbyte_self_hosted_sync(
    *,
    connection_id: str,
    timeout_seconds: int = 3600,
    poll_interval_seconds: int = 15,
) -> dict[str, Any]:
    api_base_url = ensure_env_var("AIRBYTE_API_URL").rstrip("/")

    job_id = trigger_airbyte_self_hosted_sync(
        api_base_url=api_base_url,
        connection_id=connection_id,
    )

    job_result = wait_for_airbyte_self_hosted_job(
        api_base_url=api_base_url,
        connection_id=connection_id,
        job_id=job_id,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )

    final_status = job_result.get("status")
    if final_status not in {"succeeded", "completed"}:
        raise AirflowFailException(
            f"Sync do Airbyte self-hosted finalizou com status invalido. "
            f"connection_id={connection_id} job_id={job_id} status={final_status}"
        )

    return {
        "status": "SUCCESS",
        "connection_id": connection_id,
        "job_id": job_id,
        "job_status": final_status,
        "rows_processed": job_result.get("rows_processed"),
        "rows_synced": job_result.get("rows_synced"),
        "rows_extracted": job_result.get("rows_extracted"),
        "rows_committed": job_result.get("rows_committed"),
        "rows_stream_stats_total": job_result.get("rows_stream_stats_total"),
        "rows_metric_source": job_result.get("rows_metric_source"),
        "rows_emitted": job_result.get("rows_emitted"),
        "stream_stats_summary": job_result.get("stream_stats_summary"),
    }


def build_airbyte_self_hosted_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_token = os.getenv("AIRBYTE_API_TOKEN")
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    return headers


def trigger_airbyte_self_hosted_sync(
    *,
    api_base_url: str,
    connection_id: str,
) -> int:
    response = requests.post(
        f"{api_base_url}/connections/sync",
        headers=build_airbyte_self_hosted_headers(),
        json={"connectionId": connection_id},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    job_payload = payload.get("job") if isinstance(payload.get("job"), dict) else payload
    job_id = job_payload.get("id") or job_payload.get("jobId") or payload.get("jobId")

    if job_id is None:
        raise AirflowFailException(
            f"Resposta inesperada ao disparar sync do Airbyte self-hosted: {payload}"
        )

    return int(job_id)


def wait_for_airbyte_self_hosted_job(
    *,
    api_base_url: str,
    connection_id: str,
    job_id: int,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        payload = get_airbyte_self_hosted_job_payload(
            api_base_url=api_base_url,
            job_id=job_id,
        )

        status = (
            payload.get("status")
            or payload.get("job", {}).get("status")
            or payload.get("job", {}).get("job", {}).get("status")
        )
        normalized_status = str(status or "").strip().lower()

        if normalized_status in {"running", "pending", "queued", "incomplete"}:
            time.sleep(poll_interval_seconds)
            continue

        if normalized_status in {"succeeded", "completed"}:
            details = parse_airbyte_cloud_job_details(payload)
            if details.get("rows_processed") is None:
                LOGGER.warning(
                    "Airbyte self-hosted job sem rows_processed detectado. "
                    "job_id=%s payload=%s",
                    job_id,
                    json.dumps(payload, ensure_ascii=True, default=str)[:12000],
                )
            return {"status": normalized_status, **details}

        if normalized_status in {"failed", "cancelled", "canceled"}:
            raise AirflowFailException(
                f"Job do Airbyte self-hosted falhou. "
                f"connection_id={connection_id} job_id={job_id} status={normalized_status}"
            )

        time.sleep(poll_interval_seconds)

    last_status = "unknown"
    try:
        payload = get_airbyte_self_hosted_job_payload(
            api_base_url=api_base_url,
            job_id=job_id,
        )
        raw_status = (
            payload.get("status")
            or payload.get("job", {}).get("status")
            or payload.get("job", {}).get("job", {}).get("status")
        )
        last_status = str(raw_status or "").strip().lower() or "unknown"
    except Exception:
        LOGGER.exception("Falha ao consultar status final do job do Airbyte self-hosted apos timeout.")

    raise AirflowFailException(
        "Timeout aguardando job do Airbyte self-hosted. "
        f"connection_id={connection_id} job_id={job_id} "
        f"timeout_seconds={timeout_seconds} last_status={last_status}. "
        "Acao recomendada: verificar o job no Airbyte antes de relancar a DAG."
    )


def get_airbyte_self_hosted_job_payload(
    *,
    api_base_url: str,
    job_id: int,
) -> dict[str, Any]:
    candidate_urls = build_airbyte_self_hosted_job_urls(
        api_base_url=api_base_url,
        job_id=job_id,
    )

    last_error: Exception | None = None
    for method, url, payload in candidate_urls:
        try:
            if method == "GET":
                response = requests.get(
                    url,
                    headers=build_airbyte_self_hosted_headers(),
                    timeout=30,
                )
            else:
                response = requests.post(
                    url,
                    headers=build_airbyte_self_hosted_headers(),
                    json=payload,
                    timeout=30,
                )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                continue
            raise
        except Exception as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error

    raise AirflowFailException(
        f"Nao foi possivel consultar detalhes do job do Airbyte. job_id={job_id}"
    )


def build_airbyte_self_hosted_job_urls(
    *,
    api_base_url: str,
    job_id: int,
) -> list[tuple[str, str, dict[str, Any] | None]]:
    normalized_base_url = api_base_url.rstrip("/")
    candidate_urls: list[tuple[str, str, dict[str, Any] | None]] = []

    if normalized_base_url.endswith("/api/v1"):
        v1_base_url = normalized_base_url
    else:
        v1_base_url = f"{normalized_base_url}/v1"

    candidate_urls.append(("GET", f"{v1_base_url}/jobs/{job_id}", None))
    candidate_urls.append(("POST", f"{normalized_base_url}/jobs/get", {"id": job_id}))
    return candidate_urls


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
    connection_id: str,
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
            if details.get("rows_processed") is None:
                LOGGER.warning(
                    "Airbyte cloud job sem rows_processed detectado. "
                    "job_id=%s payload=%s",
                    job_id,
                    json.dumps(payload, ensure_ascii=True, default=str)[:12000],
                )
            return {"status": normalized_status, **details}

        if normalized_status in {"failed", "cancelled", "canceled"}:
            raise AirflowFailException(
                f"Job do Airbyte Cloud falhou. "
                f"connection_id={connection_id} job_id={job_id} status={normalized_status}"
            )

        time.sleep(poll_interval_seconds)

    last_status = None
    try:
        response = requests.get(
            f"{api_base_url}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        raw_status = (
            payload.get("status")
            or payload.get("job", {}).get("status")
            or payload.get("job", {}).get("job", {}).get("status")
        )
        last_status = str(raw_status or "").strip().lower() or "unknown"
    except Exception:
        LOGGER.exception("Falha ao consultar status final do job do Airbyte Cloud apos timeout.")
        last_status = "unknown"

    raise AirflowFailException(
        "Timeout aguardando job do Airbyte Cloud. "
        f"connection_id={connection_id} job_id={job_id} "
        f"timeout_seconds={timeout_seconds} last_status={last_status}. "
        "Acao recomendada: verificar o job no Airbyte antes de relancar a DAG."
    )


def parse_airbyte_cloud_job_details(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "rows_processed": None,
            "rows_synced": None,
            "rows_emitted": None,
            "rows_extracted": None,
            "rows_metric_source": None,
            "stream_stats_summary": None,
        }

    job_payload = payload.get("job") if "job" in payload else payload
    terminal_attempt = extract_airbyte_terminal_attempt(job_payload)

    def parse_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    sync_stats = extract_airbyte_sync_stats(terminal_attempt) or extract_airbyte_sync_stats(job_payload) or {}
    total_stats = extract_airbyte_total_stats(terminal_attempt) or extract_airbyte_total_stats(job_payload) or {}

    rows_synced = parse_int(
        first_non_null(
            sync_stats.get("records_synced"),
            sync_stats.get("recordsSynced"),
            total_stats.get("records_synced"),
            total_stats.get("recordsSynced"),
            total_stats.get("recordsLoaded"),
            total_stats.get("records_loaded"),
        )
    )
    rows_emitted = parse_int(
        first_non_null(
            sync_stats.get("records_emitted"),
            sync_stats.get("recordsEmitted"),
            total_stats.get("records_emitted"),
            total_stats.get("recordsEmitted"),
        )
    )
    rows_extracted = parse_int(
        first_non_null(
            sync_stats.get("records_extracted"),
            sync_stats.get("recordsExtracted"),
            total_stats.get("records_extracted"),
            total_stats.get("recordsExtracted"),
        )
    )
    rows_committed = parse_int(
        first_non_null(
            sync_stats.get("records_committed"),
            sync_stats.get("recordsCommitted"),
            total_stats.get("records_committed"),
            total_stats.get("recordsCommitted"),
        )
    )

    stream_stats = extract_airbyte_stream_stats(terminal_attempt) or extract_airbyte_stream_stats(job_payload)
    stream_stats_summary = build_airbyte_stream_stats_summary(stream_stats)
    stream_stats_records_total = sum_airbyte_stream_stats_records(stream_stats)
    text_stats = extract_airbyte_text_stats(payload)
    rows_from_text = parse_int(
        first_non_null(
            text_stats.get("records_synced"),
            text_stats.get("records_committed"),
            text_stats.get("records_emitted"),
            text_stats.get("records_extracted"),
            text_stats.get("rows_inserted"),
        )
    )

    rows_processed = (
        rows_synced
        if rows_synced is not None
        else rows_committed
        if rows_committed is not None
        else rows_emitted
        if rows_emitted is not None
        else rows_extracted
        if rows_extracted is not None
        else stream_stats_records_total
        if stream_stats_records_total is not None
        else rows_from_text
    )

    rows_metric_source = (
        "records_synced"
        if rows_synced is not None
        else "records_committed"
        if rows_committed is not None
        else "records_emitted"
        if rows_emitted is not None
        else "records_extracted"
        if rows_extracted is not None
        else "stream_stats_total"
        if stream_stats_records_total is not None
        else text_stats.get("metric_name")
        if rows_from_text is not None
        else None
    )

    return {
        "rows_processed": rows_processed,
        "rows_synced": rows_synced,
        "rows_emitted": rows_emitted,
        "rows_extracted": rows_extracted,
        "rows_committed": rows_committed,
        "rows_stream_stats_total": stream_stats_records_total,
        "rows_metric_source": rows_metric_source,
        "stream_stats_summary": stream_stats_summary,
    }


def first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def find_first_dict_by_predicate(payload: Any, predicate: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        if predicate(payload):
            return payload
        for value in payload.values():
            found = find_first_dict_by_predicate(value, predicate)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_first_dict_by_predicate(item, predicate)
            if found is not None:
                return found
    return None


def find_first_list_by_keys(payload: Any, *keys: str) -> list[dict[str, Any]] | None:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                normalized = [item for item in value if isinstance(item, dict)]
                if normalized:
                    return normalized

        for value in payload.values():
            found = find_first_list_by_keys(value, *keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_first_list_by_keys(item, *keys)
            if found is not None:
                return found
    return None


def extract_airbyte_terminal_attempt(job_payload: Any) -> dict[str, Any] | None:
    if not isinstance(job_payload, dict):
        return None

    attempts = job_payload.get("attempts")
    if isinstance(attempts, dict):
        attempts = [attempts]
    if not isinstance(attempts, list) or not attempts:
        single_attempt = job_payload.get("attempt")
        return single_attempt if isinstance(single_attempt, dict) else None

    normalized_attempts = [item for item in attempts if isinstance(item, dict)]
    if not normalized_attempts:
        return None

    for attempt in reversed(normalized_attempts):
        attempt_status = str(
            first_non_null(
                attempt.get("status"),
                attempt.get("attempt", {}).get("status") if isinstance(attempt.get("attempt"), dict) else None,
            )
            or ""
        ).strip().lower()
        if attempt_status in {"succeeded", "completed", "failed", "cancelled", "canceled"}:
            return attempt

    return normalized_attempts[-1]


def extract_airbyte_sync_stats(payload: Any) -> dict[str, Any] | None:
    return find_first_dict_by_predicate(
        payload,
        lambda item: any(
            key in item
            for key in [
                "recordsSynced",
                "records_synced",
                "recordsCommitted",
                "records_committed",
                "recordsEmitted",
                "records_emitted",
                "recordsExtracted",
                "records_extracted",
            ]
        ),
    )


def extract_airbyte_total_stats(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    total_stats = payload.get("totalStats") or payload.get("total_stats")
    if isinstance(total_stats, dict):
        return total_stats

    attempt_payload = payload.get("attempt") if isinstance(payload.get("attempt"), dict) else None
    if attempt_payload:
        total_stats = attempt_payload.get("totalStats") or attempt_payload.get("total_stats")
        if isinstance(total_stats, dict):
            return total_stats

    return find_first_dict_by_predicate(
        payload,
        lambda item: any(
            key in item
            for key in [
                "recordsLoaded",
                "records_loaded",
                "recordsCommitted",
                "records_committed",
                "recordsEmitted",
                "records_emitted",
                "recordsExtracted",
                "records_extracted",
                "recordsSynced",
                "records_synced",
            ]
        ),
    )


def extract_airbyte_stream_stats(payload: Any) -> list[dict[str, Any]] | None:
    return find_first_list_by_keys(payload, "streamStats", "stream_stats")


def build_airbyte_stream_stats_summary(stream_stats: Any) -> str | None:
    if not isinstance(stream_stats, list):
        return None

    stream_summaries: list[str] = []
    for item in stream_stats:
        if not isinstance(item, dict):
            continue

        stream_name = item.get("streamName") or item.get("stream_name")
        stats = item.get("stats") if isinstance(item.get("stats"), dict) else {}
        records_committed = (
            stats.get("recordsCommitted")
            or stats.get("records_committed")
            or item.get("recordsCommitted")
            or item.get("records_committed")
        )
        records_emitted = (
            stats.get("recordsEmitted")
            or stats.get("records_emitted")
            or item.get("recordsEmitted")
            or item.get("records_emitted")
        )
        records_extracted = (
            stats.get("recordsExtracted")
            or stats.get("records_extracted")
            or item.get("recordsExtracted")
            or item.get("records_extracted")
        )

        records_value = (
            records_committed
            if records_committed is not None
            else records_emitted
            if records_emitted is not None
            else records_extracted
        )
        if stream_name and records_value is not None:
            stream_summaries.append(f"{stream_name}={records_value}")

    return ", ".join(stream_summaries) if stream_summaries else None


def sum_airbyte_stream_stats_records(stream_stats: Any) -> int | None:
    if not isinstance(stream_stats, list):
        return None

    total = 0
    found_any = False
    for item in stream_stats:
        if not isinstance(item, dict):
            continue

        stats = item.get("stats") if isinstance(item.get("stats"), dict) else {}
        for key in (
            "recordsCommitted",
            "records_committed",
            "recordsEmitted",
            "records_emitted",
            "recordsExtracted",
            "records_extracted",
            "recordsSynced",
            "records_synced",
        ):
            value = stats.get(key)
            if value is None:
                value = item.get(key)
            if value is None:
                continue
            try:
                total += int(value)
                found_any = True
                break
            except Exception:
                continue

    return total if found_any else None


def iter_string_values(payload: Any) -> list[str]:
    values: list[str] = []
    if isinstance(payload, str):
        values.append(payload)
    elif isinstance(payload, dict):
        for value in payload.values():
            values.extend(iter_string_values(value))
    elif isinstance(payload, list):
        for item in payload:
            values.extend(iter_string_values(item))
    return values


def extract_airbyte_text_stats(payload: Any) -> dict[str, Any]:
    text_values = iter_string_values(payload)
    if not text_values:
        return {}

    text_patterns: list[tuple[str, str]] = [
        ("records_synced", r"recordsSynced[\"'\s:=]+(\d+)"),
        ("records_committed", r"recordsCommitted[\"'\s:=]+(\d+)"),
        ("records_emitted", r"recordsEmitted[\"'\s:=]+(\d+)"),
        ("records_extracted", r"recordsExtracted[\"'\s:=]+(\d+)"),
        ("rows_inserted", r"Finished insert of (\d+) row\(s\)"),
    ]

    for text in text_values:
        for metric_name, pattern in text_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            try:
                return {
                    metric_name: int(match.group(1)),
                    "metric_name": metric_name,
                    "raw_excerpt": text[:500],
                }
            except Exception:
                continue

    return {}


def fetch_batch_row_totals(batch_id: str) -> dict[str, int | None]:
    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()
        escaped_batch_id = batch_id.replace("'", "''")
        cursor.execute(f"""
select
    count(*) as AUDIT_EVENTS,
    coalesce(sum(case when STEP_NAME = 'AIRBYTE_SYNC' and STATUS = 'SUCCESS' then coalesce(ROWS_PROCESSED, 0) else 0 end), 0) as ROWS_EXTRACTED,
    coalesce(sum(case when STEP_NAME = 'DBT_RUN' and STATUS = 'SUCCESS' then coalesce(ROWS_PROCESSED, 0) else 0 end), 0) as ROWS_LOADED,
    coalesce(sum(
        case
            when STEP_NAME = 'DBT_RUN' and STATUS = 'SUCCESS'
                then coalesce(try_to_number(regexp_substr(DETAILS, 'rows_affected=([0-9]+)', 1, 1, 'e', 1)), 0)
            else 0
        end
    ), 0) as ROWS_AFFECTED
from SOLIX_BI.DS.CTL_LOAD_AUDIT
where BATCH_ID = '{escaped_batch_id}'
""")
        row = cursor.fetchone() or (0, 0, 0, 0)

        audit_events = int(row[0] or 0)
        rows_extracted = int(row[1] or 0)
        rows_loaded = int(row[2] or 0)
        rows_affected = int(row[3] or 0)

        if audit_events == 0:
            LOGGER.warning(
                "Nenhum evento encontrado em CTL_LOAD_AUDIT para consolidar rows no batch_id=%s. "
                "Isso indica que as DAGs filhas nao gravaram auditoria com o mesmo BATCH_ID da orquestracao.",
                batch_id,
            )

        return {
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
            "rows_affected": rows_affected,
        }
    except Exception as exc:
        LOGGER.exception("Falha ao consolidar rows de CTL_LOAD_AUDIT para CTL_BATCH_EXECUTION.")
        if should_fail_on_audit_error():
            raise AirflowFailException(
                "Falha critica ao consolidar rows para CTL_BATCH_EXECUTION em modo fail-closed."
            ) from exc
        return {"rows_extracted": None, "rows_loaded": None, "rows_affected": None}
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def execute_snowflake_sql(*, sql: str) -> dict[str, int | None]:
    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()
        cursor.execute(sql)
        rows_affected = cursor.rowcount
        connection.commit()
        return {"rows_affected": int(rows_affected) if rows_affected is not None else None}
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


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

    final_model_results = [
        result
        for result in model_results
        if str(result.get("unique_id", "")).split(".")[-1].startswith(("dim_", "fct_"))
    ]

    return {
        "generated_at": payload.get("metadata", {}).get("generated_at"),
        "dbt_version": payload.get("metadata", {}).get("dbt_version"),
        "model_results": model_results,
        "final_model_results": final_model_results,
        "test_results": test_results,
        "all_results": model_results + test_results,
        "model_count": len(model_results),
        "test_count": len(test_results),
        "rows_inserted_total": sum(result["rows_inserted"] for result in model_results),
        "rows_updated_total": sum(result["rows_updated"] for result in model_results),
        "rows_deleted_total": sum(result["rows_deleted"] for result in model_results),
        "rows_affected_total": sum(result["rows_affected"] for result in model_results),
        "rows_inserted_final_total": sum(result["rows_inserted"] for result in final_model_results),
        "rows_updated_final_total": sum(result["rows_updated"] for result in final_model_results),
        "rows_deleted_final_total": sum(result["rows_deleted"] for result in final_model_results),
        "rows_affected_final_total": sum(result["rows_affected"] for result in final_model_results),
    }
