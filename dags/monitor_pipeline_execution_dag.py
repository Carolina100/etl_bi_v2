from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

from src.utils.airflow_helpers import (
    DEFAULT_STALL_THRESHOLD_MINUTES,
    PIPELINE_STATUS_RUNNING,
    PIPELINE_STATUS_SUCCESS,
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    open_snowflake_connection,
    send_operational_alert,
)

# ============================================================================
# MONITOR DE EXECUCOES ESPERADAS
# Verifica se cada pipeline teve um SUCCESS dentro da janela esperada.
# Adicione aqui todos os pipelines que precisam de SLA formal.
# ============================================================================
PIPELINE_MONITOR_SPECS = [
    {
        "pipeline_name": "dimensions_domain",
        "description": "dominio de dimensoes incrementais, previsto duas vezes ao dia",
        "expected_success_within_minutes": 14 * 60,
    },
    {
        "pipeline_name": "cleanup_dimensions_retention_dag",
        "description": "cleanup diario de retencao tecnica do DS",
        "expected_success_within_minutes": 26 * 60,
    },
    # Facts horarias entram aqui quando forem ativadas:
    # {
    #     "pipeline_name": "fct_nome_da_fato",
    #     "description": "pipeline incremental horario de fato",
    #     "expected_success_within_minutes": 120,
    # },
]

# ============================================================================
# MONITOR DE PIPELINES TRAVADOS (STALLED)
# Pipelines que ficaram em RUNNING por mais tempo do que o threshold esperado
# sao considerados travados e disparam alerta.
# Adicione aqui todos os pipelines que gravam LAST_RUN_STATUS em CTL_PIPELINE_WATERMARK.
# ============================================================================
STALLED_PIPELINE_SPECS = [
    {
        "pipeline_name": "dim_sx_cliente_d",
        "description": "dimensao incremental de cliente",
        "stall_threshold_minutes": DEFAULT_STALL_THRESHOLD_MINUTES,
    },
    {
        "pipeline_name": "dim_sx_estado_d",
        "description": "dimensao incremental de estado",
        "stall_threshold_minutes": DEFAULT_STALL_THRESHOLD_MINUTES,
    },
    {
        "pipeline_name": "dim_sx_equipamento_d",
        "description": "dimensao incremental de equipamento",
        "stall_threshold_minutes": DEFAULT_STALL_THRESHOLD_MINUTES,
    },
    {
        "pipeline_name": "dim_sx_operacao_d",
        "description": "dimensao incremental de operacao",
        "stall_threshold_minutes": DEFAULT_STALL_THRESHOLD_MINUTES,
    },
    # Quando novas dimensoes ou fatos forem adicionados, incluir aqui tambem.
]


def check_expected_pipeline_runs(**context: Any) -> dict[str, Any]:
    missing_successes: list[str] = []
    checked_pipelines: list[str] = []

    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        for spec in PIPELINE_MONITOR_SPECS:
            pipeline_name = spec["pipeline_name"]
            expected_minutes = int(spec["expected_success_within_minutes"])
            checked_pipelines.append(pipeline_name)

            escaped_pipeline_name = pipeline_name.replace("'", "''")
            cursor.execute(f"""
select
    max(ENDED_AT) as LAST_SUCCESS_AT
from SOLIX_BI.DS.CTL_BATCH_EXECUTION
where PIPELINE_NAME = '{escaped_pipeline_name}'
  and STATUS = '{PIPELINE_STATUS_SUCCESS}'
  and ENDED_AT >= dateadd(minute, -{expected_minutes}, convert_timezone('UTC', current_timestamp())::timestamp_ntz)
""")
            row = cursor.fetchone()
            last_success_at = row[0] if row else None

            if last_success_at is None:
                missing_successes.append(
                    f"{pipeline_name} sem SUCCESS nos ultimos {expected_minutes} minutos ({spec['description']})"
                )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    if missing_successes:
        raise AirflowFailException(
            "Execucao esperada ausente: " + " | ".join(missing_successes)
        )

    return {
        "status": "SUCCESS",
        "checked_pipelines": checked_pipelines,
    }


def check_stalled_pipelines(**context: Any) -> dict[str, Any]:
    """
    Verifica pipelines travados (stuck em RUNNING) na CTL_PIPELINE_WATERMARK.

    Um pipeline e considerado travado quando:
    - LAST_RUN_STATUS = 'RUNNING'
    - LAST_RUN_STARTED_AT < agora - stall_threshold_minutes

    Isso indica que o Airflow iniciou um run mas nao registrou conclusao,
    possivelmente por timeout, kill de worker ou falha silenciosa de auditoria.
    """
    stalled_pipelines: list[str] = []
    checked_pipelines: list[str] = []

    connection = None
    cursor = None
    try:
        connection = open_snowflake_connection()
        cursor = connection.cursor()

        for spec in STALLED_PIPELINE_SPECS:
            pipeline_name = spec["pipeline_name"]
            stall_threshold_minutes = int(spec["stall_threshold_minutes"])
            checked_pipelines.append(pipeline_name)

            escaped_pipeline_name = pipeline_name.replace("'", "''")
            cursor.execute(f"""
select
    PIPELINE_NAME,
    LAST_RUN_STATUS,
    LAST_RUN_STARTED_AT,
    datediff(minute, LAST_RUN_STARTED_AT, convert_timezone('UTC', current_timestamp())::timestamp_ntz) as minutes_running,
    LAST_RUN_BATCH_ID
from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
where PIPELINE_NAME = '{escaped_pipeline_name}'
  and LAST_RUN_STATUS = '{PIPELINE_STATUS_RUNNING}'
  and LAST_RUN_STARTED_AT < dateadd(
        minute,
        -{stall_threshold_minutes},
        convert_timezone('UTC', current_timestamp())::timestamp_ntz
      )
""")
            row = cursor.fetchone()
            if row is not None:
                minutes_running = int(row[3]) if row[3] is not None else stall_threshold_minutes
                batch_id = row[4] or "desconhecido"
                stalled_pipelines.append(
                    f"{pipeline_name} travado em RUNNING por ~{minutes_running}min "
                    f"(threshold={stall_threshold_minutes}min batch_id={batch_id} desc={spec['description']})"
                )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    if stalled_pipelines:
        title = "Airflow MONITOR: pipelines travados detectados (STALLED)"
        message = "\n".join(stalled_pipelines)
        send_operational_alert(title=title, message=message, severity="ERROR")
        raise AirflowFailException(
            "Pipelines travados em RUNNING detectados: " + " | ".join(stalled_pipelines)
        )

    return {
        "status": "SUCCESS",
        "checked_pipelines": checked_pipelines,
    }


with DAG(
    dag_id="monitor_pipeline_execution_dag",
    description="Monitora ausencia de execucoes esperadas e pipelines travados nas tabelas CTL.",
    start_date=datetime(2025, 1, 1),
    schedule="40 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["monitoring", "ctl", "alerts", "production-readiness"],
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": airflow_failure_alert_callback,
        "on_retry_callback": airflow_retry_alert_callback,
    },
    on_failure_callback=airflow_failure_alert_callback,
) as dag:
    check_expected_pipeline_runs_task = PythonOperator(
        task_id="check_expected_pipeline_runs",
        python_callable=check_expected_pipeline_runs,
        queue="dbt",
    )

    check_stalled_pipelines_task = PythonOperator(
        task_id="check_stalled_pipelines",
        python_callable=check_stalled_pipelines,
        queue="dbt",
    )

    # As duas verificacoes rodam em paralelo — uma falha nao bloqueia a outra.
    [check_expected_pipeline_runs_task, check_stalled_pipelines_task]
