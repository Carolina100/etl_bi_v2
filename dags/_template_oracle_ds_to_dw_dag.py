from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import uuid

from airflow.sdk import Param, dag, get_current_context, task

# ============================================================================
# TEMPLATE DE DAG AIRFLOW
#
# COMO USAR:
# 1. Copie este arquivo
# 2. Renomeie, por exemplo, para: load_sx_cidade_d_dag.py
# 3. Troque apenas os blocos marcados em "TROQUE AQUI"
#
# ESTRATEGIA:
# - valida parametros
# - executa a camada DS por cliente
# - executa o dbt do DW uma vez no final
# - registra auditoria do DW
# ============================================================================

PROJECT_ROOT = Path(
    os.getenv("AIRFLOW_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))
).resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================================
# TROQUE AQUI - IMPORTS DA PIPELINE
# ============================================================================
from src.utils.airflow_helpers import normalize_client_conf, run_dbt_command  # noqa: E402

logger = logging.getLogger(__name__)

# ============================================================================
# TROQUE AQUI - CONFIGURACAO DA DAG
# ============================================================================
DAG_ID = "load_nome_da_tabela_d_dag"
PIPELINE_DESCRIPTION = "Carga Oracle -> Snowflake DS -> dbt DW para NOME_DA_TABELA_D"
TAGS = ["oracle", "snowflake", "dbt", "ds", "dw", "nome_da_tabela_d"]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "solix_dbt"
DBT_SELECT_MODELS = ["stg_ds__nome_da_tabela_d", "dim_nome_da_tabela_d"]
PIPELINE_TASK_ID = "run_ds_pipeline"
DW_PIPELINE_NAME = "dw_nome_da_tabela_d_dbt"
DW_SOURCE_NAME = "DBT.SOLIX_BI.DS.NOME_DA_TABELA_D"
DW_TARGET_NAME = "SOLIX_BI.DW.NOME_DA_TABELA_D"
DW_MAIN_STEP_NAME = "DBT_BUILD"
DS_QUEUE = "ds"
DBT_QUEUE = "dbt"

# ============================================================================
# TROQUE AQUI - POLITICA DE EXECUCAO
#
# PADRAO RECOMENDADO:
# - max_active_runs=1
# - max_active_tasks=4
# - DS com retry maior por cliente
# - DW com retry menor
# ============================================================================
DS_RETRIES = 3
DS_RETRY_DELAY_MINUTES = 5
DW_RETRIES = 1
DW_RETRY_DELAY_MINUTES = 5
MAX_ACTIVE_RUNS = 1
MAX_ACTIVE_TASKS = 4


@dag(
    dag_id=DAG_ID,
    description=PIPELINE_DESCRIPTION,
    start_date=datetime(2026, 3, 21),
    schedule=None,
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=TAGS,
    render_template_as_native_obj=True,
    default_args={
        "owner": "data-platform",
        "retries": 0,
    },
    params={
        "id_cliente": Param(1, type=["null", "integer"], minimum=1),
        "id_clientes": Param(None, type=["null", "array"]),
        "data_inicio": Param(None, type=["null", "string"]),
        "data_fim": Param(None, type=["null", "string"]),
    },
)
def load_template_dag():
    @task(task_id="validate_and_prepare_params")
    def validate_and_prepare_params() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        params = context.get("params", {})
        raw_conf = dag_run.conf if dag_run and dag_run.conf else params
        return normalize_client_conf(raw_conf)

    @task(task_id="build_ds_requests")
    def build_ds_requests(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "id_cliente": id_cliente,
                "data_inicio": payload["data_inicio"],
                "data_fim": payload["data_fim"],
            }
            for id_cliente in payload["id_clientes"]
        ]

    @task(
        task_id=PIPELINE_TASK_ID,
        retries=DS_RETRIES,
        retry_delay=timedelta(minutes=DS_RETRY_DELAY_MINUTES),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
    )
    def run_ds_pipeline_task(request: dict[str, Any]) -> dict[str, Any]:
        from src.pipelines.load_sx_estado_d import run_pipeline

        return run_pipeline(
            id_cliente=int(request["id_cliente"]),
            data_inicio_text=request["data_inicio"],
            data_fim_text=request["data_fim"],
        )

    @task(
        task_id="run_dw_dbt",
        retries=DW_RETRIES,
        retry_delay=timedelta(minutes=DW_RETRY_DELAY_MINUTES),
    )
    def run_dw_dbt_task(ds_results: list[dict[str, Any]], payload: dict[str, Any]) -> dict[str, Any]:
        from src.audit.audit_repository import AuditRepository
        from src.load.snowflake_loader import SnowflakeLoader

        materialized_ds_results = list(ds_results)
        profiles_dir = os.getenv("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
        audit_loader = SnowflakeLoader()
        audit_repository = AuditRepository(audit_loader)
        dw_batch_id = uuid.uuid4().hex
        audit_id_cliente = payload["id_clientes"][0] if len(payload["id_clientes"]) == 1 else -1
        resolved_window_starts = sorted(
            str(result["window_start"])
            for result in materialized_ds_results
            if isinstance(result, dict) and result.get("window_start")
        )
        resolved_window_ends = sorted(
            str(result["window_end"])
            for result in materialized_ds_results
            if isinstance(result, dict) and result.get("window_end")
        )
        audit_dt_inicio = resolved_window_starts[0] if resolved_window_starts else datetime.now().isoformat()
        audit_dt_fim = resolved_window_ends[-1] if resolved_window_ends else datetime.now().isoformat()

        try:
            audit_repository.insert_batch_start(
                batch_id=dw_batch_id,
                pipeline_name=DW_PIPELINE_NAME,
                source_name=DW_SOURCE_NAME,
                target_name=DW_TARGET_NAME,
                id_cliente=audit_id_cliente,
                dt_inicio=audit_dt_inicio,
                dt_fim=audit_dt_fim,
            )

            result = run_dbt_command(
                dbt_project_dir=str(DBT_PROJECT_DIR),
                dbt_profiles_dir=profiles_dir,
                select_models=DBT_SELECT_MODELS,
            )

            run_results = result.get("run_results", {})
            model_results = run_results.get("model_results", [])
            rows_affected_total = int(run_results.get("rows_affected_total") or 0)

            audit_repository.insert_audit_event(
                batch_id=dw_batch_id,
                step_name=DW_MAIN_STEP_NAME,
                source_name=DW_SOURCE_NAME,
                target_name=DW_TARGET_NAME,
                status="SUCCESS",
                rows_processed=rows_affected_total,
                details=f"dbt build concluido com sucesso. load_mode={payload['load_mode']}.",
                id_cliente=audit_id_cliente,
                dt_inicio=audit_dt_inicio,
                dt_fim=audit_dt_fim,
            )

            for model_result in model_results:
                model_name = str(model_result.get("unique_id", "")).split(".")[-1].upper()
                audit_repository.insert_audit_event(
                    batch_id=dw_batch_id,
                    step_name=f"DBT_MODEL_{model_name}",
                    source_name=DW_SOURCE_NAME,
                    target_name=DW_TARGET_NAME,
                    status=str(model_result.get("status", "UNKNOWN")).upper(),
                    rows_processed=int(model_result.get("rows_affected") or 0),
                    details=f"query_id={model_result.get('query_id')}",
                    id_cliente=audit_id_cliente,
                    dt_inicio=audit_dt_inicio,
                    dt_fim=audit_dt_fim,
                )

            audit_repository.update_batch_success(
                batch_id=dw_batch_id,
                rows_extracted=sum(int(result.get("rows_loaded") or 0) for result in materialized_ds_results),
                rows_loaded=rows_affected_total,
            )
            return result
        except Exception as exc:
            try:
                audit_repository.insert_audit_event(
                    batch_id=dw_batch_id,
                    step_name="DBT_ERROR",
                    source_name=DW_SOURCE_NAME,
                    target_name=DW_TARGET_NAME,
                    status="ERROR",
                    rows_processed=0,
                    details=str(exc),
                    id_cliente=audit_id_cliente,
                    dt_inicio=audit_dt_inicio,
                    dt_fim=audit_dt_fim,
                )
                audit_repository.update_batch_error(
                    batch_id=dw_batch_id,
                    error_message=str(exc),
                )
            except Exception:
                logger.exception("Falha ao registrar erro da auditoria do DW.")
            raise
        finally:
            try:
                audit_loader.close()
            except Exception:
                logger.exception("Falha ao fechar conexao de auditoria do DW.")

    prepared_params = validate_and_prepare_params.override(queue=DS_QUEUE)()
    ds_requests = build_ds_requests.override(queue=DS_QUEUE)(prepared_params)
    ds_results = run_ds_pipeline_task.override(queue=DS_QUEUE).expand(request=ds_requests)
    run_dw_dbt_task.override(queue=DBT_QUEUE)(ds_results, prepared_params)


load_template = load_template_dag()
