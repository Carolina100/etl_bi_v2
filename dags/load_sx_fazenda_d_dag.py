from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import uuid

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

# Permite importar o codigo da pasta src mesmo com a DAG rodando isolada.
PROJECT_ROOT = Path(
    os.getenv("AIRFLOW_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))
).resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.airflow_helpers import normalize_client_conf, run_dbt_command  # noqa: E402

logger = logging.getLogger(__name__)

ACTIVE_CLIENTS_QUERY = """
SELECT ID_CLIENTE
FROM SOLIX_BI.DS.SX_CLIENTE_D
WHERE FL_ATIVO = TRUE
ORDER BY ID_CLIENTE
"""

# ============================================================================
# CONFIGURACAO DA DAG
#
# AO REUTILIZAR ESTA DAG PARA OUTRA ENTIDADE, NORMALMENTE TROQUE:
# 1. DAG_ID
# 2. PIPELINE_DESCRIPTION
# 3. TAGS
# 4. DBT_PROJECT_DIR
# 5. DBT_SELECT_MODELS
# 6. PIPELINE_TASK_ID
# 7. a pipeline Python importada
#
# ESTRATEGIA DE ORQUESTRACAO:
# - task 1: valida e normaliza parametros
# - task 2: executa DS por cliente com task mapping
# - task 3: executa DW uma vez ao final
#
# MOTIVO:
# - suporta multiplos clientes no mesmo run
# - evita duplicar regra da pipeline no Airflow
# - aproxima do desenho futuro com Airflow em producao
# ============================================================================
DAG_ID = "load_sx_fazenda_d_dag"
PIPELINE_DESCRIPTION = "Carga Oracle -> Snowflake DS -> dbt DW para SX_FAZENDA_D"
TAGS = ["oracle", "snowflake", "dbt", "ds", "dw", "sx_fazenda_d"]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "solix_dbt"
DBT_SELECT_MODELS = ["stg_ds__sx_fazenda_d", "dim_sx_fazenda_d"]
PIPELINE_TASK_ID = "run_ds_pipeline"
DW_PIPELINE_NAME = "dw_sx_fazenda_d_dbt"
DW_SOURCE_NAME = "DBT.SOLIX_BI.DS.SX_FAZENDA_D"
DW_TARGET_NAME = "SOLIX_BI.DW.SX_FAZENDA_D"
DW_MAIN_STEP_NAME = "DBT_BUILD"
DS_QUEUE = "ds"
DBT_QUEUE = "dbt"

# ============================================================================
# ESTRATEGIA DE AGENDAMENTO E RETRIES
#
# PADRAO RECOMENDADO:
# - max_active_runs=1
#   Evita duas execucoes da mesma DAG concorrendo entre si.
#
# - max_active_tasks=4
#   Limita o paralelismo total da DAG e ajuda a proteger Oracle/Snowflake.
#
# - DS por cliente:
#   retries=3, retry_delay=5 min, exponential backoff.
#   Motivo: erros de rede/origem costumam ser temporarios.
#
# - DW via dbt:
#   retries=1, retry_delay=5 min.
#   Motivo: se falhar, normalmente uma nova tentativa ja basta.
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
        "id_cliente": Param(None, type=["null", "integer"], minimum=1),
        "id_clientes": Param(None, type=["null", "array"]),
        "data_inicio": Param(None, type=["null", "string"]),
        "data_fim": Param(None, type=["null", "string"]),
        "full_reconciliation": Param(False, type="boolean"),
    },
)
def load_sx_fazenda_d_dag():
    """
    DAG pronta para evolucao:
    - hoje: DS por cliente + DW ao final
    - depois: pode ganhar validacoes, alertas e observabilidade separadas
    """

    @task(task_id="validate_and_prepare_params")
    def validate_and_prepare_params() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        params = context.get("params", {})
        raw_conf = dag_run.conf if dag_run and dag_run.conf else params

        normalized_conf = normalize_client_conf(raw_conf, allow_missing_client=True)

        logger.info(
            "Parametros validados. id_clientes=%s, load_mode=%s, data_inicio=%s, data_fim=%s, full_reconciliation=%s",
            normalized_conf["id_clientes"],
            normalized_conf["load_mode"],
            normalized_conf["data_inicio"],
            normalized_conf["data_fim"],
            normalized_conf["full_reconciliation"],
        )
        return normalized_conf

    @task(task_id="resolve_clientes")
    def resolve_clientes(payload: dict[str, Any]) -> dict[str, Any]:
        if payload["id_clientes"]:
            logger.info(
                "Execucao manual detectada. Clientes informados explicitamente: %s",
                payload["id_clientes"],
            )
            return payload

        from src.load.snowflake_loader import SnowflakeLoader

        loader = SnowflakeLoader()
        try:
            rows = loader.fetch_all(ACTIVE_CLIENTS_QUERY)
        finally:
            loader.close()

        resolved_ids = [int(row[0]) for row in rows]
        if not resolved_ids:
            raise ValueError("Nenhum cliente ativo encontrado em SOLIX_BI.DS.SX_CLIENTE_D.")

        resolved_payload = dict(payload)
        resolved_payload["id_clientes"] = resolved_ids

        logger.info(
            "Execucao automatica detectada. Clientes ativos encontrados: %s",
            resolved_ids,
        )
        return resolved_payload

    @task(task_id="build_ds_requests")
    def build_ds_requests(payload: dict[str, Any]) -> list[dict[str, Any]]:
        requests = [
            {
                "id_cliente": id_cliente,
                "data_inicio": payload["data_inicio"],
                "data_fim": payload["data_fim"],
                "full_reconciliation": payload["full_reconciliation"],
            }
            for id_cliente in payload["id_clientes"]
        ]
        logger.info("Requests da camada DS montados: %s", requests)
        return requests

    @task(
        task_id=PIPELINE_TASK_ID,
        retries=DS_RETRIES,
        retry_delay=timedelta(minutes=DS_RETRY_DELAY_MINUTES),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
    )
    def run_ds_pipeline_task(request: dict[str, Any]) -> dict[str, Any]:
        from src.pipelines.load_sx_fazenda_d import run_pipeline

        try:
            logger.info(
                "Executando camada DS para id_cliente=%s, data_inicio=%s, data_fim=%s, full_reconciliation=%s",
                request["id_cliente"],
                request["data_inicio"],
                request["data_fim"],
                request["full_reconciliation"],
            )

            result = run_pipeline(
                id_cliente=int(request["id_cliente"]),
                data_inicio_text=request["data_inicio"],
                data_fim_text=request["data_fim"],
                full_reconciliation=bool(request["full_reconciliation"]),
            )

            logger.info("Camada DS concluida com resultado: %s", result)
            return {
                "id_cliente": int(request["id_cliente"]),
                "status": "SUCCESS",
                "result": result,
                "error_message": None,
            }
        except Exception as exc:
            logger.exception(
                "Falha na camada DS para id_cliente=%s.",
                request["id_cliente"],
            )
            return {
                "id_cliente": int(request["id_cliente"]),
                "status": "ERROR",
                "result": None,
                "error_message": str(exc),
            }

    @task(task_id="summarize_ds_results")
    def summarize_ds_results(ds_results: list[dict[str, Any]]) -> dict[str, Any]:
        materialized_ds_results = list(ds_results)
        successful_items = [
            item for item in materialized_ds_results if item.get("status") == "SUCCESS"
        ]
        failed_items = [
            item for item in materialized_ds_results if item.get("status") == "ERROR"
        ]
        successful_results = [
            item["result"]
            for item in successful_items
            if isinstance(item.get("result"), dict)
        ]

        summary = {
            "successful_clientes": [int(item["id_cliente"]) for item in successful_items],
            "failed_clientes": [int(item["id_cliente"]) for item in failed_items],
            "successful_results": successful_results,
            "failed_results": failed_items,
        }
        logger.info("Resumo da camada DS: %s", summary)
        return summary

    @task(
        task_id="run_dw_dbt",
        retries=DW_RETRIES,
        retry_delay=timedelta(minutes=DW_RETRY_DELAY_MINUTES),
    )
    def run_dw_dbt_task(ds_summary: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        from src.audit.audit_repository import AuditRepository
        from src.load.snowflake_loader import SnowflakeLoader

        materialized_ds_results = list(ds_summary.get("successful_results", []))
        successful_clientes = list(ds_summary.get("successful_clientes", []))
        failed_clientes = list(ds_summary.get("failed_clientes", []))
        failed_results = list(ds_summary.get("failed_results", []))

        if not materialized_ds_results:
            raise AirflowFailException(
                "Nenhum cliente processado com sucesso na camada DS. A camada DW nao sera executada."
            )
        profiles_dir = os.getenv("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
        audit_loader = SnowflakeLoader()
        audit_repository = AuditRepository(audit_loader)
        dw_batch_id = uuid.uuid4().hex
        audit_id_cliente = successful_clientes[0] if len(successful_clientes) == 1 else -1
        audit_dt_inicio = payload["data_inicio"]
        audit_dt_fim = payload["data_fim"]
        upstream_batch_ids = [
            str(result.get("batch_id"))
            for result in materialized_ds_results
            if isinstance(result, dict) and result.get("batch_id")
        ]
        upstream_rows_loaded = sum(
            int(result.get("rows_loaded") or 0)
            for result in materialized_ds_results
            if isinstance(result, dict)
        )

        logger.info(
            "Iniciando camada DW via dbt. profiles_dir=%s, select=%s, clientes=%s, load_mode=%s",
            profiles_dir,
            " ".join(DBT_SELECT_MODELS),
            successful_clientes,
            payload["load_mode"],
        )
        logger.info(
            "Resultados recebidos da camada DS. sucesso=%s, falha=%s",
            successful_clientes,
            failed_clientes,
        )

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

        audit_repository.insert_batch_start(
            batch_id=dw_batch_id,
            pipeline_name=DW_PIPELINE_NAME,
            source_name=DW_SOURCE_NAME,
            target_name=DW_TARGET_NAME,
            id_cliente=audit_id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        audit_repository.insert_audit_event(
            batch_id=dw_batch_id,
            step_name="DS_SUCCESSFUL_CLIENTS",
            source_name=DW_SOURCE_NAME,
            target_name=DW_TARGET_NAME,
            status="SUCCESS",
            rows_processed=len(successful_clientes),
            details=f"Clientes processados com sucesso na camada DS: {successful_clientes}",
            id_cliente=audit_id_cliente,
            dt_inicio=audit_dt_inicio,
            dt_fim=audit_dt_fim,
        )

        if failed_clientes:
            audit_repository.insert_audit_event(
                batch_id=dw_batch_id,
                step_name="DS_FAILED_CLIENTS",
                source_name=DW_SOURCE_NAME,
                target_name=DW_TARGET_NAME,
                status="ERROR",
                rows_processed=len(failed_clientes),
                details=(
                    f"Clientes com falha na camada DS e excluidos do DW nesta execucao: "
                    f"{failed_clientes}. Detalhes={failed_results}"
                ),
                id_cliente=audit_id_cliente,
                dt_inicio=audit_dt_inicio,
                dt_fim=audit_dt_fim,
            )

        try:
            result = run_dbt_command(
                dbt_project_dir=str(DBT_PROJECT_DIR),
                dbt_profiles_dir=profiles_dir,
                select_models=DBT_SELECT_MODELS,
            )

            if result.get("stdout"):
                logger.info("dbt stdout:\n%s", result["stdout"])
            if result.get("stderr"):
                logger.warning("dbt stderr:\n%s", result["stderr"])

            run_results = result.get("run_results", {})
            model_results = run_results.get("model_results", [])
            rows_inserted_total = int(run_results.get("rows_inserted_total") or 0)
            rows_updated_total = int(run_results.get("rows_updated_total") or 0)
            rows_deleted_total = int(run_results.get("rows_deleted_total") or 0)
            rows_affected_total = int(run_results.get("rows_affected_total") or 0)
            rows_loaded_total = rows_inserted_total + rows_updated_total
            if rows_loaded_total == 0 and rows_affected_total > 0:
                rows_loaded_total = rows_affected_total

            audit_repository.insert_audit_event(
                batch_id=dw_batch_id,
                step_name=DW_MAIN_STEP_NAME,
                source_name=DW_SOURCE_NAME,
                target_name=DW_TARGET_NAME,
                status="SUCCESS",
                rows_processed=rows_loaded_total,
                details=(
                    f"dbt build concluido com sucesso. models={DBT_SELECT_MODELS}, "
                    f"successful_clientes={successful_clientes}, "
                    f"failed_clientes={failed_clientes}, "
                    f"load_mode={payload['load_mode']}, "
                    f"upstream_batch_ids={upstream_batch_ids}, "
                    f"rows_inserted_total={rows_inserted_total}, "
                    f"rows_updated_total={rows_updated_total}, "
                    f"rows_deleted_total={rows_deleted_total}, "
                    f"rows_affected_total={rows_affected_total}, "
                    f"model_count={run_results.get('model_count', 0)}, "
                    f"test_count={run_results.get('test_count', 0)}."
                ),
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
                    details=(
                        f"query_id={model_result.get('query_id')}, "
                        f"rows_inserted={model_result.get('rows_inserted')}, "
                        f"rows_updated={model_result.get('rows_updated')}, "
                        f"rows_deleted={model_result.get('rows_deleted')}."
                    ),
                    id_cliente=audit_id_cliente,
                    dt_inicio=audit_dt_inicio,
                    dt_fim=audit_dt_fim,
                )

            audit_repository.update_batch_success(
                batch_id=dw_batch_id,
                rows_extracted=upstream_rows_loaded,
                rows_loaded=rows_loaded_total,
            )

            result.update(
                {
                    "dw_batch_id": dw_batch_id,
                    "id_clientes": successful_clientes,
                    "failed_clientes": failed_clientes,
                    "data_inicio": payload["data_inicio"],
                    "data_fim": payload["data_fim"],
                }
            )
            logger.info("Camada DW concluida com sucesso: %s", result)
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

    @task(task_id="report_failed_clients")
    def report_failed_clients(ds_summary: dict[str, Any]) -> dict[str, Any]:
        failed_clientes = list(ds_summary.get("failed_clientes", []))
        failed_results = list(ds_summary.get("failed_results", []))

        if failed_clientes:
            logger.warning(
                "Clientes com falha na camada DS e excluidos do DW nesta execucao: %s. Detalhes=%s",
                failed_clientes,
                failed_results,
            )
        else:
            logger.info("Nenhum cliente falhou na camada DS.")

        return {
            "failed_clientes": failed_clientes,
            "failed_results": failed_results,
        }

    prepared_params = validate_and_prepare_params.override(queue=DS_QUEUE)()
    resolved_params = resolve_clientes.override(queue=DS_QUEUE)(prepared_params)
    ds_requests = build_ds_requests.override(queue=DS_QUEUE)(resolved_params)
    ds_results = run_ds_pipeline_task.override(queue=DS_QUEUE).expand(request=ds_requests)
    ds_summary = summarize_ds_results.override(queue=DS_QUEUE)(ds_results)
    report_failed_clients.override(queue=DS_QUEUE)(ds_summary)
    run_dw_dbt_task.override(queue=DBT_QUEUE)(ds_summary, resolved_params)


load_sx_fazenda_d = load_sx_fazenda_d_dag()
