from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

from src.utils.airflow_helpers import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_STARTED,
    AUDIT_STATUS_SUCCESS,
    PIPELINE_STATUS_FAILED,
    PIPELINE_STATUS_SUCCESS,
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
    audit_batch_execution_end,
    audit_batch_execution_start,
    audit_load_audit,
    execute_snowflake_sql,
)

# ============================================================================
# CONFIGURACAO CENTRAL DE RETENCAO
# ----------------------------------------------------------------------------
# Para adicionar novas tabelas no cleanup:
# 1. copie um item abaixo
# 2. ajuste task_id, step_name e target_name
# 3. escreva o SQL de delete da janela tecnica
# 4. ajuste a descricao para ficar clara para a operacao
#
# Exemplo de novo item:
# {
#     "task_id": "cleanup_ds_sx_estado",
#     "step_name": "CLEANUP_DS_SX_ESTADO",
#     "target_name": "SOLIX_BI.DS.SX_ESTADO_D",
#     "description": "retencao DS: manter 1 dia por BI_UPDATED_AT",
#     "sql": \"\"\"
# delete from SOLIX_BI.DS.SX_ESTADO_D
# where BI_UPDATED_AT < dateadd(day, -1, current_timestamp())
# \"\"\".strip(),
# },
# ============================================================================
RETENTION_SPECS = [
    {
        "task_id": "cleanup_ds_sx_cliente",
        "step_name": "CLEANUP_DS_SX_CLIENTE",
        "target_name": "SOLIX_BI.DS.SX_CLIENTE_D",
        "description": "retencao DS: manter apenas o dia atual por BI_UPDATED_AT",
        "sql": """
delete from SOLIX_BI.DS.SX_CLIENTE_D
where cast(BI_UPDATED_AT as date) < current_date()
""".strip(),
    },
    {
        "task_id": "cleanup_ds_sx_estado",
        "step_name": "CLEANUP_DS_SX_ESTADO",
        "target_name": "SOLIX_BI.DS.SX_ESTADO_D",
        "description": "retencao DS: manter apenas o dia atual por BI_UPDATED_AT",
        "sql": """
delete from SOLIX_BI.DS.SX_ESTADO_D
where cast(BI_UPDATED_AT as date) < current_date()
""".strip(),
    },
    {
        "task_id": "cleanup_ds_sx_equipamento",
        "step_name": "CLEANUP_DS_SX_EQUIPAMENTO",
        "target_name": "SOLIX_BI.DS.SX_EQUIPAMENTO_D",
        "description": "retencao DS: manter apenas o dia atual por BI_UPDATED_AT",
        "sql": """
delete from SOLIX_BI.DS.SX_EQUIPAMENTO_D
where cast(BI_UPDATED_AT as date) < current_date()
""".strip(),
    },
    {
        "task_id": "cleanup_ds_sx_operacao",
        "step_name": "CLEANUP_DS_SX_OPERACAO",
        "target_name": "SOLIX_BI.DS.SX_OPERACAO_D",
        "description": "retencao DS: manter apenas o dia atual por BI_UPDATED_AT",
        "sql": """
delete from SOLIX_BI.DS.SX_OPERACAO_D
where cast(BI_UPDATED_AT as date) < current_date()
""".strip(),
    },
]


def run_retention_cleanup(*, spec: dict[str, str], **context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    batch_id = dag_run.run_id
    step_start_time = time.time()
    started_at = datetime.utcnow()

    audit_load_audit(
        batch_id=batch_id,
        step_name=spec["step_name"],
        source_name="RETENTION",
        target_name=spec["target_name"],
        status=AUDIT_STATUS_STARTED,
        details=spec["description"],
        execution_order=spec.get("execution_order"),
        started_at=started_at,
    )

    try:
        execution_result = execute_snowflake_sql(sql=spec["sql"])
        rows_affected = execution_result.get("rows_affected")
        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()

        audit_load_audit(
            batch_id=batch_id,
            step_name=spec["step_name"],
            source_name="RETENTION",
            target_name=spec["target_name"],
            status=AUDIT_STATUS_SUCCESS,
            details="cleanup concluido",
            rows_processed=rows_affected,
            execution_order=spec.get("execution_order"),
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        return {
            "status": "SUCCESS",
            "target_name": spec["target_name"],
            "rows_affected": rows_affected,
        }
    except Exception as exc:
        duration_seconds = int(time.time() - step_start_time)
        ended_at = datetime.utcnow()

        audit_load_audit(
            batch_id=batch_id,
            step_name=spec["step_name"],
            source_name="RETENTION",
            target_name=spec["target_name"],
            status=AUDIT_STATUS_FAILED,
            details=str(exc),
            execution_order=spec.get("execution_order"),
            duration_seconds=duration_seconds,
            started_at=started_at,
            ended_at=ended_at,
        )
        raise


def register_cleanup_batch_start(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    audit_batch_execution_start(
        batch_id=dag_run.run_id,
        pipeline_name="cleanup_dimensions_retention_dag",
        source_name="RETENTION",
        target_name="DS",
        orchestration_type="MANUAL_TRIGGER" if dag_run.run_type == "manual" else "SCHEDULER",
    )
    return {"status": "STARTED"}


def register_cleanup_batch_end(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    total_rows_deleted = 0
    ti_context = context["ti"]
    failed_tasks = []
    for spec in RETENTION_SPECS:
        result = ti_context.xcom_pull(task_ids=spec["task_id"])
        if result is None:
            failed_tasks.append(spec["task_id"])
            continue
        if isinstance(result, dict):
            normalized_status = str(result.get("status", "")).upper()
            if normalized_status not in {"SUCCESS", "SKIPPED"}:
                failed_tasks.append(spec["task_id"])
            if result.get("rows_affected") is not None:
                total_rows_deleted += int(result["rows_affected"])
            continue
        failed_tasks.append(spec["task_id"])

    status = PIPELINE_STATUS_SUCCESS if not failed_tasks else PIPELINE_STATUS_FAILED
    error_message = None if not failed_tasks else f"tasks failed: {', '.join(failed_tasks)}"

    batch_start_time_seconds = dag_run.start_date.timestamp() if dag_run.start_date else time.time()
    duration_seconds = int(time.time() - batch_start_time_seconds)

    audit_batch_execution_end(
        batch_id=dag_run.run_id,
        pipeline_name="cleanup_dimensions_retention_dag",
        source_name="RETENTION",
        target_name="DS",
        status=status,
        error_message=error_message,
        duration_seconds=duration_seconds,
    )
    return {"status": status, "rows_deleted": total_rows_deleted}


with DAG(
    dag_id="cleanup_dimensions_retention_dag",
    description="Executa limpeza tecnica diaria de retencao em DS para a trilha de dimensoes.",
    start_date=datetime(2025, 1, 1),
    schedule="50 23 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cleanup", "retention", "ds", "dimensions"],
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
    register_batch_start_task = PythonOperator(
        task_id="register_cleanup_batch_start",
        python_callable=register_cleanup_batch_start,
        queue="dbt",
    )

    previous_task = register_batch_start_task

    for execution_order, spec in enumerate(RETENTION_SPECS, start=1):
        task_spec = {**spec, "execution_order": execution_order}
        task = PythonOperator(
            task_id=task_spec["task_id"],
            python_callable=run_retention_cleanup,
            op_kwargs={"spec": task_spec},
            queue="dbt",
            pool="retention_cleanup_pool",
        )

        if previous_task is not None:
            previous_task >> task

        previous_task = task

    register_batch_end_task = PythonOperator(
        task_id="register_cleanup_batch_end",
        python_callable=register_cleanup_batch_end,
        queue="dbt",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    if previous_task is not None:
        previous_task >> register_batch_end_task
