from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from src.utils.airflow_helpers import (
    airflow_failure_alert_callback,
    airflow_retry_alert_callback,
)


DIMENSIONS_DAG_IDS = {
    "airbyte": "load_ds_airbyte_dimensions_dag",
    "dbt": "load_dw_dbt_dimensions_dag",
    "orchestrator": "orchestrate_ds_dw_dimensions_dag",
    "cleanup": "cleanup_dimensions_retention_dag",
}

FACTS_DAG_IDS = {
    "airbyte": "load_ds_airbyte_facts_dag",
    "dbt": "load_dw_dbt_facts_dag",
    "orchestrator": "orchestrate_ds_dw_facts_dag",
    "cleanup": "cleanup_facts_retention_dag",
}


def build_raw_cleanup_specs(*, raw_tables: list[str], entity_label: str) -> list[dict[str, str]]:
    cleanup_specs: list[dict[str, str]] = []
    for raw_table in raw_tables:
        table_name = raw_table.split(".")[-1]
        cleanup_specs.append(
            {
                "step_name": f"CLEANUP_RAW_{table_name}",
                "target_name": raw_table,
                "description": f"cleanup tecnico do RAW de {entity_label} apos sucesso do pipeline",
                "sql": f"delete from {raw_table}",
            }
        )
    return cleanup_specs


def build_dimensions_incremental_conf(
    *,
    airbyte_connection_id: str,
    models: list[str],
    entity_label: str,
    raw_tables: list[str],
    watermark_pipeline_name: str,
    dbt_vars: dict[str, Any] | None = None,
    dbt_command: str = "run",
    airbyte_timeout_seconds: int = 3600,
    airbyte_poll_interval_seconds: int = 15,
) -> dict[str, Any]:
    return {
        "airbyte_connection_id": airbyte_connection_id,
        "models": models,
        "dbt_vars": dbt_vars or {},
        "dbt_command": dbt_command,
        "cleanup_raw_specs": build_raw_cleanup_specs(
            raw_tables=raw_tables,
            entity_label=entity_label,
        ),
        "watermark_pipeline_name": watermark_pipeline_name,
        "airbyte_timeout_seconds": airbyte_timeout_seconds,
        "airbyte_poll_interval_seconds": airbyte_poll_interval_seconds,
    }


def build_dimensions_domain_conf(
    *,
    airbyte_connection_id: str,
    models: list[str],
    raw_tables: list[str],
    watermark_pipeline_name: str,
    domain_label: str = "dimensions",
    dbt_vars: dict[str, Any] | None = None,
    dbt_command: str = "run",
    airbyte_timeout_seconds: int = 3600,
    airbyte_poll_interval_seconds: int = 15,
) -> dict[str, Any]:
    """
    Configuracao para o desenho novo de dimensoes:

    - uma unica connection do Airbyte sincroniza varias streams de dimensao
    - o scheduler do Airflow passa a orquestrar o dominio de dimensoes, nao uma entidade isolada
    - o dbt continua recebendo uma lista explicita de modelos para transformar
    - o cleanup do RAW precisa considerar todas as tabelas RAW dessa mesma connection

    Este helper existe para deixar explicita a diferenca entre:
    - scheduler por entidade
    - scheduler por dominio
    """
    return {
        "airbyte_connection_id": airbyte_connection_id,
        "models": models,
        "dbt_vars": dbt_vars or {},
        "dbt_command": dbt_command,
        "cleanup_raw_specs": build_raw_cleanup_specs(
            raw_tables=raw_tables,
            entity_label=domain_label,
        ),
        "watermark_pipeline_name": watermark_pipeline_name,
        "airbyte_timeout_seconds": airbyte_timeout_seconds,
        "airbyte_poll_interval_seconds": airbyte_poll_interval_seconds,
    }


def create_orchestration_scheduler_dag(
    *,
    dag_id: str,
    description: str,
    schedule: str | None,
    tags: list[str],
    orchestrator_dag_id: str,
    conf: dict[str, Any],
) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2025, 1, 1),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        tags=tags,
        default_args={
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "on_failure_callback": airflow_failure_alert_callback,
            "on_retry_callback": airflow_retry_alert_callback,
        },
        on_failure_callback=airflow_failure_alert_callback,
    ) as dag:
        trigger_pipeline = TriggerDagRunOperator(
            task_id="trigger_pipeline",
            trigger_dag_id=orchestrator_dag_id,
            queue="dbt",
            conf=conf,
            wait_for_completion=True,
            allowed_states=["success"],
            failed_states=["failed"],
            poke_interval=30,
        )

        trigger_pipeline

    return dag


def create_incremental_scheduler_dag(
    *,
    dag_id: str,
    entity_label: str,
    schedule: str,
    orchestrator_dag_id: str,
    conf: dict[str, Any],
    tags: list[str] | None = None,
) -> DAG:
    scheduler_tags = [
        "airbyte",
        "ds",
        "incremental",
        "scheduler",
        entity_label,
    ]
    if tags:
        scheduler_tags.extend(tags)

    return create_orchestration_scheduler_dag(
        dag_id=dag_id,
        description=f"Agenda a execucao incremental da trilha para {entity_label}.",
        schedule=schedule,
        tags=scheduler_tags,
        orchestrator_dag_id=orchestrator_dag_id,
        conf=conf,
    )


def create_domain_scheduler_dag(
    *,
    dag_id: str,
    domain_label: str,
    schedule: str,
    orchestrator_dag_id: str,
    conf: dict[str, Any],
    tags: list[str] | None = None,
) -> DAG:
    """
    Scheduler por dominio operacional.

    Usar quando o Airbyte estiver organizado por connection compartilhada, por exemplo:
    - uma connection para todas as dimensoes
    - uma connection para um grupo de fatos

    Nessa abordagem:
    - a extracao do Airbyte e global para o dominio
    - o dbt recebe os modelos do mesmo dominio
    - o cleanup do RAW e tratado no mesmo pacote operacional
    """
    scheduler_tags = [
        "airbyte",
        "scheduler",
        domain_label,
        "domain",
    ]
    if tags:
        scheduler_tags.extend(tags)

    return create_orchestration_scheduler_dag(
        dag_id=dag_id,
        description=f"Agenda a execucao incremental do dominio {domain_label}.",
        schedule=schedule,
        tags=scheduler_tags,
        orchestrator_dag_id=orchestrator_dag_id,
        conf=conf,
    )
