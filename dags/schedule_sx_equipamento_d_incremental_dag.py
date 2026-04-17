from __future__ import annotations

from dags.pipeline_patterns import (
    DIMENSIONS_DAG_IDS,
    build_raw_cleanup_specs,
    create_orchestration_scheduler_dag,
)


dag = create_orchestration_scheduler_dag(
    dag_id="schedule_sx_equipamento_d_incremental_dag",
    description="Agenda a execucao incremental da trilha de dimensoes para SX_EQUIPAMENTO_D.",
    schedule="15 * * * *",
    tags=["airbyte", "ds", "incremental", "scheduler", "sx_equipamento_d", "dimensions"],
    orchestrator_dag_id=DIMENSIONS_DAG_IDS["orchestrator"],
    conf={
        "airbyte_connection_id": "404f8969-e421-416e-96f6-cd0434047acf",
        "models": ["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"],
        "dbt_vars": {},
        "cleanup_raw_specs": build_raw_cleanup_specs(
            raw_tables=[
                "SOLIX_BI.RAW.CDT_EQUIPAMENTO",
                "SOLIX_BI.RAW.CDT_MODELO_EQUIPAMENTO",
                "SOLIX_BI.RAW.CDT_TIPO_EQUIPAMENTO",
                "SOLIX_BI.RAW.CDT_EQUIPAMENTO_HISTORICO_MOV",
            ],
            entity_label="sx_equipamento_d",
        ),
        "watermark_pipeline_name": "dim_sx_equipamento_d",
        "airbyte_timeout_seconds": 3600,
        "airbyte_poll_interval_seconds": 15,
    },
)
