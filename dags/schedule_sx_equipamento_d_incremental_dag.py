from __future__ import annotations

"""Airflow DAG de agendamento incremental para SX_EQUIPAMENTO_D."""

from dags.pipeline_patterns import (
    DIMENSIONS_DAG_IDS,
    build_dimensions_incremental_conf,
    create_incremental_scheduler_dag,
)


dag = create_incremental_scheduler_dag(
    dag_id="schedule_sx_equipamento_d_incremental_dag",
    entity_label="sx_equipamento_d",
    schedule="15 * * * *",
    orchestrator_dag_id=DIMENSIONS_DAG_IDS["orchestrator"],
    tags=["dimensions"],
    conf=build_dimensions_incremental_conf(
        airbyte_connection_id="404f8969-e421-416e-96f6-cd0434047acf",
        models=["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"],
        entity_label="sx_equipamento_d",
        raw_tables=[
            "SOLIX_BI.RAW.CDT_EQUIPAMENTO",
            "SOLIX_BI.RAW.CDT_MODELO_EQUIPAMENTO",
            "SOLIX_BI.RAW.CDT_TIPO_EQUIPAMENTO",
            "SOLIX_BI.RAW.CDT_EQUIPAMENTO_HISTORICO_MOV",
        ],
        watermark_pipeline_name="dim_sx_equipamento_d",
    ),
)
