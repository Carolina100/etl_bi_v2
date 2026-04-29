from __future__ import annotations

import os

from airflow import DAG

"""
Scheduler global de dimensoes.

DESENHO OPERACIONAL NOVO
------------------------
Antes:
- uma DAG de schedule por entidade
- cada scheduler parecia controlar uma extracao dedicada do Airbyte

Agora:
- uma unica connection do Airbyte sincroniza varias streams de dimensao
- o scheduler passa a representar o dominio "dimensions"
- o dbt continua recebendo os modelos explicitamente no conf

POR QUE ISSO EXISTE
-------------------
Quando varias dimensoes compartilham a mesma connection do Airbyte, nao faz mais
sentido ter uma DAG de agendamento com nome de entidade, porque:

- ao rodar "equipamento", o Airbyte traria outras dimensoes tambem
- o nome da DAG ficaria enganoso para a operacao
- o cleanup do RAW deixaria de refletir o escopo real da extracao

COMO EVOLUIR
------------
Conforme novas dimensoes entrarem na mesma connection:

1. adicionar os modelos dbt no bloco "models"
2. adicionar as tabelas RAW correspondentes em "raw_tables"
3. manter o schedule do dominio, nao criar novo scheduler por entidade

Se no futuro as facts tiverem alta frequencia ou outra connection, elas devem
ficar em outro scheduler de dominio, separado de dimensions.
"""

from dags.pipeline_patterns import (
    DIMENSIONS_DAG_IDS,
    build_dimensions_domain_conf,
    create_domain_scheduler_dag,
)


# Referencia explicita a DAG no proprio arquivo para ajudar a descoberta
# quando o Airflow estiver com dag_discovery_safe_mode habilitado.
dag: DAG = create_domain_scheduler_dag(
    dag_id="schedule_dimensions_incremental_dag",
    domain_label="dimensions",
    schedule="0 6,18 * * *",
    orchestrator_dag_id=DIMENSIONS_DAG_IDS["orchestrator"],
    tags=["dimensions"],
    conf=build_dimensions_domain_conf(
        # Em producao, injete este valor via env/secret manager por ambiente.
        # Se vazio, a DAG de extracao falha explicitamente exigindo configuracao.
        airbyte_connection_id=os.getenv("AIRBYTE_DIMENSIONS_CONNECTION_ID", ""),
        models=[
            # Dimensoes ja implementadas no dominio atual.
            "ds_sx_cliente_d",
            "stg_ds__sx_cliente_d",
            "dim_sx_cliente_d",
            "ds_sx_estado_d",
            "stg_ds__sx_estado_d",
            "dim_sx_estado_d",
            "ds_sx_equipamento_d",
            "stg_ds__sx_equipamento_d",
            "dim_sx_equipamento_d",
            "ds_sx_operacao_d",
            "stg_ds__sx_operacao_d",
            "dim_sx_operacao_d",
            "ds_sx_fazenda_d",
            "stg_ds__sx_fazenda_d",
            "dim_sx_fazenda_d",
            "ds_sx_frente_d",
            "stg_ds__sx_frente_d",
            "dim_sx_frente_d",
            # Quando novas dimensoes entrarem na mesma connection do Airbyte,
            # adicionar os modelos aqui.
        ],
        raw_tables=[
            # RAWs pertencentes a mesma connection de dimensoes.
            "SOLIX_BI.RAW.VW_SX_CLIENTE_D",
            "SOLIX_BI.RAW.VW_SX_ESTADO_D",
            "SOLIX_BI.RAW.VW_SX_EQUIPAMENTO_D",
            "SOLIX_BI.RAW.VW_SX_OPERACAO_D",
            "SOLIX_BI.RAW.VW_SX_FAZENDA_D",
            "SOLIX_BI.RAW.VW_SX_FRENTE_D",
            # Quando outras views de dimensao entrarem na mesma connection,
            # adicionar aqui tambem.
        ],
        domain_label="dimensions",
        # Na extracao Airbyte do dominio, usamos um identificador tecnico do
        # proprio dominio para nao confundir watermark macro de extracao com
        # watermarks individuais das dimensoes no dbt.
        watermark_pipeline_name="dimensions_domain",
    ),
)
