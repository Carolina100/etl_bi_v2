{#
  MODELO STAGING

  AO REUTILIZAR PARA OUTRA TABELA, NORMALMENTE TROQUE:
  1. SOURCE_TABLE_NAME
  2. os casts/colunas do select final
  3. a regra de deduplicacao, se a chave natural mudar

  OBJETIVO:
  - padronizar tipos
  - manter apenas a linha mais recente por chave natural
  - entregar um modelo limpo para a camada DW
#}

{% set SOURCE_TABLE_NAME = 'sx_estado_d' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_estado_d']
) }}

with source_data as (
    select
        CD_ESTADO,
        DESC_ESTADO,
        DESC_ESTADO_EN_US,
        DESC_ESTADO_PT_BR,
        DESC_ESTADO_ES_ES,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT,
        AIRBYTE_EXTRACTED_AT
    from {{ source('ds', SOURCE_TABLE_NAME) }}
),

typed_data as (
    select
        cast(CD_ESTADO as varchar(50)) as CD_ESTADO,
        cast(DESC_ESTADO as varchar) as DESC_ESTADO,
        cast(DESC_ESTADO_EN_US as varchar) as DESC_ESTADO_EN_US,
        cast(DESC_ESTADO_PT_BR as varchar) as DESC_ESTADO_PT_BR,
        cast(DESC_ESTADO_ES_ES as varchar) as DESC_ESTADO_ES_ES,
        cast(ETL_BATCH_ID as varchar) as ETL_BATCH_ID,
        cast(BI_CREATED_AT as timestamp_ntz) as BI_CREATED_AT,
        cast(BI_UPDATED_AT as timestamp_ntz) as BI_UPDATED_AT,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from source_data
),

deduplicated as (
    select
        *
    from typed_data
    qualify row_number() over (
        partition by CD_ESTADO
        order by BI_UPDATED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

select
    CD_ESTADO,
    DESC_ESTADO,
    DESC_ESTADO_EN_US,
    DESC_ESTADO_PT_BR,
    DESC_ESTADO_ES_ES,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from deduplicated
