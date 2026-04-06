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

{% set SOURCE_TABLE_NAME = 'sx_fazenda_d' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_fazenda_d']
) }}

with source_data as (
    select
        ID_CLIENTE,
        CD_FAZENDA,
        DESC_FAZENDA,
        CD_TALHAO,
        DESC_TALHAO,
        CD_ZONA,
        AREA_TOTAL,
        DESC_PRODUTOR,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT
    from {{ source('ds', SOURCE_TABLE_NAME) }}
),

typed_data as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_FAZENDA as varchar) as CD_FAZENDA,
        cast(DESC_FAZENDA as varchar) as DESC_FAZENDA,
        cast(CD_TALHAO as varchar) as CD_TALHAO,
        cast(DESC_TALHAO as varchar) as DESC_TALHAO,
        cast(CD_ZONA as varchar) as CD_ZONA,
        cast(AREA_TOTAL as number(38, 8)) as AREA_TOTAL,
        cast(DESC_PRODUTOR as varchar) as DESC_PRODUTOR,
        cast(ETL_BATCH_ID as varchar) as ETL_BATCH_ID,
        cast(BI_CREATED_AT as timestamp_ntz) as BI_CREATED_AT,
        cast(BI_UPDATED_AT as timestamp_ntz) as BI_UPDATED_AT
    from source_data
),

deduplicated as (
    select
        *
    from typed_data
    qualify row_number() over (
        partition by ID_CLIENTE, CD_FAZENDA, CD_ZONA, CD_TALHAO
        order by BI_UPDATED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

select
    ID_CLIENTE,
    CD_FAZENDA,
    DESC_FAZENDA,
    CD_TALHAO,
    DESC_TALHAO,
    CD_ZONA,
    AREA_TOTAL,
    DESC_PRODUTOR,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from deduplicated
