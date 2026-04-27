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
  - preservar o current-state sem delete fisico
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
    AREA_METRO,
    CD_PRODUTOR,
    DESC_PRODUTOR,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
    from {{ source('ds', SOURCE_TABLE_NAME) }}
),

typed_data as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_FAZENDA as varchar(20)) as CD_FAZENDA,
        cast(DESC_FAZENDA as varchar) as DESC_FAZENDA,
        cast(CD_TALHAO as varchar(20)) as CD_TALHAO,
        cast(DESC_TALHAO as varchar) as DESC_TALHAO,
        cast(CD_ZONA as varchar(20)) as CD_ZONA,
        cast(AREA_METRO as number(38, 8)) as AREA_METRO,
        cast(CD_PRODUTOR as number(38, 0)) as CD_PRODUTOR,
        cast(DESC_PRODUTOR as varchar) as DESC_PRODUTOR,
        cast(FG_ATIVO as boolean) as FG_ATIVO,
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
        partition by ID_CLIENTE, CD_FAZENDA, CD_TALHAO, CD_ZONA
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
    AREA_METRO,
    CD_PRODUTOR,
    DESC_PRODUTOR,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from deduplicated
