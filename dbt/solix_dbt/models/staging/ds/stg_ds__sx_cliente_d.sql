{#
  MODELO STAGING

  OBJETIVO:
  - padronizar tipos
  - manter apenas a linha mais recente por chave natural
  - entregar um modelo limpo para a camada DW
#}

{% set SOURCE_TABLE_NAME = 'sx_cliente_d' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_cliente_d']
) }}

with source_data as (
    select
        ID_CLIENTE,
        NOME_CLIENTE,
        OWNER_CLIENTE,
        SERVER_CLIENTE,
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
        cast(NOME_CLIENTE as varchar) as NOME_CLIENTE,
        cast(OWNER_CLIENTE as varchar) as OWNER_CLIENTE,
        cast(SERVER_CLIENTE as varchar) as SERVER_CLIENTE,
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
        partition by ID_CLIENTE
        order by BI_UPDATED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

select
    ID_CLIENTE,
    NOME_CLIENTE,
    OWNER_CLIENTE,
    SERVER_CLIENTE,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from deduplicated
