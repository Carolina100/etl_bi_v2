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

{% set SOURCE_TABLE_NAME = 'sx_operacao_d' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'sx_operacao_d']
) }}

with source_data as (
    select
    ID_CLIENTE,
    CD_OPERACAO,
    DESC_OPERACAO,
    CD_GRUPO_OPERACAO,
    DESC_GRUPO_OPERACAO,
    CD_GRUPO_PARADA,
    FG_TIPO_OPERACAO,
    CD_PROCESSO,
    DESC_PROCESSO,
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
        cast(CD_OPERACAO as number(38, 0)) as CD_OPERACAO,
        cast(DESC_OPERACAO as varchar) as DESC_OPERACAO,
        cast(CD_GRUPO_OPERACAO as number(38, 0)) as CD_GRUPO_OPERACAO,
        cast(DESC_GRUPO_OPERACAO as varchar) as DESC_GRUPO_OPERACAO,
        cast(CD_GRUPO_PARADA as number(38, 0)) as CD_GRUPO_PARADA,
        cast(FG_TIPO_OPERACAO as varchar) as FG_TIPO_OPERACAO,
        cast(CD_PROCESSO as number(38, 0)) as CD_PROCESSO,
        cast(DESC_PROCESSO as varchar) as DESC_PROCESSO,
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
        partition by ID_CLIENTE, CD_OPERACAO
        order by BI_UPDATED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

select
    ID_CLIENTE,
    CD_OPERACAO,
    DESC_OPERACAO,
    CD_GRUPO_OPERACAO,
    DESC_GRUPO_OPERACAO,
    CD_GRUPO_PARADA,
    FG_TIPO_OPERACAO,
    CD_PROCESSO,
    DESC_PROCESSO,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from deduplicated
