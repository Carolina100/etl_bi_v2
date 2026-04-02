{#
  TEMPLATE STAGING DS

  TROQUE AQUI:
  1. SOURCE_TABLE_NAME
  2. TAGS
  3. COLUNAS DE ENTRADA
  4. CASTS
  5. CHAVE NATURAL NO QUALIFY

  O QUE ESTE MODELO FAZ:
  - le a tabela DS
  - padroniza tipos
  - remove duplicidade pela chave natural
  - entrega um staging limpo para a dimensao DW
#}

{% set SOURCE_TABLE_NAME = 'nome_da_tabela_d' %}

{{ config(
    materialized='view',
    tags=['staging', 'ds', 'nome_da_tabela_d']
) }}

with source_data as (
    select
        ID_CLIENTE,
        CODIGO_NEGOCIO,
        DESCRICAO_NEGOCIO,
        ETL_BATCH_ID,
        ETL_LOADED_AT
    from {{ source('ds', SOURCE_TABLE_NAME) }}
),

typed_data as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CODIGO_NEGOCIO as varchar) as CODIGO_NEGOCIO,
        cast(DESCRICAO_NEGOCIO as varchar) as DESCRICAO_NEGOCIO,
        cast(ETL_BATCH_ID as varchar) as ETL_BATCH_ID,
        cast(ETL_LOADED_AT as timestamp_ntz) as ETL_LOADED_AT
    from source_data
),

deduplicated as (
    select
        *
    from typed_data
    qualify row_number() over (
        partition by ID_CLIENTE, CODIGO_NEGOCIO
        order by ETL_LOADED_AT desc, ETL_BATCH_ID desc
    ) = 1
)

select
    ID_CLIENTE,
    CODIGO_NEGOCIO,
    DESCRICAO_NEGOCIO,
    ETL_BATCH_ID,
    ETL_LOADED_AT
from deduplicated
