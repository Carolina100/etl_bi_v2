{#
  TEMPLATE DIMENSAO DW

  TROQUE AQUI:
  1. MODEL_ALIAS
  2. STAGING_MODEL_NAME
  3. SEQUENCE_NAME
  4. SURROGATE_KEY_COLUMN
  5. NATURAL_KEY_COLUMNS
  6. TAGS
  7. CAMPOS DE NEGOCIO
  8. REGISTRO ORFAO

  O QUE ESTE MODELO FAZ:
  - le o staging DS
  - preserva surrogate key ja existente
  - gera SK nova via sequence quando necessario
  - faz merge incremental pela chave natural
  - garante o registro orfao
#}

{% set MODEL_ALIAS = 'NOME_DA_TABELA_D' %}
{% set STAGING_MODEL_NAME = 'stg_ds__nome_da_tabela_d' %}
{% set SEQUENCE_NAME = 'SOLIX_BI.DW.SEQ_NOME_DA_TABELA_D' %}
{% set SURROGATE_KEY_COLUMN = 'SK_NOME_ENTIDADE' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CODIGO_NEGOCIO'] %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['dw', 'dimension', 'nome_da_tabela_d']
) }}

with staged_source as (
    select
        ID_CLIENTE,
        CODIGO_NEGOCIO,
        DESCRICAO_NEGOCIO,
        ETL_BATCH_ID,
        ETL_LOADED_AT
    from {{ ref(STAGING_MODEL_NAME) }}
),

existing_dimension as (
    {% if is_incremental() %}
    select
        {{ SURROGATE_KEY_COLUMN }} as SK_ENTIDADE,
        ID_CLIENTE,
        CODIGO_NEGOCIO
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as SK_ENTIDADE,
        cast(null as number(38, 0)) as ID_CLIENTE,
        cast(null as varchar) as CODIGO_NEGOCIO
    where 1 = 0
    {% endif %}
),

business_rows as (
    select
        coalesce(existing_dimension.SK_ENTIDADE, {{ SEQUENCE_NAME }}.nextval) as SK_ENTIDADE,
        staged_source.ID_CLIENTE,
        staged_source.CODIGO_NEGOCIO,
        staged_source.DESCRICAO_NEGOCIO,
        staged_source.ETL_BATCH_ID,
        convert_timezone('America/Sao_Paulo', current_timestamp())::timestamp_ntz as ETL_LOADED_AT
    from staged_source
    left join existing_dimension
        on staged_source.ID_CLIENTE = existing_dimension.ID_CLIENTE
       and staged_source.CODIGO_NEGOCIO = existing_dimension.CODIGO_NEGOCIO
),

orphan_row as (
    select
        cast(-1 as number(38, 0)) as SK_ENTIDADE,
        cast(-1 as number(38, 0)) as ID_CLIENTE,
        cast('-1' as varchar) as CODIGO_NEGOCIO,
        cast('NAO INFORMADO' as varchar) as DESCRICAO_NEGOCIO,
        cast('DBT_ORPHAN_ROW' as varchar) as ETL_BATCH_ID,
        convert_timezone('America/Sao_Paulo', current_timestamp())::timestamp_ntz as ETL_LOADED_AT
),

final as (
    select * from orphan_row
    union all
    select * from business_rows
)

select
    SK_ENTIDADE,
    ID_CLIENTE,
    CODIGO_NEGOCIO,
    DESCRICAO_NEGOCIO,
    ETL_BATCH_ID,
    ETL_LOADED_AT
from final
