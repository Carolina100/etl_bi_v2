{#
  MODELO DIMENSIONAL DW

  AO REUTILIZAR PARA OUTRA DIMENSAO, NORMALMENTE TROQUE:
  1. MODEL_ALIAS
     Nome final da tabela no DW.

  2. STAGING_MODEL_NAME
     Nome do model staging que alimenta esta dimensao.

  3. SEQUENCE_NAME
     Sequence usada para gerar a surrogate key.

  4. SURROGATE_KEY_COLUMN
     Nome da surrogate key da dimensao.

  5. NATURAL_KEY_COLUMNS
     Chave natural da dimensao usada no merge.

  6. ORPHAN ROW
     Valores padrao do registro orfao.
     Padrao moderno sugerido:
     - surrogate key = -1
     - codigos = '-1'
     - descricoes = 'NAO INFORMADO'

  7. select final de business_rows
     Ajuste os campos para refletir a nova dimensao.

  O QUE ESTE MODELO FAZ:
  - le do staging DS
  - preserva a surrogate key em registros ja existentes
  - usa sequence para novos registros
  - faz merge incremental pela chave natural
  - garante a existencia do registro orfao
#}

{% set MODEL_ALIAS = 'SX_ESTADO_D' %}
{% set STAGING_MODEL_NAME = 'stg_ds__sx_estado_d' %}
{% set SEQUENCE_NAME = 'SOLIX_BI.DW.SEQ_SX_ESTADO_D' %}
{% set SURROGATE_KEY_COLUMN = 'SK_ESTADO' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_ESTADO'] %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['dw', 'dimension', 'sx_estado_d']
) }}

with staged_source as (
    select
        ID_CLIENTE,
        CD_ESTADO,
        DESC_ESTADO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT
    from {{ ref(STAGING_MODEL_NAME) }}
    {% if is_incremental() %}
    where BI_UPDATED_AT > (
        select coalesce(max(BI_UPDATED_AT), '1900-01-01'::timestamp_ntz)
        from {{ this }}
        where {{ SURROGATE_KEY_COLUMN }} <> -1
    )
    {% endif %}
),

existing_dimension as (
    {% if is_incremental() %}
    select
        {{ SURROGATE_KEY_COLUMN }} as SK_ESTADO,
        ID_CLIENTE,
        CD_ESTADO
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as SK_ESTADO,
        cast(null as number(38, 0)) as ID_CLIENTE,
        cast(null as varchar) as CD_ESTADO
    where 1 = 0
    {% endif %}
),

business_rows as (
    select
        coalesce(existing_dimension.SK_ESTADO, {{ SEQUENCE_NAME }}.nextval) as SK_ESTADO,
        staged_source.ID_CLIENTE,
        staged_source.CD_ESTADO,
        upper(staged_source.DESC_ESTADO) as DESC_ESTADO,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
    left join existing_dimension
        on staged_source.ID_CLIENTE = existing_dimension.ID_CLIENTE
       and staged_source.CD_ESTADO = existing_dimension.CD_ESTADO
),

orphan_row as (
    select
        cast(-1 as number(38, 0)) as SK_ESTADO,
        cast(-1 as number(38, 0)) as ID_CLIENTE,
        cast('-1' as varchar) as CD_ESTADO,
        cast('UNDEFINED' as varchar) as DESC_ESTADO,
        cast('DBT_ORPHAN_ROW' as varchar) as ETL_BATCH_ID,
        convert_timezone('America/Sao_Paulo', current_timestamp())::timestamp_ntz as BI_CREATED_AT,
        convert_timezone('America/Sao_Paulo', current_timestamp())::timestamp_ntz as BI_UPDATED_AT
),    

final as (
    {% if is_incremental() %}
    select * from business_rows
    {% else %}
    select * from orphan_row
    union all
    select * from business_rows
    {% endif %}
)

select
    SK_ESTADO,
    ID_CLIENTE,
    CD_ESTADO,
    DESC_ESTADO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from final
