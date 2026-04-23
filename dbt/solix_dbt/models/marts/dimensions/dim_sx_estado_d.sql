{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DIMENSIONAL DW                                                      ║
║                                                                              ║
║  DIMENSAO GLOBAL / CURRENT STATE                                             ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le do staging DS                                                          ║
║  - preserva a surrogate key em registros ja existentes                       ║
║  - usa sequence para novos registros                                         ║
║  - faz merge incremental pela chave natural                                  ║
║  - controla watermark global por PIPELINE_NAME                               ║
║  - garante a existencia do registro orfao                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_ESTADO_D' %}
{% set STAGING_MODEL_NAME = 'stg_ds__sx_estado_d' %}
{% set SEQUENCE_NAME = 'SOLIX_BI.DW.SEQ_SX_ESTADO_D' %}
{% set SURROGATE_KEY_COLUMN = 'SK_ESTADO' %}
{% set NATURAL_KEY_COLUMNS = ['CD_ESTADO'] %}
{% set WATERMARK_PIPELINE_NAME = 'dim_sx_estado_d' %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    post_hook=[
      "
      merge into SOLIX_BI.DS.CTL_PIPELINE_WATERMARK as tgt
      using (
          select
              '" ~ WATERMARK_PIPELINE_NAME ~ "' as PIPELINE_NAME,
              max(BI_UPDATED_AT) as LAST_BI_UPDATED_AT,
              '" ~ invocation_id ~ "' as LAST_SUCCESS_BATCH_ID,
              'INCREMENTAL_WATERMARK' as LAST_LOAD_MODE,
              '" ~ invocation_id ~ "' as LAST_RUN_BATCH_ID,
              'SUCCESS' as LAST_RUN_STATUS,
              null as LAST_ERROR_MESSAGE,
              convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_RUN_STARTED_AT,
              convert_timezone('UTC', current_timestamp())::timestamp_ntz as LAST_RUN_COMMITTED_AT,
              convert_timezone('UTC', current_timestamp())::timestamp_ntz as UPDATED_AT
          from {{ this }}
          where " ~ SURROGATE_KEY_COLUMN ~ " <> -1
      ) as src
      on tgt.PIPELINE_NAME = src.PIPELINE_NAME
      when matched then update set
          tgt.LAST_BI_UPDATED_AT = src.LAST_BI_UPDATED_AT,
          tgt.LAST_SUCCESS_BATCH_ID = src.LAST_SUCCESS_BATCH_ID,
          tgt.LAST_LOAD_MODE = src.LAST_LOAD_MODE,
          tgt.LAST_RUN_BATCH_ID = src.LAST_RUN_BATCH_ID,
          tgt.LAST_RUN_STATUS = src.LAST_RUN_STATUS,
          tgt.LAST_ERROR_MESSAGE = src.LAST_ERROR_MESSAGE,
          tgt.LAST_RUN_STARTED_AT = src.LAST_RUN_STARTED_AT,
          tgt.LAST_RUN_COMMITTED_AT = src.LAST_RUN_COMMITTED_AT,
          tgt.UPDATED_AT = src.UPDATED_AT
      when not matched then insert (
          PIPELINE_NAME,
          LAST_BI_UPDATED_AT,
          LAST_SUCCESS_BATCH_ID,
          LAST_LOAD_MODE,
          LAST_RUN_BATCH_ID,
          LAST_RUN_STATUS,
          LAST_ERROR_MESSAGE,
          LAST_RUN_STARTED_AT,
          LAST_RUN_COMMITTED_AT,
          UPDATED_AT
      ) values (
          src.PIPELINE_NAME,
          src.LAST_BI_UPDATED_AT,
          src.LAST_SUCCESS_BATCH_ID,
          src.LAST_LOAD_MODE,
          src.LAST_RUN_BATCH_ID,
          src.LAST_RUN_STATUS,
          src.LAST_ERROR_MESSAGE,
          src.LAST_RUN_STARTED_AT,
          src.LAST_RUN_COMMITTED_AT,
          src.UPDATED_AT
      )
      "
    ],
    tags=['dw', 'dimension', 'sx_estado_d']
) }}

with watermark_control as (
    select
        coalesce(max(LAST_BI_UPDATED_AT), '1900-01-01'::timestamp_ntz) as LAST_BI_UPDATED_AT
    from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
    where PIPELINE_NAME = '{{ WATERMARK_PIPELINE_NAME }}'
),

staged_source as (
    select
        s.CD_ESTADO,
        s.DESC_ESTADO,
        s.DESC_ESTADO_EN_US,
        s.DESC_ESTADO_PT_BR,
        s.DESC_ESTADO_ES_ES,
        s.ETL_BATCH_ID,
        s.BI_CREATED_AT,
        s.BI_UPDATED_AT
    from {{ ref(STAGING_MODEL_NAME) }} s
    cross join watermark_control w
    where 1 = 1
    {% if is_incremental() %}
      and s.BI_UPDATED_AT > coalesce(w.LAST_BI_UPDATED_AT, '1900-01-01'::timestamp_ntz)
    {% endif %}
),

existing_dimension as (
    {% if is_incremental() %}
    select
        {{ SURROGATE_KEY_COLUMN }},
        CD_ESTADO
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as {{ SURROGATE_KEY_COLUMN }},
        cast(null as varchar(50)) as CD_ESTADO
    where 1 = 0
    {% endif %}
),

business_rows as (
    select
        coalesce(existing_dimension.{{ SURROGATE_KEY_COLUMN }}, {{ SEQUENCE_NAME }}.nextval) as {{ SURROGATE_KEY_COLUMN }},
        staged_source.CD_ESTADO,
        upper(staged_source.DESC_ESTADO) as DESC_ESTADO,
        upper(staged_source.DESC_ESTADO_EN_US) as DESC_ESTADO_EN_US,
        upper(staged_source.DESC_ESTADO_PT_BR) as DESC_ESTADO_PT_BR,
        upper(staged_source.DESC_ESTADO_ES_ES) as DESC_ESTADO_ES_ES,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
    left join existing_dimension
        on staged_source.CD_ESTADO = existing_dimension.CD_ESTADO
),

orphan_row as (
    select
        cast(-1 as number(38, 0)) as {{ SURROGATE_KEY_COLUMN }},
        cast('-1' as varchar(50)) as CD_ESTADO,
        cast('UNDEFINED' as varchar) as DESC_ESTADO,
        cast('UNDEFINED' as varchar) as DESC_ESTADO_EN_US,
        cast('UNDEFINED' as varchar) as DESC_ESTADO_PT_BR,
        cast('UNDEFINED' as varchar) as DESC_ESTADO_ES_ES,
        cast('{{ invocation_id }}' as varchar) as ETL_BATCH_ID,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_CREATED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_UPDATED_AT
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
    {{ SURROGATE_KEY_COLUMN }},
    CD_ESTADO,
    DESC_ESTADO,
    DESC_ESTADO_EN_US,
    DESC_ESTADO_PT_BR,
    DESC_ESTADO_ES_ES,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from final
