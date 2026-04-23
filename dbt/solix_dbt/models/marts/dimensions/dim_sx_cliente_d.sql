{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DIMENSIONAL DW                                                      ║
║                                                                              ║
║  DIMENSAO GLOBAL / CURRENT STATE                                             ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le do staging DS                                                          ║
║  - usa o proprio ID_CLIENTE como chave da dimensao                           ║
║  - faz merge incremental pela chave natural                                  ║
║  - controla watermark global por PIPELINE_NAME                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_CLIENTE_D' %}
{% set STAGING_MODEL_NAME = 'stg_ds__sx_cliente_d' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE'] %}
{% set WATERMARK_PIPELINE_NAME = 'dim_sx_cliente_d' %}

{{ config(
    materialized='incremental',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    post_hook=[
      "
      delete from {{ this }}
      where ID_CLIENTE = -1
      ",
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
          where ID_CLIENTE <> -1
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
    tags=['dw', 'dimension', 'sx_cliente_d']
) }}

with watermark_control as (
    select
        coalesce(max(LAST_BI_UPDATED_AT), '1900-01-01'::timestamp_ntz) as LAST_BI_UPDATED_AT
    from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
    where PIPELINE_NAME = '{{ WATERMARK_PIPELINE_NAME }}'
),

staged_source as (
    select
        s.ID_CLIENTE,
        s.NOME_CLIENTE,
        s.OWNER_CLIENTE,
        s.SERVER_CLIENTE,
        s.FG_ATIVO,
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

business_rows as (
    select
        staged_source.ID_CLIENTE,
        upper(staged_source.NOME_CLIENTE) as NOME_CLIENTE,
        upper(staged_source.OWNER_CLIENTE) as OWNER_CLIENTE,
        upper(staged_source.SERVER_CLIENTE) as SERVER_CLIENTE,
        staged_source.FG_ATIVO,
        staged_source.ETL_BATCH_ID,
        staged_source.BI_CREATED_AT,
        staged_source.BI_UPDATED_AT
    from staged_source
),

final as (
    select * from business_rows
)

select
    ID_CLIENTE,
    NOME_CLIENTE,
    OWNER_CLIENTE,
    SERVER_CLIENTE,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT
from final
