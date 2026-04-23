{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW -> DS (incremental por merge)                              ║
║                                                                              ║
║  ENTIDADE CURADA NA ORIGEM                                                   ║
║                                                                              ║
║  A origem atual do Airbyte e uma view PostgreSQL ja consolidada:             ║
║  bi.vw_sx_estado_d                                                           ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le uma RAW unica gerada pelo Airbyte                                      ║
║  - padroniza tipos para o contrato DS                                        ║
║  - mantem 1 linha por chave natural no DS                                    ║
║  - nao faz delete fisico da entidade                                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_ESTADO_D' %}
{% set NATURAL_KEY_COLUMNS = ['CD_ESTADO'] %}
{% set RAW_SOURCE_TABLE = 'SOLIX_BI.RAW.VW_SX_ESTADO_D' %}
{% set BATCH_ID = invocation_id %}

{{ config(
    materialized='incremental',
    schema='DS',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['ds', 'incremental', MODEL_ALIAS | lower]
) }}

with raw_source as (
    select
        cast(CD_ESTADO as varchar(50)) as CD_ESTADO,
        cast(DESC_ESTADO as varchar) as DESC_ESTADO,
        cast(DESC_ESTADO_EN_US as varchar) as DESC_ESTADO_EN_US,
        cast(DESC_ESTADO_PT_BR as varchar) as DESC_ESTADO_PT_BR,
        cast(DESC_ESTADO_ES_ES as varchar) as DESC_ESTADO_ES_ES,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ RAW_SOURCE_TABLE }}
),

latest_source as (
    select *
    from raw_source
    qualify row_number() over (
        partition by CD_ESTADO
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

current_target as (
    {% if is_incremental() %}
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
    from {{ this }}
    {% else %}
    select
        cast(null as varchar(50)) as CD_ESTADO,
        cast(null as varchar) as DESC_ESTADO,
        cast(null as varchar) as DESC_ESTADO_EN_US,
        cast(null as varchar) as DESC_ESTADO_PT_BR,
        cast(null as varchar) as DESC_ESTADO_ES_ES,
        cast(null as varchar) as ETL_BATCH_ID,
        cast(null as timestamp_ntz) as BI_CREATED_AT,
        cast(null as timestamp_ntz) as BI_UPDATED_AT,
        cast(null as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(null as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    where 1 = 0
    {% endif %}
),

changed_or_new as (
    select
        s.CD_ESTADO,
        s.DESC_ESTADO,
        s.DESC_ESTADO_EN_US,
        s.DESC_ESTADO_PT_BR,
        s.DESC_ESTADO_ES_ES,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,
        case
            when t.CD_ESTADO is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_ESTADO, '') <> coalesce(s.DESC_ESTADO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_ESTADO_EN_US, '') <> coalesce(s.DESC_ESTADO_EN_US, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_ESTADO_PT_BR, '') <> coalesce(s.DESC_ESTADO_PT_BR, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_ESTADO_ES_ES, '') <> coalesce(s.DESC_ESTADO_ES_ES, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            else t.BI_UPDATED_AT
        end as BI_UPDATED_AT,
        s.SOURCE_UPDATED_AT,
        s.AIRBYTE_EXTRACTED_AT
    from latest_source s
    left join current_target t
        on s.CD_ESTADO = t.CD_ESTADO
    where
        t.CD_ESTADO is null
        or coalesce(t.DESC_ESTADO, '') <> coalesce(s.DESC_ESTADO, '')
        or coalesce(t.DESC_ESTADO_EN_US, '') <> coalesce(s.DESC_ESTADO_EN_US, '')
        or coalesce(t.DESC_ESTADO_PT_BR, '') <> coalesce(s.DESC_ESTADO_PT_BR, '')
        or coalesce(t.DESC_ESTADO_ES_ES, '') <> coalesce(s.DESC_ESTADO_ES_ES, '')
        or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
           <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
        or coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
           <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
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
from changed_or_new
