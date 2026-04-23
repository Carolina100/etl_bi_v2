{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW -> DS (incremental por merge)                              ║
║                                                                              ║
║  ENTIDADE CURADA NA ORIGEM                                                   ║
║                                                                              ║
║  A origem atual do Airbyte e uma view PostgreSQL ja consolidada:             ║
║  bi.vw_sx_equipamento_d                                                      ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - le uma RAW unica gerada pelo Airbyte                                       ║
║  - padroniza tipos para o contrato DS                                         ║
║  - mantem 1 linha por chave natural no DS                                     ║
║  - trata inativacao como update da mesma linha                                ║
║  - nao faz delete fisico da entidade                                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_EQUIPAMENTO_D' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_EQUIPAMENTO'] %}
{% set RAW_SOURCE_TABLE = 'SOLIX_BI.RAW.VW_SX_EQUIPAMENTO_D' %}
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
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_EQUIPAMENTO as varchar(20)) as CD_EQUIPAMENTO,
        cast(DESC_EQUIPAMENTO as varchar) as DESC_EQUIPAMENTO,
        cast(CD_MODELO_EQUIPAMENTO as number(38, 0)) as CD_MODELO_EQUIPAMENTO,
        cast(DESC_MODELO_EQUIPAMENTO as varchar) as DESC_MODELO_EQUIPAMENTO,
        cast(CD_TIPO_EQUIPAMENTO as number(38, 0)) as CD_TIPO_EQUIPAMENTO,
        cast(DESC_TIPO_EQUIPAMENTO as varchar) as DESC_TIPO_EQUIPAMENTO,
        cast(DESC_STATUS as varchar) as DESC_STATUS,
        cast(TP_USO_EQUIPAMENTO as number(38, 0)) as TP_USO_EQUIPAMENTO,
        case
            when upper(cast(FG_ATIVO as varchar)) = 'TRUE' then true
            when upper(cast(FG_ATIVO as varchar)) = 'FALSE' then false
            else try_to_boolean(cast(FG_ATIVO as varchar))
        end as FG_ATIVO,
        cast(SOURCE_UPDATED_AT as timestamp_ntz) as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ RAW_SOURCE_TABLE }}
),

latest_source as (
    select *
    from raw_source
    qualify row_number() over (
        partition by ID_CLIENTE, CD_EQUIPAMENTO
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

current_target as (
    {% if is_incremental() %}
    select
        ID_CLIENTE,
        CD_EQUIPAMENTO,
        DESC_EQUIPAMENTO,
        CD_MODELO_EQUIPAMENTO,
        DESC_MODELO_EQUIPAMENTO,
        CD_TIPO_EQUIPAMENTO,
        DESC_TIPO_EQUIPAMENTO,
        DESC_STATUS,
        TP_USO_EQUIPAMENTO,
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT,
        AIRBYTE_EXTRACTED_AT
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)) as ID_CLIENTE,
        cast(null as varchar(20)) as CD_EQUIPAMENTO,
        cast(null as varchar) as DESC_EQUIPAMENTO,
        cast(null as number(38, 0)) as CD_MODELO_EQUIPAMENTO,
        cast(null as varchar) as DESC_MODELO_EQUIPAMENTO,
        cast(null as number(38, 0)) as CD_TIPO_EQUIPAMENTO,
        cast(null as varchar) as DESC_TIPO_EQUIPAMENTO,
        cast(null as varchar) as DESC_STATUS,
        cast(null as number(38, 0)) as TP_USO_EQUIPAMENTO,
        cast(null as boolean) as FG_ATIVO,
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
        s.ID_CLIENTE,
        s.CD_EQUIPAMENTO,
        s.DESC_EQUIPAMENTO,
        s.CD_MODELO_EQUIPAMENTO,
        s.DESC_MODELO_EQUIPAMENTO,
        s.CD_TIPO_EQUIPAMENTO,
        s.DESC_TIPO_EQUIPAMENTO,
        s.DESC_STATUS,
        s.TP_USO_EQUIPAMENTO,
        s.FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,
        case
            when t.ID_CLIENTE is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_EQUIPAMENTO, '') <> coalesce(s.DESC_EQUIPAMENTO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_MODELO_EQUIPAMENTO, -999) <> coalesce(s.CD_MODELO_EQUIPAMENTO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_MODELO_EQUIPAMENTO, '') <> coalesce(s.DESC_MODELO_EQUIPAMENTO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.CD_TIPO_EQUIPAMENTO, -999) <> coalesce(s.CD_TIPO_EQUIPAMENTO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_TIPO_EQUIPAMENTO, '') <> coalesce(s.DESC_TIPO_EQUIPAMENTO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_STATUS, '') <> coalesce(s.DESC_STATUS, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.TP_USO_EQUIPAMENTO, -999) <> coalesce(s.TP_USO_EQUIPAMENTO, -999)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.FG_ATIVO, false) <> coalesce(s.FG_ATIVO, false)
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
        on s.ID_CLIENTE = t.ID_CLIENTE
       and s.CD_EQUIPAMENTO = t.CD_EQUIPAMENTO
    where
        (
            t.ID_CLIENTE is null
            and coalesce(s.FG_ATIVO, true) = true
        )
        or (
            t.ID_CLIENTE is not null
            and (
                coalesce(t.DESC_EQUIPAMENTO, '') <> coalesce(s.DESC_EQUIPAMENTO, '')
                or coalesce(t.CD_MODELO_EQUIPAMENTO, -999) <> coalesce(s.CD_MODELO_EQUIPAMENTO, -999)
                or coalesce(t.DESC_MODELO_EQUIPAMENTO, '') <> coalesce(s.DESC_MODELO_EQUIPAMENTO, '')
                or coalesce(t.CD_TIPO_EQUIPAMENTO, -999) <> coalesce(s.CD_TIPO_EQUIPAMENTO, -999)
                or coalesce(t.DESC_TIPO_EQUIPAMENTO, '') <> coalesce(s.DESC_TIPO_EQUIPAMENTO, '')
                or coalesce(t.DESC_STATUS, '') <> coalesce(s.DESC_STATUS, '')
                or coalesce(t.TP_USO_EQUIPAMENTO, -999) <> coalesce(s.TP_USO_EQUIPAMENTO, -999)
                or coalesce(t.FG_ATIVO, false) <> coalesce(s.FG_ATIVO, false)
                or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                   <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                or coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
                   <> coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
            )
        )
)

select
    ID_CLIENTE,
    CD_EQUIPAMENTO,
    DESC_EQUIPAMENTO,
    CD_MODELO_EQUIPAMENTO,
    DESC_MODELO_EQUIPAMENTO,
    CD_TIPO_EQUIPAMENTO,
    DESC_TIPO_EQUIPAMENTO,
    DESC_STATUS,
    TP_USO_EQUIPAMENTO,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
