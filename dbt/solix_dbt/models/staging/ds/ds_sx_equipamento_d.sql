{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW → DS (incremental por merge)                               ║
║                                                                              ║
║  ENTIDADE COMPOSTA                                                           ║
║  - tabela dirigente: CDT_EQUIPAMENTO                                         ║
║  - tabelas complementares: CDT_MODELO_EQUIPAMENTO, CDT_TIPO_EQUIPAMENTO     ║
║  - status atual: CDT_EQUIPAMENTO_HISTORICO_MOV                               ║
║                                                                              ║
║  REGRA DE NEGOCIO                                                            ║
║  - 1 linha por chave natural no DS                                           ║
║  - FG_ATIVO oficial vem da CDT_EQUIPAMENTO                                   ║
║  - registro inativo atualiza a mesma linha existente                         ║
║  - registro inativo novo nao gera insert                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

{% set MODEL_ALIAS = 'SX_EQUIPAMENTO_D' %}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_EQUIPAMENTO'] %}
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

with equipamento_stage as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_EQUIPAMENTO as varchar(20)) as CD_EQUIPAMENTO,
        cast(DESC_EQUIPAMENTO as varchar) as DESC_EQUIPAMENTO,
        cast(CD_MODELO_EQUIPAMENTO as number(38, 0)) as CD_MODELO_EQUIPAMENTO,
        cast(CD_TP_EQUIPAMENTO as number(38, 0)) as CD_TIPO_EQUIPAMENTO,
        cast(TP_USO_EQUIPAMENTO as number(38, 0)) as TP_USO_EQUIPAMENTO,
        cast(FG_ATIVO as number(1, 0)) as FG_ATIVO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from SOLIX_BI.RAW.CDT_EQUIPAMENTO
    where CD_TP_EQUIPAMENTO in ('101', '103', '104', '105', '106', '107', '108', '120', '121', '122')
),

equipamento_latest as (
    select *
    from equipamento_stage
    qualify row_number() over (
        partition by ID_CLIENTE, CD_EQUIPAMENTO
        order by DT_UPDATED desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

modelo_stage as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_MODELO_EQUIPAMENTO as number(38, 0)) as CD_MODELO_EQUIPAMENTO,
        cast(DESC_MODELO_EQUIPAMENTO as varchar) as DESC_MODELO_EQUIPAMENTO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from SOLIX_BI.RAW.CDT_MODELO_EQUIPAMENTO
),

modelo_latest as (
    select *
    from modelo_stage
    qualify row_number() over (
        partition by ID_CLIENTE, CD_MODELO_EQUIPAMENTO
        order by DT_UPDATED desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

tipo_stage as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_TP_EQUIPAMENTO as number(38, 0)) as CD_TIPO_EQUIPAMENTO,
        cast(DESC_TP_EQUIPAMENTO as varchar) as DESC_TIPO_EQUIPAMENTO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from SOLIX_BI.RAW.CDT_TIPO_EQUIPAMENTO
),

tipo_latest as (
    select *
    from tipo_stage
    qualify row_number() over (
        partition by ID_CLIENTE, CD_TIPO_EQUIPAMENTO
        order by DT_UPDATED desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

historico_status_stage as (
    select
        cast(ID_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_EQUIPAMENTO as varchar(20)) as CD_EQUIPAMENTO,
        cast(FG_STATUS as varchar) as FG_STATUS,
        cast(DT_HR_UTC_MOVIMENTO as timestamp_ntz) as DT_HR_UTC_MOVIMENTO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from SOLIX_BI.RAW.CDT_EQUIPAMENTO_HISTORICO_MOV
),

historico_status_latest as (
    select *
    from historico_status_stage
    qualify row_number() over (
        partition by ID_CLIENTE, CD_EQUIPAMENTO
        order by DT_HR_UTC_MOVIMENTO desc nulls last,
                 DT_UPDATED desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

consolidated_source as (
    select
        e.ID_CLIENTE,
        e.CD_EQUIPAMENTO,
        e.DESC_EQUIPAMENTO,
        coalesce(m.CD_MODELO_EQUIPAMENTO, -1) as CD_MODELO_EQUIPAMENTO,
        coalesce(m.DESC_MODELO_EQUIPAMENTO, 'NOT DEFINED') as DESC_MODELO_EQUIPAMENTO,
        coalesce(t.CD_TIPO_EQUIPAMENTO, -1) as CD_TIPO_EQUIPAMENTO,
        coalesce(t.DESC_TIPO_EQUIPAMENTO, 'NOT DEFINED') as DESC_TIPO_EQUIPAMENTO,
        coalesce(case when hs.FG_STATUS = 'I' then 'INACTIVE' else 'ACTIVE' end, 'UNDEFINED') as DESC_STATUS,
        coalesce(e.TP_USO_EQUIPAMENTO, -1) as TP_USO_EQUIPAMENTO,
        e.FG_ATIVO,
        greatest(
            coalesce(e.DT_UPDATED, '1900-01-01'::timestamp_ntz),
            coalesce(m.DT_UPDATED, '1900-01-01'::timestamp_ntz),
            coalesce(t.DT_UPDATED, '1900-01-01'::timestamp_ntz),
            coalesce(hs.DT_HR_UTC_MOVIMENTO, '1900-01-01'::timestamp_ntz),
            coalesce(hs.DT_UPDATED, '1900-01-01'::timestamp_ntz)
        ) as SOURCE_UPDATED_AT,
        greatest(
            coalesce(e.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz),
            coalesce(m.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz),
            coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz),
            coalesce(hs.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
        ) as AIRBYTE_EXTRACTED_AT
    from equipamento_latest e
    left join modelo_latest m
        on e.ID_CLIENTE = m.ID_CLIENTE
       and e.CD_MODELO_EQUIPAMENTO = m.CD_MODELO_EQUIPAMENTO
    left join tipo_latest t
        on e.ID_CLIENTE = t.ID_CLIENTE
       and e.CD_TIPO_EQUIPAMENTO = t.CD_TIPO_EQUIPAMENTO
    left join historico_status_latest hs
        on e.ID_CLIENTE = hs.ID_CLIENTE
       and e.CD_EQUIPAMENTO = hs.CD_EQUIPAMENTO
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
        cast(null as number(1, 0)) as FG_ATIVO,
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
            when coalesce(t.FG_ATIVO, 1) <> coalesce(s.FG_ATIVO, 1)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            else t.BI_UPDATED_AT
        end as BI_UPDATED_AT,
        s.SOURCE_UPDATED_AT,
        s.AIRBYTE_EXTRACTED_AT
    from consolidated_source s
    left join current_target t
        on s.ID_CLIENTE = t.ID_CLIENTE
       and s.CD_EQUIPAMENTO = t.CD_EQUIPAMENTO
    where
        (
            t.ID_CLIENTE is null
            and coalesce(s.FG_ATIVO, 1) = 1
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
                or coalesce(t.FG_ATIVO, 1) <> coalesce(s.FG_ATIVO, 1)
                or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                   <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
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
