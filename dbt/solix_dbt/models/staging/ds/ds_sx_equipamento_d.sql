{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW → DS (incremental por merge)                               ║
║                                                                              ║
║  ENTIDADE COMPOSTA / CURRENT STATE                                           ║
║                                                                              ║
║  QUANDO REUTILIZAR ESTE MODELO PARA OUTRA ENTIDADE COMPOSTA, AJUSTE:        ║
║  1. BLOCO DE CONFIGURACAO                                                    ║
║  2. CTE base_entity_stage / latest                                           ║
║  3. CTEs auxiliares *_stage / *_latest                                       ║
║  4. CTE consolidated_source                                                  ║
║  5. COMPARACAO em changed_or_new                                             ║
║                                                                              ║
║  O QUE ESTE MODELO FAZ                                                       ║
║  - usa uma tabela dirigente como verdade da entidade                         ║
║  - enriquece com tabelas complementares                                      ║
║  - calcula SOURCE_UPDATED_AT pela maior data entre as fontes                 ║
║  - mantem 1 linha por chave natural no DS                                    ║
║  - trata soft delete atualizando a mesma linha                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

-- ============================================================================
-- BLOCO DE CONFIGURACAO — AJUSTE SO AQUI PARA REUSO
-- ============================================================================

-- Nome final da tabela no schema DS
{% set MODEL_ALIAS = 'SX_EQUIPAMENTO_D' %}

-- Chave natural da entidade no DS
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_EQUIPAMENTO'] %}

-- Tabela dirigente: controla existencia do registro e FG_ATIVO oficial
{% set BASE_RAW_TABLE = 'SOLIX_BI.RAW.CDT_EQUIPAMENTO' %}

-- Tabelas auxiliares de enriquecimento
{% set MODELO_RAW_TABLE = 'SOLIX_BI.RAW.CDT_MODELO_EQUIPAMENTO' %}
{% set TIPO_RAW_TABLE = 'SOLIX_BI.RAW.CDT_TIPO_EQUIPAMENTO' %}
{% set STATUS_RAW_TABLE = 'SOLIX_BI.RAW.CDT_EQUIPAMENTO_HISTORICO_MOV' %}

-- Batch tecnico do dbt
{% set BATCH_ID = invocation_id %}
{% set FILTER_ID_CLIENTE = var('id_cliente', none) %}

-- Filtro temporario de tipos aceitos.
-- Se futuramente a regra for removida, elimine apenas o WHERE da base dirigente.
{% set EQUIPAMENTO_TYPE_FILTER = "('101', '103', '104', '105', '106', '107', '108', '120', '121', '122')" %}

{{ config(
    materialized='incremental',
    schema='DS',
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['ds', 'incremental', MODEL_ALIAS | lower]
) }}

-- ============================================================================
-- PASSO 1 — LEITURA E DEDUPLICACAO DA TABELA DIRIGENTE
-- A tabela dirigente define:
-- - a chave natural do registro
-- - o FG_ATIVO oficial
-- - a existencia do equipamento no DS
-- ============================================================================

with base_entity_stage as (
    select
        cast(CD_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_EQUIPAMENTO as varchar(20)) as CD_EQUIPAMENTO,
        cast(DESC_EQUIPAMENTO as varchar) as DESC_EQUIPAMENTO,
        cast(CD_MODELO_EQUIPAMENTO as number(38, 0)) as CD_MODELO_EQUIPAMENTO,
        cast(CD_TP_EQUIPAMENTO as number(38, 0)) as CD_TIPO_EQUIPAMENTO,
        cast(TP_USO_EQUIPAMENTO as number(38, 0)) as TP_USO_EQUIPAMENTO,
        case
            when upper(cast(FG_ATIVO as varchar)) = 'TRUE' then 1
            when upper(cast(FG_ATIVO as varchar)) = 'FALSE' then 0
            else try_to_number(cast(FG_ATIVO as varchar))
        end as FG_ATIVO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ BASE_RAW_TABLE }}
    where CD_TP_EQUIPAMENTO in {{ EQUIPAMENTO_TYPE_FILTER }}
    {% if FILTER_ID_CLIENTE is not none %}
      and CD_CLIENTE = {{ FILTER_ID_CLIENTE }}
    {% endif %}
), 

base_entity_latest as (
    select *
    from base_entity_stage
    qualify row_number() over (
        partition by ID_CLIENTE, CD_EQUIPAMENTO
        order by DT_UPDATED desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

-- ============================================================================
-- PASSO 2 — LEITURA E DEDUPLICACAO DAS TABELAS AUXILIARES
-- Cada tabela auxiliar precisa entregar apenas a ultima versao por chave.
-- ============================================================================

modelo_stage as (
    select
        cast(CD_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_MODELO_EQUIPAMENTO as number(38, 0)) as CD_MODELO_EQUIPAMENTO,
        cast(DESC_MODELO_EQUIPAMENTO as varchar) as DESC_MODELO_EQUIPAMENTO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ MODELO_RAW_TABLE }}
    {% if FILTER_ID_CLIENTE is not none %}
    where CD_CLIENTE = {{ FILTER_ID_CLIENTE }}
    {% endif %}
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
        cast(CD_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_TP_EQUIPAMENTO as number(38, 0)) as CD_TIPO_EQUIPAMENTO,
        cast(DESC_TP_EQUIPAMENTO as varchar) as DESC_TIPO_EQUIPAMENTO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ TIPO_RAW_TABLE }}
    {% if FILTER_ID_CLIENTE is not none %}
    where CD_CLIENTE = {{ FILTER_ID_CLIENTE }}
    {% endif %}
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

status_history_stage as (
    select
        cast(CD_CLIENTE as number(38, 0)) as ID_CLIENTE,
        cast(CD_EQUIPAMENTO as varchar(20)) as CD_EQUIPAMENTO,
        case
            when upper(cast(FG_ATIVO as varchar)) = 'TRUE' then true
            when upper(cast(FG_ATIVO as varchar)) = 'FALSE' then false
            else null
        end as FG_ATIVO_STATUS,
        cast(DT_HR_UTC_MOVIMENTO as timestamp_ntz) as DT_HR_UTC_MOVIMENTO,
        cast(DT_UPDATED as timestamp_ntz) as DT_UPDATED,
        cast(_AIRBYTE_EXTRACTED_AT as timestamp_ntz) as AIRBYTE_EXTRACTED_AT
    from {{ STATUS_RAW_TABLE }}
    {% if FILTER_ID_CLIENTE is not none %}
    where CD_CLIENTE = {{ FILTER_ID_CLIENTE }}
    {% endif %}
),

status_last_event as (
    select
        ID_CLIENTE,
        CD_EQUIPAMENTO,
        max(DT_HR_UTC_MOVIMENTO) as DT_HR_UTC_MOVIMENTO
    from status_history_stage
    group by
        ID_CLIENTE,
        CD_EQUIPAMENTO
),

status_latest as (
    select
        s.ID_CLIENTE,
        s.CD_EQUIPAMENTO,
        s.FG_ATIVO_STATUS,
        s.DT_HR_UTC_MOVIMENTO,
        s.DT_UPDATED,
        s.AIRBYTE_EXTRACTED_AT
    from status_history_stage s
    inner join status_last_event e
        on s.ID_CLIENTE = e.ID_CLIENTE
       and s.CD_EQUIPAMENTO = e.CD_EQUIPAMENTO
       and s.DT_HR_UTC_MOVIMENTO = e.DT_HR_UTC_MOVIMENTO
    qualify row_number() over (
        partition by s.ID_CLIENTE, s.CD_EQUIPAMENTO
        order by s.DT_UPDATED desc nulls last,
                 s.AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),

-- ============================================================================
-- PASSO 3 — CONSOLIDACAO DA ENTIDADE
-- Aqui concentramos a regra de negocio da entidade final no DS.
-- REGRAS:
-- - FG_ATIVO sempre vem da tabela dirigente
-- - tabelas auxiliares apenas enriquecem atributos
-- - SOURCE_UPDATED_AT e calculado pela maior data entre as fontes
-- ============================================================================

consolidated_source as (
    select
        e.ID_CLIENTE,
        e.CD_EQUIPAMENTO,
        e.DESC_EQUIPAMENTO,
        coalesce(m.CD_MODELO_EQUIPAMENTO, -1) as CD_MODELO_EQUIPAMENTO,
        coalesce(m.DESC_MODELO_EQUIPAMENTO, 'UNDEFINED') as DESC_MODELO_EQUIPAMENTO,
        coalesce(t.CD_TIPO_EQUIPAMENTO, -1) as CD_TIPO_EQUIPAMENTO,
        coalesce(t.DESC_TIPO_EQUIPAMENTO, 'UNDEFINED') as DESC_TIPO_EQUIPAMENTO,
        coalesce(case when s.FG_ATIVO_STATUS = false then 'I' else 'A' end, 'UNDEFINED') as DESC_STATUS,
        coalesce(e.TP_USO_EQUIPAMENTO, -1) as TP_USO_EQUIPAMENTO,
        e.FG_ATIVO,
        greatest(
            coalesce(e.DT_UPDATED, '1900-01-01'::timestamp_ntz),
            coalesce(m.DT_UPDATED, '1900-01-01'::timestamp_ntz),
            coalesce(t.DT_UPDATED, '1900-01-01'::timestamp_ntz),
            coalesce(s.DT_HR_UTC_MOVIMENTO, '1900-01-01'::timestamp_ntz),
            coalesce(s.DT_UPDATED, '1900-01-01'::timestamp_ntz)
        ) as SOURCE_UPDATED_AT,
        greatest(
            coalesce(e.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz),
            coalesce(m.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz),
            coalesce(t.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz),
            coalesce(s.AIRBYTE_EXTRACTED_AT, '1900-01-01'::timestamp_ntz)
        ) as AIRBYTE_EXTRACTED_AT
    from base_entity_latest e
    left join modelo_latest m
        on e.ID_CLIENTE = m.ID_CLIENTE
       and e.CD_MODELO_EQUIPAMENTO = m.CD_MODELO_EQUIPAMENTO
    left join tipo_latest t
        on e.ID_CLIENTE = t.ID_CLIENTE
       and e.CD_TIPO_EQUIPAMENTO = t.CD_TIPO_EQUIPAMENTO
    left join status_latest s
        on e.ID_CLIENTE = s.ID_CLIENTE
       and e.CD_EQUIPAMENTO = s.CD_EQUIPAMENTO
),

-- ============================================================================
-- PASSO 4 — ESTADO ATUAL DA TABELA DS
-- Usado para identificar apenas inserts ou updates necessarios.
-- ============================================================================

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
    {% if FILTER_ID_CLIENTE is not none %}
    where ID_CLIENTE = {{ FILTER_ID_CLIENTE }}
    {% endif %}
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

-- ============================================================================
-- PASSO 5 — DETECCAO DE NOVOS OU ALTERADOS
-- REGRAS:
-- - ativo e nao existe -> insert
-- - ativo e existe -> update se houve mudanca
-- - inativo e existe -> update da mesma linha
-- - inativo e nao existe -> ignorar
-- ============================================================================

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
