{#
  MODELO OPERACIONAL DS — SX_ESTADO_D (Multi-Tenant)

  OBJETIVO:
  - Ler todas as tabelas de estado de clientes no schema RAW (ex: RAW.AMAGGI_CDT_ESTADO)
  - Derivar ID_CLIENTE a partir de DS.CTL_AIRBYTE_CONEXOES via STREAM_PREFIX
  - Deduplicar por chave natural (ID_CLIENTE, CD_ESTADO)
  - Aplicar merge semantico na tabela final DS.SX_ESTADO_D
  - Preservar BI_CREATED_AT e atualizar BI_UPDATED_AT apenas quando houver mudanca real
  - Inativar registros ausentes somente na reconciliacao full

  PREMISSA OPERACIONAL:
  - A inativacao de ausentes so acontece quando sx_estado_d_reconciliation_mode='full'
  - Numa rodada full, o conjunto de dados deve representar a foto completa da entidade
  - Se todas as fontes RAW vierem vazias, o modelo nao inativa os registros atuais

  CONVENCAO RAW:
  - Tabelas no formato: {STREAM_PREFIX}{NOME_TABELA_POSTGRES} em maiusculas
  - Ex: AMAGGI_ + CDT_ESTADO = AMAGGI_CDT_ESTADO
  - STREAM_PREFIX e definido em DS.CTL_AIRBYTE_CONEXOES

  AJUSTE SE NECESSARIO:
  - A coluna de timestamp da origem e mapeada como UPDATED_ON (nome no Postgres)
  - Se o nome for diferente, ajuste o cast de SOURCE_UPDATED_AT abaixo
  - Para verificar: SELECT * FROM SOLIX_BI.RAW.AMAGGI_CDT_ESTADO LIMIT 1;
#}

{% set MODEL_ALIAS          = 'SX_ESTADO_D' %}
{% set TARGET_SCHEMA        = 'DS' %}
{% set NATURAL_KEY_COLUMNS  = ['ID_CLIENTE', 'CD_ESTADO'] %}
{% set BATCH_ID             = invocation_id %}
{% set RECONCILIATION_MODE  = var('sx_estado_d_reconciliation_mode', 'incremental') | lower %}
{% set IS_FULL_RECONCILIATION = RECONCILIATION_MODE == 'full' %}

{{ config(
    materialized='incremental',
    schema=TARGET_SCHEMA,
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['ds', 'incremental', 'sx_estado_d']
) }}

{# ── Descoberta dinamica de tabelas de clientes no RAW ──────────────────────
   A macro consulta INFORMATION_SCHEMA.TABLES filtrando por sufixo da entidade
   e faz join com CTL_AIRBYTE_CONEXOES via STREAM_PREFIX para obter ID_CLIENTE.
   Resultado: lista de (TABLE_NAME, ID_CLIENTE) para clientes ativos.
────────────────────────────────────────────────────────────────────────────── #}

{% if execute %}
    {% set client_tables_sql %}
        {{ discover_raw_client_tables('CDT_ESTADO') }}
    {% endset %}
    {% set client_tables = run_query(client_tables_sql) %}
    {% set table_rows = client_tables.rows %}
{% else %}
    {% set table_rows = [] %}
{% endif %}

with staged_airbyte as (

{% if table_rows | length == 0 %}

    {# Fallback: nenhuma tabela de cliente encontrada no RAW — retorna conjunto vazio #}
    select
        cast(null as number(38, 0))  as ID_CLIENTE,
        cast(null as varchar)         as CD_ESTADO,
        cast(null as varchar)         as DESC_ESTADO,
        cast(null as timestamp_ntz)   as SOURCE_UPDATED_AT,
        cast(null as timestamp_ntz)   as _airbyte_extracted_at
    where 1 = 0

{% else %}

    {# UNION ALL dinamico: uma branch por cliente ativo encontrado no RAW #}
    {% for row in table_rows %}
    {% set raw_table_name = row[0] %}
    {% set id_cliente      = row[1] %}

    select
        cast({{ id_cliente }}               as number(38, 0))  as ID_CLIENTE,
        cast(CD_ESTADO                     as varchar)          as CD_ESTADO,
        cast(DESC_ESTADO                   as varchar)          as DESC_ESTADO,
        cast(UPDATED_ON                    as timestamp_ntz)    as SOURCE_UPDATED_AT,
        cast(_airbyte_extracted_at         as timestamp_ntz)    as _airbyte_extracted_at
    from SOLIX_BI.RAW.{{ raw_table_name }}

    {% if not loop.last %} union all {% endif %}
    {% endfor %}

{% endif %}

),

latest_stage as (
    select
        ID_CLIENTE,
        CD_ESTADO,
        DESC_ESTADO,
        SOURCE_UPDATED_AT,
        _airbyte_extracted_at
    from staged_airbyte
    qualify row_number() over (
        partition by ID_CLIENTE, CD_ESTADO
        order by SOURCE_UPDATED_AT desc nulls last, _airbyte_extracted_at desc nulls last
    ) = 1
),

current_target as (
    {% if is_incremental() %}
    select
        ID_CLIENTE,
        CD_ESTADO,
        DESC_ESTADO,
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0))  as ID_CLIENTE,
        cast(null as varchar)         as CD_ESTADO,
        cast(null as varchar)         as DESC_ESTADO,
        cast(null as number(1, 0))    as FG_ATIVO,
        cast(null as varchar)         as ETL_BATCH_ID,
        cast(null as timestamp_ntz)   as BI_CREATED_AT,
        cast(null as timestamp_ntz)   as BI_UPDATED_AT,
        cast(null as timestamp_ntz)   as SOURCE_UPDATED_AT
    where 1 = 0
    {% endif %}
),

changed_or_new_rows as (
    select
        s.ID_CLIENTE,
        s.CD_ESTADO,
        s.DESC_ESTADO,
        cast(1 as number(1, 0))           as FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar)  as ETL_BATCH_ID,
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        )                                  as BI_CREATED_AT,
        case
            when t.ID_CLIENTE is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_ESTADO, '') <> coalesce(s.DESC_ESTADO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.FG_ATIVO, 1) <> 1
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            else t.BI_UPDATED_AT
        end                                as BI_UPDATED_AT,
        s.SOURCE_UPDATED_AT
    from latest_stage s
    left join current_target t
        on  s.ID_CLIENTE = t.ID_CLIENTE
        and s.CD_ESTADO  = t.CD_ESTADO
    where t.ID_CLIENTE is null
       or coalesce(t.DESC_ESTADO, '') <> coalesce(s.DESC_ESTADO, '')
       or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
       <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
       or coalesce(t.FG_ATIVO, 1) <> 1
),

missing_rows as (
    {% if is_incremental() and IS_FULL_RECONCILIATION %}
    select
        t.ID_CLIENTE,
        t.CD_ESTADO,
        t.DESC_ESTADO,
        cast(0 as number(1, 0))            as FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar)   as ETL_BATCH_ID,
        t.BI_CREATED_AT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as BI_UPDATED_AT,
        t.SOURCE_UPDATED_AT
    from current_target t
    where exists (select 1 from latest_stage)
      and coalesce(t.FG_ATIVO, 1) <> 0
      and not exists (
          select 1
          from latest_stage s
          where s.ID_CLIENTE = t.ID_CLIENTE
            and s.CD_ESTADO  = t.CD_ESTADO
      )
    {% else %}
    select
        cast(null as number(38, 0))  as ID_CLIENTE,
        cast(null as varchar)         as CD_ESTADO,
        cast(null as varchar)         as DESC_ESTADO,
        cast(null as number(1, 0))    as FG_ATIVO,
        cast(null as varchar)         as ETL_BATCH_ID,
        cast(null as timestamp_ntz)   as BI_CREATED_AT,
        cast(null as timestamp_ntz)   as BI_UPDATED_AT,
        cast(null as timestamp_ntz)   as SOURCE_UPDATED_AT
    where 1 = 0
    {% endif %}
),

final as (
    select * from changed_or_new_rows
    union all
    select * from missing_rows
)

select
    ID_CLIENTE,
    CD_ESTADO,
    DESC_ESTADO,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT
from final
