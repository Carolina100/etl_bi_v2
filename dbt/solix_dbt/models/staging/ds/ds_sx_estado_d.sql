{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — TEMPLATE MULTI-TENANT (RAW → DS com UNION ALL dinâmico)        ║
║                                                                              ║
║  PARA REUTILIZAR PARA OUTRA TABELA, ALTERE APENAS AS VARIAVEIS ABAIXO       ║
║  MARCADAS COM OS NUMEROS (1), (2), (3), (4) E (5).                          ║
║  O restante do modelo NAO precisa ser tocado.                                ║
╚══════════════════════════════════════════════════════════════════════════════╝

  COMO FUNCIONA:
  - O Airbyte deposita uma tabela por cliente no schema RAW com o padrao:
      RAW.{STREAM_PREFIX}{NOME_TABELA_POSTGRES}
      Ex: RAW.AMAGGI_CDT_ESTADO
  - Este modelo descobre essas tabelas automaticamente via macro
  - Faz UNION ALL de todos os clientes ativos na CTL_AIRBYTE_CONEXOES
  - Aplica merge incremental na tabela DS correspondente
#}

{# ┌─────────────────────────────────────────────────────────────────────────┐
   │  (1) SUFIXO DA ENTIDADE NO RAW                                          │
   │  Parte final do nome da tabela no RAW, sem o prefixo do cliente.        │
   │  Ex: RAW.AMAGGI_CDT_ESTADO → sufixo é 'CDT_ESTADO'                     │
   └───────────────────────────────────────────────────────────────────────── #}
{% set ENTITY_TABLE_SUFFIX = 'CDT_ESTADO' %}

{# ┌─────────────────────────────────────────────────────────────────────────┐
   │  (2) NOME DA TABELA DESTINO NO SCHEMA DS                                │
   │  Nome exato da tabela que será criada/atualizada em DS.                 │
   └───────────────────────────────────────────────────────────────────────── #}
{% set MODEL_ALIAS = 'SX_ESTADO_D' %}

{# ┌─────────────────────────────────────────────────────────────────────────┐
   │  (3) CHAVE NATURAL — colunas que identificam unicamente um registro      │
   │  Usadas no merge incremental (unique_key).                              │
   └───────────────────────────────────────────────────────────────────────── #}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_ESTADO'] %}

{# ┌─────────────────────────────────────────────────────────────────────────┐
   │  (4) NOME DA VARIAVEL dbt PARA MODO DE RECONCILIACAO                    │
   │  Padrao: <alias_em_minusculo>_reconciliation_mode                       │
   │  Uso: dbt run --vars '{"sx_estado_d_reconciliation_mode": "full"}'      │
   └───────────────────────────────────────────────────────────────────────── #}
{% set RECONCILIATION_VAR = 'sx_estado_d_reconciliation_mode' %}

{# ┌─────────────────────────────────────────────────────────────────────────┐
   │  (5) COLUNAS LIDAS DO RAW — mude os casts e nomes conforme a entidade   │
   │                                                                         │
   │  REGRA: a coluna SOURCE_UPDATED_AT deve apontar para o campo de         │
   │  data de atualizacao da tabela no Postgres (geralmente updated_on,      │
   │  updated_at, dt_atualizacao, etc.)                                      │
   │                                                                         │
   │  Para descobrir as colunas disponíveis no RAW, execute:                 │
   │  SELECT * FROM SOLIX_BI.RAW.AMAGGI_CDT_ESTADO LIMIT 1;                 │
   └───────────────────────────────────────────────────────────────────────── #}
{# (5) As colunas sao usadas no bloco staged_airbyte mais abaixo #}

{# ══════════════════════════════════════════════════════════════════════════ #}
{# DAQUI PARA BAIXO NAO E NECESSARIO ALTERAR — LOGICA GENERICA DO TEMPLATE  #}
{# ══════════════════════════════════════════════════════════════════════════ #}

{% set TARGET_SCHEMA            = 'DS' %}
{% set BATCH_ID                 = invocation_id %}
{% set RECONCILIATION_MODE      = var(RECONCILIATION_VAR, 'incremental') | lower %}
{% set IS_FULL_RECONCILIATION   = RECONCILIATION_MODE == 'full' %}

{{ config(
    materialized='incremental',
    schema=TARGET_SCHEMA,
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['ds', 'incremental', MODEL_ALIAS | lower]
) }}

{% if execute %}
    {% set client_tables_sql %}
        {{ discover_raw_client_tables(ENTITY_TABLE_SUFFIX) }}
    {% endset %}
    {% set client_tables = run_query(client_tables_sql) %}
    {% set table_rows = client_tables.rows %}
{% else %}
    {% set table_rows = [] %}
{% endif %}

with staged_airbyte as (

{% if table_rows | length == 0 %}

    select
        cast(null as number(38, 0))  as ID_CLIENTE,
        cast(null as varchar)         as CD_ESTADO,
        cast(null as varchar)         as DESC_ESTADO,
        cast(null as timestamp_ntz)   as SOURCE_UPDATED_AT,
        cast(null as timestamp_ntz)   as _airbyte_extracted_at
    where 1 = 0

{% else %}

    {% for row in table_rows %}
    {% set raw_table_name = row[0] %}
    {% set id_cliente      = row[1] %}

    select
        {# ── (5) AJUSTE AS COLUNAS ABAIXO CONFORME A ENTIDADE ─────────────── #}
        cast({{ id_cliente }}        as number(38, 0))  as ID_CLIENTE,
        cast(CD_ESTADO               as varchar)         as CD_ESTADO,
        cast(DESC_ESTADO             as varchar)         as DESC_ESTADO,
        cast(UPDATED_ON              as timestamp_ntz)   as SOURCE_UPDATED_AT,
        {# ── FIM DO BLOCO DE COLUNAS ──────────────────────────────────────── #}
        cast(_airbyte_extracted_at   as timestamp_ntz)   as _airbyte_extracted_at
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
