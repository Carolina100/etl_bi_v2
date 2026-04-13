{#
╔══════════════════════════════════════════════════════════════════════════════╗
║  MODELO DS — RAW → DS (incremental por merge)                               ║
║                                                                              ║
║  PARA REUTILIZAR PARA OUTRA TABELA, ALTERE APENAS AS VARIAVEIS ABAIXO       ║
║  MARCADAS COM OS NUMEROS (1) A (6).                                          ║
║  O restante do modelo NAO precisa ser tocado.                                ║
╚══════════════════════════════════════════════════════════════════════════════╝
#}

-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCO DE CONFIGURAÇÃO — ALTERE SÓ AQUI AO REUTILIZAR PARA OUTRA TABELA
-- ═══════════════════════════════════════════════════════════════════════════

{# (1) Nome da tabela no RAW. Exemplo: RAW.CDT_ESTADO → sufixo = 'CDT_ESTADO' #}
{% set ENTITY_RAW_TABLE = 'CDT_FAZENDA' %}

{# (2) Nome da tabela destino que será criada/atualizada no schema DS #}
{% set MODEL_ALIAS = 'SX_FAZENDA_D' %}

{# (3) Colunas que identificam unicamente um registro (chave para o merge) #}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_FAZENDA', 'CD_ZONA', 'CD_TALHAO'] %}

{# (4) Coluna de data de atualização da origem — usada como cursor pelo Airbyte #}
{% set SOURCE_UPDATED_AT_COLUMN = 'dt_updated' %}

{# (5) A tabela de origem possui flag de ativo/inativo?
       true  → lê a coluna da origem e propaga para o DS (dimensões por cliente)
       false → dimensão global/compartilhada, sem flag; FG_ATIVO sempre = 1 #}
{% set HAS_FG_ATIVO = true %}

{# (6) [Só preencher se HAS_FG_ATIVO = true] Nome da coluna de flag na origem #}
{% set FG_ATIVO_COLUMN = 'FG_ATIVO' %}

-- ═══════════════════════════════════════════════════════════════════════════
-- CONFIGURAÇÃO DO MODELO dbt (não alterar)
-- ═══════════════════════════════════════════════════════════════════════════

{% set TARGET_SCHEMA = 'DS' %}
{% set BATCH_ID      = invocation_id %}

{{ config(
    materialized='incremental',
    schema=TARGET_SCHEMA,
    alias=MODEL_ALIAS,
    incremental_strategy='merge',
    unique_key=NATURAL_KEY_COLUMNS,
    on_schema_change='sync_all_columns',
    tags=['ds', 'incremental', MODEL_ALIAS | lower]
) }}


-- ═══════════════════════════════════════════════════════════════════════════
-- PASSO 1 — LEITURA DO RAW
-- Lê diretamente a tabela RAW da entidade. Sem UNION ALL, sem CTL de conexões.
-- O ID_CLIENTE já vem como coluna do dado — não precisa ser injetado pelo dbt.
-- O FG_ATIVO da origem é lido aqui e propagado para o DS (soft delete real).
--
-- ⚠️  Ajuste as colunas do SELECT abaixo para refletir a entidade:
--     - Mantenha ID_CLIENTE, FG_ATIVO, SOURCE_UPDATED_AT e AIRBYTE_EXTRACTED_AT
--     - Altere as colunas do meio para as colunas de negócio da tabela RAW
-- ═══════════════════════════════════════════════════════════════════════════

with staged_airbyte as (
    select
        -- ▼ Colunas de negócio da entidade
        cast(ID_CLIENTE                as number(38, 0)  ) as ID_CLIENTE,
        cast(CD_FAZENDA                as varchar        ) as CD_FAZENDA,
        cast(DESC_FAZENDA              as varchar        ) as DESC_FAZENDA,
        cast(CD_TALHAO                 as varchar        ) as CD_TALHAO,
        cast(DESC_TALHAO               as varchar        ) as DESC_TALHAO,
        cast(CD_ZONA                   as varchar        ) as CD_ZONA,
        cast(AREA_TOTAL                as number(38, 8)  ) as AREA_TOTAL,
        cast(DESC_PRODUTOR             as varchar        ) as DESC_PRODUTOR,
        -- ▲ Fim das colunas de negócio
        -- FG_ATIVO: lido da origem se HAS_FG_ATIVO=true; caso contrário, sempre ativo
        {% if HAS_FG_ATIVO %}
        cast({{ FG_ATIVO_COLUMN }}          as number(1, 0))   as FG_ATIVO,
        {% else %}
        cast(1                              as number(1, 0))   as FG_ATIVO,  -- global: sem flag
        {% endif %}
        cast({{ SOURCE_UPDATED_AT_COLUMN }} as timestamp_ntz)  as SOURCE_UPDATED_AT,
        cast(_AIRBYTE_EXTRACTED_AT          as timestamp_ntz)  as AIRBYTE_EXTRACTED_AT
    from SOLIX_BI.RAW.{{ ENTITY_RAW_TABLE }}
),


-- ═══════════════════════════════════════════════════════════════════════════
-- PASSO 2 — DEDUPLICAÇÃO
-- Garante uma única linha por chave natural, pegando a versão mais recente.
-- Cobre o caso de re-entrega pelo Airbyte (ex: recargas, sobreposição de cursor).
-- ═══════════════════════════════════════════════════════════════════════════

latest_stage as (
    select *
    from staged_airbyte
    qualify row_number() over (
        partition by ID_CLIENTE, CD_FAZENDA, CD_ZONA, CD_TALHAO
        order by SOURCE_UPDATED_AT desc nulls last,
                 AIRBYTE_EXTRACTED_AT desc nulls last
    ) = 1
),


-- ═══════════════════════════════════════════════════════════════════════════
-- PASSO 3 — ESTADO ATUAL DA TABELA DS
-- Lê o que já existe no DS para comparar com o que veio do RAW.
-- Na primeira execução (full-refresh) a tabela ainda não existe → vazio.
-- ═══════════════════════════════════════════════════════════════════════════

current_target as (
    {% if is_incremental() %}
    select
        ID_CLIENTE,
        CD_FAZENDA,
        DESC_FAZENDA,
        CD_TALHAO,
        DESC_TALHAO,
        CD_ZONA,
        AREA_TOTAL,
        DESC_PRODUTOR,,
        FG_ATIVO,
        ETL_BATCH_ID,
        BI_CREATED_AT,
        BI_UPDATED_AT,
        SOURCE_UPDATED_AT,
        AIRBYTE_EXTRACTED_AT
    from {{ this }}
    {% else %}
    select
        cast(null as number(38, 0)  ) as ID_CLIENTE,
        cast(null as varchar        ) as CD_FAZENDA,
        cast(null as varchar        ) as DESC_FAZENDA,
        cast(null as varchar        ) as CD_TALHAO,
        cast(null as varchar        ) as DESC_TALHAO,
        cast(null as varchar        ) as CD_ZONA,
        cast(null as number(38, 8)  ) as AREA_TOTAL,
        cast(null as varchar        ) as DESC_PRODUTOR,,
        cast(null as number(1, 0))   as FG_ATIVO,
        cast(null as varchar)        as ETL_BATCH_ID,
        cast(null as timestamp_ntz)  as BI_CREATED_AT,
        cast(null as timestamp_ntz)  as BI_UPDATED_AT,
        cast(null as timestamp_ntz)  as SOURCE_UPDATED_AT,
        cast(null as timestamp_ntz)  as AIRBYTE_EXTRACTED_AT
    where 1 = 0
    {% endif %}
),


-- ═══════════════════════════════════════════════════════════════════════════
-- PASSO 4 — LINHAS NOVAS OU ALTERADAS
-- Compara o RAW (latest_stage) com o DS atual (current_target) e seleciona:
--   • Registros novos (não existem no DS ainda)
--   • Registros alterados (qualquer coluna de negócio mudou)
--   • Registros cujo status ativo/inativo mudou na origem (soft delete)
--
-- REGRAS IMPORTANTES:
--   • FG_ATIVO vem sempre da origem, nunca hardcoded
--   • BI_CREATED_AT é preservado do INSERT original e nunca sobrescrito
--   • BI_UPDATED_AT só muda quando há mudança real de negócio
--   • AIRBYTE_EXTRACTED_AT não entra no critério de mudança — é metadado
--     de extração e muda a cada run mesmo sem alteração de dado
-- ═══════════════════════════════════════════════════════════════════════════

changed_or_new as (
    select
        s.ID_CLIENTE,
        s.CD_FAZENDA,
        s.DESC_FAZENDA,
        s.CD_TALHAO,
        s.DESC_TALHAO,
        s.CD_ZONA,
        s.AREA_TOTAL,
        s.DESC_PRODUTOR,,
        s.FG_ATIVO,
        cast('{{ BATCH_ID }}' as varchar) as ETL_BATCH_ID,

        -- BI_CREATED_AT: preenche só na criação, preserva nas atualizações
        coalesce(
            t.BI_CREATED_AT,
            convert_timezone('UTC', current_timestamp())::timestamp_ntz
        ) as BI_CREATED_AT,

        -- BI_UPDATED_AT: atualiza SOMENTE quando há mudança real de negócio
        case
            when t.ID_CLIENTE is null
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz  -- novo registro
                        when coalesce(t.DESC_FAZENDA, '') <> coalesce(s.DESC_FAZENDA, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_TALHAO, '') <> coalesce(s.DESC_TALHAO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.AREA_TOTAL, -999.99) <> coalesce(s.AREA_TOTAL, -999.99)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_PRODUTOR, '') <> coalesce(s.DESC_PRODUTOR, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
              <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz  -- origem atualizou
            {% if HAS_FG_ATIVO %}
            when coalesce(t.FG_ATIVO, 1) <> coalesce(s.FG_ATIVO, 1)
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz  -- status mudou
            {% endif %}
            else t.BI_UPDATED_AT                                                  -- sem mudanca real
        end as BI_UPDATED_AT,

        s.SOURCE_UPDATED_AT,
        s.AIRBYTE_EXTRACTED_AT

    from latest_stage s
    left join current_target t
        on s.ID_CLIENTE = t.ID_CLIENTE and
           s.CD_FAZENDA = t.CD_FAZENDA and
           s.CD_ZONA = t.CD_ZONA and
           s.CD_TALHAO = t.CD_TALHAO

    -- Filtra apenas o que realmente mudou para evitar writes desnecessários no DS
    where t.ID_CLIENTE is null
       or coalesce(t.DESC_FAZENDA, '') <> coalesce(s.DESC_FAZENDA, '')
       or coalesce(t.DESC_TALHAO, '') <> coalesce(s.DESC_TALHAO, '')
       or coalesce(t.AREA_TOTAL, -999.99) <> coalesce(s.AREA_TOTAL, -999.99)
       or coalesce(t.DESC_PRODUTOR, '') <> coalesce(s.DESC_PRODUTOR, '')
       or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
         <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)                -- origem atualizou
       {% if HAS_FG_ATIVO %}
       or coalesce(t.FG_ATIVO, 1)               <> coalesce(s.FG_ATIVO, 1)            -- status mudou
       {% endif %}
)

select
    ID_CLIENTE,
    CD_FAZENDA,
    DESC_FAZENDA,
    CD_TALHAO,
    DESC_TALHAO,
    CD_ZONA,
    AREA_TOTAL,
    DESC_PRODUTOR,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
