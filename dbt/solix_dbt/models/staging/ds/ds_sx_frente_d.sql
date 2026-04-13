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
{% set ENTITY_RAW_TABLE = 'CDT_FRENTE' %}

{# (2) Nome da tabela destino que será criada/atualizada no schema DS #}
{% set MODEL_ALIAS = 'SX_FRENTE_D' %}

{# (3) Colunas que identificam unicamente um registro (chave para o merge) #}
{% set NATURAL_KEY_COLUMNS = ['ID_CLIENTE', 'CD_CORPORATIVO', 'CD_REGIONAL', 'CD_UNIDADE', 'CD_FRENTE'] %}

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
        cast(CD_CORPORATIVO            as number(38, 0)  ) as CD_CORPORATIVO,
        cast(CD_REGIONAL               as number(38, 0)  ) as CD_REGIONAL,
        cast(CD_UNIDADE                as number(38, 0)  ) as CD_UNIDADE,
        cast(CD_FRENTE                 as number(38, 0)  ) as CD_FRENTE,
        cast(DESC_CORPORATIVO          as varchar        ) as DESC_CORPORATIVO,
        cast(DESC_REGIONAL             as varchar        ) as DESC_REGIONAL,
        cast(DESC_UNIDADE              as varchar        ) as DESC_UNIDADE,
        cast(DESC_FRENTE               as varchar        ) as DESC_FRENTE,
        cast(FG_FRENTE_TRABALHO        as number(38, 0)  ) as FG_FRENTE_TRABALHO,
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
        partition by ID_CLIENTE, CD_CORPORATIVO, CD_REGIONAL, CD_UNIDADE, CD_FRENTE
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
        CD_CORPORATIVO,
        CD_REGIONAL,
        CD_UNIDADE,
        CD_FRENTE,
        DESC_CORPORATIVO,
        DESC_REGIONAL,
        DESC_UNIDADE,
        DESC_FRENTE,
        FG_FRENTE_TRABALHO,,
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
        cast(null as number(38, 0)  ) as CD_CORPORATIVO,
        cast(null as number(38, 0)  ) as CD_REGIONAL,
        cast(null as number(38, 0)  ) as CD_UNIDADE,
        cast(null as number(38, 0)  ) as CD_FRENTE,
        cast(null as varchar        ) as DESC_CORPORATIVO,
        cast(null as varchar        ) as DESC_REGIONAL,
        cast(null as varchar        ) as DESC_UNIDADE,
        cast(null as varchar        ) as DESC_FRENTE,
        cast(null as number(38, 0)  ) as FG_FRENTE_TRABALHO,,
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
        s.CD_CORPORATIVO,
        s.CD_REGIONAL,
        s.CD_UNIDADE,
        s.CD_FRENTE,
        s.DESC_CORPORATIVO,
        s.DESC_REGIONAL,
        s.DESC_UNIDADE,
        s.DESC_FRENTE,
        s.FG_FRENTE_TRABALHO,,
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
                        when coalesce(t.DESC_CORPORATIVO, '') <> coalesce(s.DESC_CORPORATIVO, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_REGIONAL, '') <> coalesce(s.DESC_REGIONAL, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_UNIDADE, '') <> coalesce(s.DESC_UNIDADE, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.DESC_FRENTE, '') <> coalesce(s.DESC_FRENTE, '')
                then convert_timezone('UTC', current_timestamp())::timestamp_ntz
            when coalesce(t.FG_FRENTE_TRABALHO, -999) <> coalesce(s.FG_FRENTE_TRABALHO, -999)
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
           s.CD_CORPORATIVO = t.CD_CORPORATIVO and
           s.CD_REGIONAL = t.CD_REGIONAL and
           s.CD_UNIDADE = t.CD_UNIDADE and
           s.CD_FRENTE = t.CD_FRENTE

    -- Filtra apenas o que realmente mudou para evitar writes desnecessários no DS
    where t.ID_CLIENTE is null
       or coalesce(t.DESC_CORPORATIVO, '') <> coalesce(s.DESC_CORPORATIVO, '')
       or coalesce(t.DESC_REGIONAL, '') <> coalesce(s.DESC_REGIONAL, '')
       or coalesce(t.DESC_UNIDADE, '') <> coalesce(s.DESC_UNIDADE, '')
       or coalesce(t.DESC_FRENTE, '') <> coalesce(s.DESC_FRENTE, '')
       or coalesce(t.FG_FRENTE_TRABALHO, -999) <> coalesce(s.FG_FRENTE_TRABALHO, -999)
       or coalesce(t.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)
         <> coalesce(s.SOURCE_UPDATED_AT, '1900-01-01'::timestamp_ntz)                -- origem atualizou
       {% if HAS_FG_ATIVO %}
       or coalesce(t.FG_ATIVO, 1)               <> coalesce(s.FG_ATIVO, 1)            -- status mudou
       {% endif %}
)

select
    ID_CLIENTE,
    CD_CORPORATIVO,
    CD_REGIONAL,
    CD_UNIDADE,
    CD_FRENTE,
    DESC_CORPORATIVO,
    DESC_REGIONAL,
    DESC_UNIDADE,
    DESC_FRENTE,
    FG_FRENTE_TRABALHO,
    FG_ATIVO,
    ETL_BATCH_ID,
    BI_CREATED_AT,
    BI_UPDATED_AT,
    SOURCE_UPDATED_AT,
    AIRBYTE_EXTRACTED_AT
from changed_or_new
