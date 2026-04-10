{#
  MACRO: discover_raw_client_tables

  OBJETIVO:
  - Descobrir dinamicamente todas as tabelas de clientes no schema RAW
    para uma entidade específica, usando o STREAM_PREFIX armazenado em
    DS.CTL_AIRBYTE_CONEXOES.
  - Retorna pares (TABLE_NAME, ID_CLIENTE) prontos para iteração no modelo.

  ARGUMENTO:
  - entity_table_suffix: sufixo do nome da tabela no RAW (ex: 'CDT_ESTADO')
    A busca é feita por: TABLE_NAME ILIKE '%_<sufixo>' E STARTSWITH(TABLE_NAME, STREAM_PREFIX)

  EXEMPLO DE USO:
    {% set client_tables_sql %}
        {{ discover_raw_client_tables('CDT_ESTADO') }}
    {% endset %}
    {% set results = run_query(client_tables_sql) %}
    {% for row in results.rows %}
        -- row[0] = TABLE_NAME, row[1] = ID_CLIENTE
    {% endfor %}

  CONVENCAO:
  - Tabelas no RAW seguem o padrao: {STREAM_PREFIX}{NOME_TABELA_POSTGRES} em maiusculas
  - Exemplo: STREAM_PREFIX='AMAGGI_' + entidade 'CDT_ESTADO' = 'AMAGGI_CDT_ESTADO'
  - STREAM_PREFIX e definido em DS.CTL_AIRBYTE_CONEXOES por conexao cadastrada

  ESCALABILIDADE:
  - Novos clientes sao incluidos automaticamente ao inserir uma linha na CTL
    com FI_ATIVO=TRUE e o STREAM_PREFIX correto. Zero alteracao de codigo.
#}

{% macro discover_raw_client_tables(entity_table_suffix) %}

    SELECT
        t.TABLE_NAME,
        c.ID_CLIENTE
    FROM SOLIX_BI.INFORMATION_SCHEMA.TABLES t
    JOIN SOLIX_BI.DS.CTL_AIRBYTE_CONEXOES c
        ON STARTSWITH(t.TABLE_NAME, c.STREAM_PREFIX)
    WHERE t.TABLE_SCHEMA = 'RAW'
      AND t.TABLE_NAME   ILIKE '%_{{ entity_table_suffix }}'
      AND c.FI_ATIVO     = TRUE
    ORDER BY c.ID_CLIENTE

{% endmacro %}
