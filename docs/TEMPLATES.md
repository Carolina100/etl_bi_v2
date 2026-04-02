# Templates do Projeto

Este arquivo resume o que copiar para criar uma nova tabela no fluxo completo `Oracle -> DS -> DW`.

## Ordem recomendada

1. Copiar a pipeline DS:
   - [template pipeline DS](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/src/pipelines/_template_load_ds_pipeline.py)
2. Copiar a DAG:
   - [template DAG Airflow](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dags/_template_oracle_ds_to_dw_dag.py)
3. Copiar o staging do dbt:
   - [template staging dbt](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dbt/solix_dbt/models/staging/ds/_template_stg_ds__entity.sql)
4. Copiar a dimensão do dbt:
   - [template dimensão dbt](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dbt/solix_dbt/models/marts/dimensions/_template_dim_entity.sql)

## O que trocar em cada camada

### 1. Pipeline DS

Troque:
- `PIPELINE_NAME`
- `OUTPUT_FOLDER_NAME`
- `TARGET_TABLE`
- `SOURCE_NAME`
- `TARGET_COLUMNS`
- `ORACLE_EXTRACTION_QUERY`

### 2. DAG

Troque:
- `DAG_ID`
- `PIPELINE_DESCRIPTION`
- `TAGS`
- import da pipeline em `src.pipelines`
- `DBT_SELECT_MODELS`
- `DW_PIPELINE_NAME`
- `DW_SOURCE_NAME`
- `DW_TARGET_NAME`

### 3. Staging dbt

Troque:
- `SOURCE_TABLE_NAME`
- `tags`
- colunas da tabela DS
- casts
- chave natural no `qualify row_number()`

### 4. Dimensão dbt

Troque:
- `MODEL_ALIAS`
- `STAGING_MODEL_NAME`
- `SEQUENCE_NAME`
- `SURROGATE_KEY_COLUMN`
- `NATURAL_KEY_COLUMNS`
- campos de negócio
- regra do órfão

## Regras de ouro

- No `DS`, a lista de colunas deve bater com a tabela Snowflake.
- No `DW`, a chave natural usada no `merge` deve refletir a unicidade da dimensão.
- O registro órfão deve continuar técnico e estável, normalmente com `-1`.
- `ETL_LOADED_AT` deve ser gravado em horário de Brasília.
- Em multi-cliente, a auditoria do `DW` pode usar `ID_CLIENTE = -1`.

## Estratégia de agendamento e retries

### Padrão recomendado para começar

- `schedule=None`
  Use execução manual enquanto a tabela ainda está sendo validada.
- `max_active_runs=1`
  Evita duas execuções da mesma DAG competindo entre si.
- `max_active_tasks=4`
  Limita o paralelismo total da DAG e ajuda a proteger Oracle e Snowflake.

### Camada DS por cliente

- `retries=3`
- `retry_delay=5 minutos`
- `retry_exponential_backoff=True`
- `max_retry_delay=30 minutos`

Motivo:
- a task do `DS` costuma falhar por conexão, rede ou indisponibilidade temporária
- como ela é mapeada por cliente, só o cliente com erro tenta novamente

### Camada DW via dbt

- `retries=1`
- `retry_delay=5 minutos`

Motivo:
- o `DW` roda uma vez ao final
- se falhar, normalmente uma nova tentativa já é suficiente
- retries demais no `dbt` tendem a repetir o mesmo erro sem ganho

### Quando evoluir

Depois que a tabela estabilizar, o mais comum é:

- trocar `schedule=None` por um agendamento diário
- exemplo: `0 6 * * *`
- avaliar `pool` do Airflow para limitar quantos clientes podem bater no Oracle ao mesmo tempo
