# Replicação do Projeto

Este projeto passou a adotar um modelo simples:

- a entidade `SX_ESTADO_D` e a referencia funcional completa
- novas entidades devem ser criadas copiando esse fluxo real
- nao considere templates antigos como fonte principal

## Arquivos de referência

Para criar uma nova entidade no fluxo `Oracle -> DS -> DW`, use estes arquivos como base:

1. Pipeline DS
   - [load_sx_estado_d.py](../src/pipelines/load_sx_estado_d.py)
2. DAG Airflow
   - [load_sx_estado_d_dag.py](../dags/load_sx_estado_d_dag.py)
3. Staging dbt
   - [stg_ds__sx_estado_d.sql](../dbt/solix_dbt/models/staging/ds/stg_ds__sx_estado_d.sql)
4. Dimensão dbt
   - [dim_sx_estado_d.sql](../dbt/solix_dbt/models/marts/dimensions/dim_sx_estado_d.sql)
5. Regras incrementais
   - [INCREMENTAL_LOADING.md](INCREMENTAL_LOADING.md)

## Ordem recomendada

1. Criar a tabela DS no Snowflake
2. Copiar a pipeline DS e ajustar as constantes
3. Garantir a chave natural da entidade
4. Garantir a coluna de atualização da origem
5. Criar ou ajustar a DAG
6. Criar source e staging no dbt
7. Criar dimensão DW
8. Criar sequence do DW, se necessário
9. Testar primeiro a camada DS
10. Testar depois o dbt e a DAG completa

## O que trocar na pipeline DS

Ao copiar [load_sx_estado_d.py](../src/pipelines/load_sx_estado_d.py), troque:

- `PIPELINE_NAME`
- `OUTPUT_FOLDER_NAME`
- `TARGET_TABLE`
- `SOURCE_NAME`
- `TARGET_COLUMNS`
- `NATURAL_KEY_COLUMNS`
- `SOURCE_UPDATED_AT_COLUMN`
- `ORACLE_EXTRACTION_QUERY`

Mantenha o padrão atual:

- carga incremental por watermark
- backfill manual por `data_inicio` e `data_fim`
- `MERGE` no DS
- colunas técnicas `BI_CREATED_AT` e `BI_UPDATED_AT`

## O que trocar na DAG

Ao copiar [load_sx_estado_d_dag.py](../dags/load_sx_estado_d_dag.py), troque:

- `DAG_ID`
- `PIPELINE_DESCRIPTION`
- `TAGS`
- import da pipeline em `run_ds_pipeline_task`
- `DBT_SELECT_MODELS`
- `DW_PIPELINE_NAME`
- `DW_SOURCE_NAME`
- `DW_TARGET_NAME`

Mantenha o padrão atual:

- fila `ds` para a camada Python
- fila `dbt` para a camada DW
- execução incremental por padrão
- backfill manual como exceção operacional

## O que trocar no staging dbt

Ao copiar [stg_ds__sx_estado_d.sql](../dbt/solix_dbt/models/staging/ds/stg_ds__sx_estado_d.sql), troque:

- nome do source
- colunas da tabela DS
- casts
- chave natural no `row_number()`

Mantenha o padrão atual:

- usar `BI_UPDATED_AT` para priorizar a linha mais recente
- manter `ETL_BATCH_ID`
- usar `dbt_utils.unique_combination_of_columns` quando a chave natural for composta

## O que trocar na dimensão DW

Ao copiar [dim_sx_estado_d.sql](../dbt/solix_dbt/models/marts/dimensions/dim_sx_estado_d.sql), troque:

- alias da tabela
- sequence
- surrogate key
- chave natural
- campos de negócio
- regras do registro órfão

## Regras de ouro

- use como base o caso real de `SX_ESTADO_D`, nao um template paralelo
- mantenha `LAST_SOURCE_UPDATED_AT` fiel ao valor da origem
- mantenha `UPDATED_AT` da tabela de watermark em horário de Brasília
- atualize o watermark apenas em execução bem-sucedida
- preserve `BI_CREATED_AT` e atualize `BI_UPDATED_AT` no `MERGE`
- documente a chave natural da entidade antes de começar a implementação

## Checklist rápido

- a tabela DS existe no Snowflake
- a role de ingestão tem permissão na tabela DS e na `CTL_PIPELINE_WATERMARK`
- a coluna de atualização da origem foi validada
- a sequence do DW existe
- `dbt deps` foi executado após adicionar ou atualizar pacotes em [packages.yml](../dbt/solix_dbt/packages.yml)
- source/staging/dimensão do dbt foram criados
- a DAG foi ajustada para os nomes corretos
- o fluxo DS rodou sozinho antes do teste completo no Airflow
