# Modelos e replicação

Este projeto agora privilegia a ingestão via Airbyte e a transformação via dbt.

- `docs/AIRBYTE.md`: configuração local do Airbyte
- `dags/load_dw_dbt_dimensions_dag.py`: DAG Airflow para executar dbt de dimensoes
- `dags/pipeline_patterns.py`: padrão reutilizável para novas trilhas de dimensões e fatos
- `dbt/solix_dbt/models/staging/`: modelos de staging
- `dbt/solix_dbt/models/marts/dimensions/`: modelos de dimensão
- `docs/INCREMENTAL_LOADING.md`: padrão incremental

O código Python de ingestão DS foi removido; use Airbyte para ingestão em `RAW` e dbt para transformar em `DS` e `DW`.

## Padrão de trilhas

### Dimensões

Arquivos-base atuais:

- `dags/load_ds_airbyte_dimensions_dag.py`
- `dags/load_dw_dbt_dimensions_dag.py`
- `dags/orchestrate_ds_dw_dimensions_dag.py`
- `dags/cleanup_dimensions_retention_dag.py`

Scheduler recomendado por dominio de dimensão:

- `dags/schedule_dimensions_incremental_dag.py`

Regra nova do projeto:

- se varias dimensoes compartilham a mesma connection do Airbyte, o scheduler deve ser do dominio `dimensions`
- o Airbyte e tratado como extracao global do dominio
- o dbt continua separado por modelos
- nao criar scheduler por entidade quando a extracao real for compartilhada

Quando novas dimensoes entrarem:

- adicionar os modelos dbt no scheduler de `dimensions`
- adicionar as RAWs correspondentes no cleanup do mesmo scheduler
- manter um unico schedule para a connection compartilhada

### Fatos

Padrão reservado para a próxima etapa:

- `dags/load_ds_airbyte_facts_dag.py`
- `dags/load_dw_dbt_facts_dag.py`
- `dags/orchestrate_ds_dw_facts_dag.py`
- `dags/cleanup_facts_retention_dag.py`

Scheduler por fato:

- `dags/schedule_<fato>_incremental_dag.py`

## Convenções operacionais

- dimensões e fatos não compartilham nomes genéricos de DAG
- a connection do Airbyte deve ser explícita no `conf`
- dimensões com connection compartilhada devem usar scheduler por dominio
- para fatos, a recomendação do projeto é connection separada por fato
- o helper `build_raw_cleanup_specs` em `dags/pipeline_patterns.py` deve ser reutilizado para novos pipelines
