# Modelos e replicação

Este projeto agora privilegia a ingestão via Airbyte e a transformação via dbt.

- `docs/AIRBYTE.md`: configuração local do Airbyte
- `dags/load_dw_dbt_dimensions_dag.py`: DAG Airflow para executar dbt de dimensoes
- `dags/pipeline_patterns.py`: padrão reutilizável para novas trilhas de dimensões e fatos
- `dbt/solix_dbt/models/staging/`: modelos de staging
- `dbt/solix_dbt/models/marts/dimensions/`: modelos de dimensão
- `docs/INCREMENTAL_LOADING.md`: padrão incremental

O código Python de ingestão DS foi removido; use Airbyte para Oracle/PostgreSQL -> Snowflake e dbt para transformar em DW.

## Padrão de trilhas

### Dimensões

Arquivos-base atuais:

- `dags/load_ds_airbyte_dimensions_dag.py`
- `dags/load_dw_dbt_dimensions_dag.py`
- `dags/orchestrate_ds_dw_dimensions_dag.py`
- `dags/cleanup_dimensions_retention_dag.py`

Scheduler por entidade de dimensão:

- `dags/schedule_<entidade>_incremental_dag.py`

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
- para fatos, a recomendação do projeto é connection separada por fato
- o helper `build_raw_cleanup_specs` em `dags/pipeline_patterns.py` deve ser reutilizado para novos pipelines
