# Modelos e replicação

Este projeto agora privilegia a ingestão via Airbyte e a transformação via dbt.

- `docs/AIRBYTE.md`: configuração local do Airbyte
- `dags/load_dw_dbt_dag.py`: DAG Airflow para executar dbt
- `dbt/solix_dbt/models/staging/`: modelos de staging
- `dbt/solix_dbt/models/marts/dimensions/`: modelos de dimensão
- `docs/INCREMENTAL_LOADING.md`: padrão incremental

O código Python de ingestão DS foi removido; use Airbyte para Oracle/PostgreSQL -> Snowflake e dbt para transformar em DW.
