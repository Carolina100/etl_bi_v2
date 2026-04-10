# DAGs do Airflow

Este projeto usa Airflow como orquestrador da etapa analitica do pipeline.

O desenho atual e:

- Airbyte sincroniza a camada `DS`
- Airflow coordena a ordem de execucao
- dbt transforma `DS` em `DW`

## DAG principal

- [load_dw_dbt_dag.py](../dags/load_dw_dbt_dag.py)
- [schedule_sx_estado_d_incremental_dag.py](../dags/schedule_sx_estado_d_incremental_dag.py)
- [schedule_sx_estado_d_full_dag.py](../dags/schedule_sx_estado_d_full_dag.py)

## O que a DAG faz

A DAG principal foi preparada para:

1. opcionalmente disparar uma sync do Airbyte via API
2. aguardar a conclusao da sync, quando configurado
3. executar `dbt build` no projeto `dbt/solix_dbt`

As duas DAGs de agendamento foram preparadas para:

- disparar a DAG principal com `conf` apropriado
- separar agenda incremental e agenda full sem duplicar a logica da execucao

Isso permite dois modos operacionais:

- modo simples
  - a sync do Airbyte acontece fora do Airflow
  - a DAG roda apenas o `dbt build`

- modo orquestrado
  - o Airflow recebe o `airbyte_connection_id`
  - dispara a sync do Airbyte
  - espera sucesso
  - roda o `dbt build`

## Parametros de execucao

### `dag_run.conf`

Campos suportados:

- `models`
  - lista de modelos dbt ou string separada por virgula
- `airbyte_connection_id`
  - `connection_id` da conexao Airbyte a ser sincronizada
- `wait_for_airbyte`
  - `true` por padrao
- `airbyte_timeout_seconds`
  - timeout maximo de espera da sync
- `airbyte_poll_interval_seconds`
  - intervalo de polling da API do Airbyte
- `reconciliation_mode`
  - `incremental` ou `full`
  - controla quando o dbt pode inativar ausentes em `DS.SX_ESTADO_D`

### Exemplo apenas com dbt

```json
{
  "models": ["stg_ds__sx_estado_d", "dim_sx_estado_d"]
}
```

### Exemplo com sync Airbyte antes do dbt

```json
{
  "airbyte_connection_id": "00000000-0000-0000-0000-000000000000",
  "models": ["stg_ds__sx_estado_d", "dim_sx_estado_d"],
  "reconciliation_mode": "incremental",
  "wait_for_airbyte": true,
  "airbyte_timeout_seconds": 3600,
  "airbyte_poll_interval_seconds": 15
}
```

### Exemplo de reconciliacao full

```json
{
  "airbyte_connection_id": "00000000-0000-0000-0000-000000000000",
  "models": ["ds_sx_estado_d", "stg_ds__sx_estado_d", "dim_sx_estado_d"],
  "reconciliation_mode": "full",
  "wait_for_airbyte": true
}
```

## Requisitos de ambiente

Para o disparo da sync via API, o runtime do Airflow precisa conhecer:

- `AIRBYTE_API_URL`
- `AIRBYTE_API_TOKEN`, quando a instalacao exigir autenticacao

No Docker local deste projeto, o recomendado e:

- Airbyte rodando localmente via `abctl`
- Airflow acessando a API do Airbyte por `http://host.docker.internal:8000`

Opcionalmente, o `connection_id` pode vir de:

- `dag_run.conf`
- parametro padrao da DAG
- variavel de ambiente externa usada na automacao do deploy

## Execucao local

- subir o Airbyte localmente via `abctl`
- subir a stack do Airflow com `docker compose -f docker-compose.local.yml up --build -d`
- acessar Airflow em `http://localhost:8080`
- configurar o Airbyte conforme [AIRBYTE.md](./AIRBYTE.md)
- usar `load_dw_dbt_dag` para execucoes manuais
- usar `schedule_sx_estado_d_incremental_dag` para o agendamento frequente
- usar `schedule_sx_estado_d_full_dag` para a reconciliacao diaria

## Modelo operacional recomendado

- `load_dw_dbt_dag`
  - DAG principal de execucao
  - sem `schedule` proprio
- `schedule_sx_estado_d_incremental_dag`
  - agenda a rotina incremental
- `schedule_sx_estado_d_full_dag`
  - agenda a rotina full de reconciliacao

## Limites de responsabilidade

- Airflow nao implementa extracao customizada de Oracle ou PostgreSQL
- Airflow apenas integra com a API do Airbyte quando a sync precisa entrar no mesmo fluxo orquestrado
- transformacoes e modelagem continuam no `dbt`
