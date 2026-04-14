# DAGs do Airflow

Este projeto usa Airflow como orquestrador da etapa analitica do pipeline.

O desenho atual e:

- uma DAG executa o Airbyte para `DS`
- uma DAG executa o dbt para `DW`
- uma DAG orquestradora encadeia as duas quando o fluxo precisa ser fim a fim

## DAG principal

- [load_ds_airbyte_dag.py](../dags/load_ds_airbyte_dag.py)
- [load_dw_dbt_dag.py](../dags/load_dw_dbt_dag.py)
- [orchestrate_ds_dw_dag.py](../dags/orchestrate_ds_dw_dag.py)
- [schedule_sx_estado_d_incremental_dag.py](../dags/schedule_sx_estado_d_incremental_dag.py)
- [schedule_sx_estado_d_full_dag.py](../dags/schedule_sx_estado_d_full_dag.py)
- [schedule_sx_equipamento_d_incremental_dag.py](../dags/schedule_sx_equipamento_d_incremental_dag.py)
- [schedule_sx_equipamento_d_full_dag.py](../dags/schedule_sx_equipamento_d_full_dag.py)

## O que a DAG faz

As DAGs principais ficaram separadas por produto:

- `load_ds_airbyte_dag`
  1. registra `LAST_EXTRACT_STARTED_AT`
  2. dispara a sync do Airbyte
  3. registra `LAST_EXTRACT_ENDED_AT`

- `load_dw_dbt_dag`
  1. executa `dbt build` no projeto `dbt/solix_dbt`
  2. pode rodar modelos compartilhados uma vez
  3. pode rodar modelos por cliente, continuando os demais mesmo com falha pontual

- `orchestrate_ds_dw_dag`
  1. dispara `load_ds_airbyte_dag`
  2. aguarda conclusao com sucesso
  3. dispara `load_dw_dbt_dag`
  4. aguarda conclusao com sucesso

As DAGs de agendamento foram preparadas para:

- disparar a DAG orquestradora com `conf` apropriado
- separar agenda incremental e agenda full sem duplicar a logica da execucao

Isso permite dois modos operacionais:

- modo por produto
  - roda apenas `load_ds_airbyte_dag`
  - ou roda apenas `load_dw_dbt_dag`

- modo orquestrado
  - o Airflow usa `orchestrate_ds_dw_dag`
  - primeiro executa o Airbyte
  - depois executa o dbt

## Parametros de execucao

### `dag_run.conf`

Campos suportados:

- `models`
  - lista de modelos dbt ou string separada por virgula
- `client_models`
  - lista de modelos dbt que devem rodar cliente a cliente
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
  - controla se a execucao deve processar apenas delta ou fazer reconciliacao completa da entidade
- `dbt_vars`
  - dicionario opcional de variaveis repassadas ao dbt
- `full_refresh`
  - `true` ou `false`
  - quando `true`, executa o dbt com `--full-refresh`
- `continue_on_client_error`
  - `true` ou `false`
  - quando `true`, continua processando os demais clientes mesmo que um cliente falhe
- `watermark_pipeline_name`
  - nome do pipeline na `CTL_PIPELINE_WATERMARK`
- `watermark_id_clientes`
  - lista de clientes afetados pela extracao
  - usar `[0]` para pipeline global
- `watermark_client_source_table`
  - tabela usada para buscar todos os clientes quando `watermark_id_clientes` nao for informado
- `watermark_client_id_column`
  - coluna do id do cliente na tabela de clientes
- `watermark_client_active_column`
  - coluna opcional para filtrar clientes ativos

### Exemplo apenas com dbt

```json
{
  "models": ["stg_ds__sx_estado_d", "dim_sx_estado_d"]
}
```

### Exemplo apenas com Airbyte

```json
{
  "airbyte_connection_id": "00000000-0000-0000-0000-000000000000",
  "watermark_pipeline_name": "dim_sx_estado_d",
  "watermark_id_clientes": [0]
}
```

### Exemplo orquestrado Airbyte -> dbt

```json
{
  "airbyte_connection_id": "00000000-0000-0000-0000-000000000000",
  "models": ["stg_ds__sx_estado_d", "dim_sx_estado_d"],
  "reconciliation_mode": "incremental",
  "full_refresh": false,
  "dbt_vars": {},
  "watermark_pipeline_name": "dim_sx_estado_d",
  "watermark_id_clientes": [0]
}
```

### Exemplo de reconciliacao full

```json
{
  "airbyte_connection_id": "00000000-0000-0000-0000-000000000000",
  "models": ["ds_sx_estado_d", "stg_ds__sx_estado_d", "dim_sx_estado_d"],
  "reconciliation_mode": "full",
  "full_refresh": true,
  "dbt_vars": {
    "sx_estado_d_reconciliation_mode": "full"
  },
  "watermark_pipeline_name": "dim_sx_estado_d",
  "watermark_id_clientes": [0]
}
```

## Requisitos de ambiente

Para o disparo da sync via API, o runtime do Airflow precisa conhecer:

- `AIRBYTE_API_URL`
- `AIRBYTE_API_TOKEN`, quando a instalacao exigir autenticacao

No Docker local deste projeto, o recomendado e:

- Airbyte Cloud acessado via API HTTPS
- Airflow com as credenciais da API do Airbyte Cloud no ambiente

O `connection_id` pode vir de:

- `dag_run.conf`
- parametro padrao da DAG

Para Airbyte Cloud, o runtime do Airflow tambem precisa conhecer:

- `AIRBYTE_CLOUD_CLIENT_ID`
- `AIRBYTE_CLOUD_CLIENT_SECRET`
- `AIRBYTE_CLOUD_API_URL`
  - opcional
  - padrao: `https://api.airbyte.com/v1`

## Execucao local

- subir o Airbyte localmente via `abctl`
- subir a stack do Airflow com `docker compose -f docker-compose.local.yml up --build -d`
- acessar Airflow em `http://localhost:8080`
- configurar o Airbyte conforme [AIRBYTE.md](./AIRBYTE.md)
- usar `load_dw_dbt_dag` para execucoes manuais
- usar `load_ds_airbyte_dag` para execucoes manuais apenas de extracao
- usar `orchestrate_ds_dw_dag` para execucoes manuais fim a fim
- usar `schedule_sx_estado_d_incremental_dag` para o agendamento frequente
- usar `schedule_sx_estado_d_full_dag` para a reconciliacao diaria
- usar `schedule_sx_equipamento_d_incremental_dag` para o agendamento frequente do equipamento
- usar `schedule_sx_equipamento_d_full_dag` para a reconciliacao do equipamento

## Modelo operacional recomendado

- `load_ds_airbyte_dag`
  - DAG principal de extracao
  - sem `schedule` proprio
- `load_dw_dbt_dag`
  - DAG principal de transformacao
  - sem `schedule` proprio
- `orchestrate_ds_dw_dag`
  - DAG principal de orquestracao fim a fim
  - sem `schedule` proprio
- `schedule_sx_estado_d_incremental_dag`
  - agenda a rotina incremental
- `schedule_sx_estado_d_full_dag`
  - agenda a rotina full de reconciliacao
- `schedule_sx_equipamento_d_incremental_dag`
  - agenda a rotina incremental do equipamento
- `schedule_sx_equipamento_d_full_dag`
  - agenda a rotina full de reconciliacao do equipamento

## Pools recomendados

Para producao, criar estes pools no Airflow:

- `airbyte_sync_pool`
  - usado na task `sync_ds_airbyte`
  - recomendacao inicial: `1` ou `2` slots
- `dbt_build_pool`
  - usado na task `run_dw_dbt`
  - recomendacao inicial: `2` a `4` slots, conforme capacidade do warehouse

As DAGs de agendamento mais criticas foram configuradas com:

- `max_active_runs = 1`

Isso evita concorrencia desnecessaria do mesmo pipeline ao mesmo tempo.

## Fluxo operacional completo

O fluxo recomendado passa a ser:

1. `load_ds_airbyte_dag`
   - grava `LAST_EXTRACT_STARTED_AT`
   - dispara e aguarda a sync do Airbyte
   - grava `LAST_EXTRACT_ENDED_AT`
2. `load_dw_dbt_dag`
   - executa dbt para `DS -> DW`
3. `orchestrate_ds_dw_dag`
   - encadeia as duas DAGs acima quando o fluxo precisar ser completo

Esse desenho preserva a separacao de responsabilidade:

- Airflow/Airbyte
  - metadados de extracao
- dbt
  - metadados de transformacao e carga

## Regra de clientes para pipelines por cliente

Para pipelines segregados por cliente, a regra recomendada e:

- se `watermark_id_clientes` for informado:
  - a DAG registra extract start/end apenas para os clientes informados
- se `watermark_id_clientes` nao for informado:
  - a DAG busca todos os clientes na tabela configurada em `watermark_client_source_table`
  - e registra extract start/end para todos eles

Exemplo adotado para `sx_equipamento`:

- `watermark_pipeline_name = 'dim_sx_equipamento_d'`
- `watermark_id_clientes = null`
- `watermark_client_source_table = 'SOLIX_BI.DS.SX_CLIENTE_D'`
- `watermark_client_id_column = 'ID_CLIENTE'`
- `watermark_client_active_column = 'FL_ATIVO'`

## Reprocessamento manual por cliente e data

Para `sx_equipamento`, o desenho recomendado e:

- se nao informar `watermark_id_clientes`
  - a extracao registra todos os clientes ativos
- se informar `watermark_id_clientes`
  - a extracao registra apenas os clientes desejados
- se nao informar data de reprocessamento
  - o dbt roda incremental normal
- se informar data de reprocessamento
  - o dbt reprocessa a partir da data informada
- no modo atual de producao do equipamento
  - `DS -> STG -> DIM` rodam por cliente
  - o Airbyte continua rodando uma vez por tabela/connection
  - se um cliente falhar no dbt, os demais continuam e a falha fica auditada

Exemplo manual para reprocessar apenas o cliente `7` desde `2026-04-01 00:00:00`:

```json
{
  "airbyte_connection_id": "404f8969-e421-416e-96f6-cd0434047acf",
  "models": [],
  "client_models": ["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"],
  "reconciliation_mode": "incremental",
  "full_refresh": false,
  "continue_on_client_error": true,
  "dbt_vars": {
    "dim_sx_equipamento_d_reprocess_from": "2026-04-01 00:00:00"
  },
  "watermark_pipeline_name": "dim_sx_equipamento_d",
  "watermark_id_clientes": [7],
  "watermark_client_source_table": "SOLIX_BI.DS.SX_CLIENTE_D",
  "watermark_client_id_column": "ID_CLIENTE",
  "watermark_client_active_column": "FL_ATIVO",
  "airbyte_timeout_seconds": 3600,
  "airbyte_poll_interval_seconds": 15
}
```

## Limites de responsabilidade

- Airflow nao implementa extracao customizada de Oracle ou PostgreSQL
- Airflow apenas integra com a API do Airbyte quando a sync precisa entrar no mesmo fluxo orquestrado
- transformacoes e modelagem continuam no `dbt`
