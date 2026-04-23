# DAGs do Airflow

Este projeto usa Airflow como orquestrador da etapa analitica do pipeline.

O desenho atual e:

- uma DAG executa o Airbyte para `RAW`
- uma DAG executa o dbt para `DS -> DW`
- uma DAG orquestradora encadeia as duas quando o fluxo precisa ser fim a fim
- uma DAG separada faz a retencao tecnica do `DS`

## DAG principal

- [load_ds_airbyte_dimensions_dag.py](../dags/load_ds_airbyte_dimensions_dag.py)
- [load_dw_dbt_dimensions_dag.py](../dags/load_dw_dbt_dimensions_dag.py)
- [orchestrate_ds_dw_dimensions_dag.py](../dags/orchestrate_ds_dw_dimensions_dag.py)
- [cleanup_dimensions_retention_dag.py](../dags/cleanup_dimensions_retention_dag.py)
- [schedule_dimensions_incremental_dag.py](../dags/schedule_dimensions_incremental_dag.py)
- [monitor_pipeline_execution_dag.py](../dags/monitor_pipeline_execution_dag.py)

## O que a DAG faz

As DAGs principais ficaram separadas por responsabilidade:

- `load_ds_airbyte_dimensions_dag`
  1. registra `LAST_EXTRACT_STARTED_AT`
  2. dispara a sync do Airbyte
  3. registra `LAST_EXTRACT_ENDED_AT`

- `load_dw_dbt_dimensions_dag`
  1. executa `dbt build` no projeto `dbt/solix_dbt`
  2. roda os modelos informados no `conf`
  3. usa o watermark do proprio modelo para fazer incremental

- `orchestrate_ds_dw_dimensions_dag`
  1. dispara `load_ds_airbyte_dimensions_dag`
  2. aguarda conclusao com sucesso
  3. dispara `load_dw_dbt_dimensions_dag`
  4. aguarda conclusao com sucesso
  5. executa o cleanup tecnico do `RAW` da entidade, quando configurado

- `cleanup_dimensions_retention_dag`
  1. registra batch de cleanup
  2. executa deletes tecnicos por tabela do `DS`
  3. registra quantidade de linhas deletadas

- `monitor_pipeline_execution_dag`
  1. consulta a `CTL_BATCH_EXECUTION`
  2. valida se pipelines esperados tiveram `SUCCESS` dentro da janela configurada
  3. gera alerta quando uma execucao esperada nao aparece

Para `sx_equipamento`, o desenho de producao atual e:

- `Airbyte -> RAW`: global por dominio de dimensoes
- `RAW -> DS`: global
- `DS -> DW`: global

Decisao atual para custo e storage:

- o `RAW` deve ser tratado como aterrissagem tecnica `TRANSIENT` no Snowflake
- o `RAW` nao deve ser `TEMPORARY TABLE`
- o `RAW` e limpo ao final da execucao bem-sucedida da entidade
- a reducao de storage vem de cleanup tecnico, nao de `truncate`

As DAGs de agendamento foram preparadas para:

- disparar a DAG orquestradora com `conf` apropriado
- manter a execucao operacional incremental sem duplicar a logica da execucao
- representar o escopo real da connection do Airbyte

Isso permite dois modos operacionais:

- modo por produto
  - roda apenas `load_ds_airbyte_dimensions_dag`
  - ou roda apenas `load_dw_dbt_dimensions_dag`

- modo orquestrado
  - o Airflow usa `orchestrate_ds_dw_dimensions_dag`
  - primeiro executa o Airbyte
  - depois executa o dbt

No desenho atual de dimensoes compartilhadas:

- a extracao do Airbyte e do dominio `dimensions`
- o scheduler nao deve ter nome de entidade isolada
- os modelos dbt continuam explicitamente listados no `conf`

## Parametros de execucao

### `dag_run.conf`

Campos suportados:

- `models`
  - lista de modelos dbt ou string separada por virgula
- `airbyte_connection_id`
  - `connection_id` da conexao Airbyte a ser sincronizada
- `airbyte_timeout_seconds`
  - timeout maximo de espera da sync
- `airbyte_poll_interval_seconds`
  - intervalo de polling da API do Airbyte
- `dbt_vars`
  - dicionario opcional de variaveis repassadas ao dbt
- `watermark_pipeline_name`
  - nome do pipeline na `CTL_PIPELINE_WATERMARK`
- `cleanup_raw_specs`
  - lista opcional de deletes tecnicos do `RAW` para rodar ao final da orquestracao com sucesso

### Exemplo apenas com dbt

```json
{
  "models": ["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"]
}
```

### Exemplo apenas com Airbyte

```json
{
  "airbyte_connection_id": "404f8969-e421-416e-96f6-cd0434047acf",
  "watermark_pipeline_name": "dim_sx_equipamento_d"
}
```

### Exemplo orquestrado Airbyte -> dbt

```json
{
  "airbyte_connection_id": "404f8969-e421-416e-96f6-cd0434047acf",
  "models": ["ds_sx_equipamento_d", "stg_ds__sx_equipamento_d", "dim_sx_equipamento_d"],
  "dbt_vars": {},
  "watermark_pipeline_name": "dim_sx_equipamento_d",
  "cleanup_raw_specs": [
    {
      "step_name": "CLEANUP_RAW_VW_SX_EQUIPAMENTO_D",
      "target_name": "SOLIX_BI.RAW.VW_SX_EQUIPAMENTO_D",
      "description": "cleanup tecnico do RAW de dimensions apos sucesso do pipeline",
      "sql": "delete from SOLIX_BI.RAW.VW_SX_EQUIPAMENTO_D"
    }
  ]
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

Para alertas operacionais por webhook, o runtime do Airflow pode conhecer:

- `AIRFLOW_ALERT_WEBHOOK_URL`
  - opcional
  - se informado, recebe alertas de falha e retry das DAGs criticas

## Execucao local

- subir o Airbyte localmente via `abctl`
- subir a stack do Airflow com `docker compose -f docker-compose.local.yml up --build -d`
- acessar Airflow em `http://localhost:8080`
- configurar o Airbyte conforme [AIRBYTE.md](./AIRBYTE.md)
- usar `load_dw_dbt_dimensions_dag` para execucoes manuais
- usar `load_ds_airbyte_dimensions_dag` para execucoes manuais apenas de extracao
- usar `orchestrate_ds_dw_dimensions_dag` para execucoes manuais fim a fim
- usar `cleanup_dimensions_retention_dag` para limpeza tecnica manual de retencao
- usar `schedule_dimensions_incremental_dag` para o agendamento do dominio de dimensoes
- usar `monitor_pipeline_execution_dag` para validar ausencia de execucao esperada

## Modelo operacional recomendado

- `load_ds_airbyte_dimensions_dag`
  - DAG principal de extracao
  - sem `schedule` proprio
- `load_dw_dbt_dimensions_dag`
  - DAG principal de transformacao
  - sem `schedule` proprio
- `orchestrate_ds_dw_dimensions_dag`
  - DAG principal de orquestracao fim a fim
  - sem `schedule` proprio
- `cleanup_dimensions_retention_dag`
  - DAG separada para retencao tecnica de `DS`
  - com `schedule` diario proprio
- `schedule_dimensions_incremental_dag`
  - agenda a rotina incremental do dominio `dimensions`
  - dispara uma unica connection compartilhada do Airbyte para as dimensoes
  - recebe explicitamente os modelos dbt que pertencem ao dominio
  - nao expoe reprocessamento dbt nem `full_refresh`
- `monitor_pipeline_execution_dag`
  - monitora ausencia de execucao esperada
  - roda de hora em hora
  - alerta se `dim_sx_equipamento_d` nao tiver sucesso nas ultimas 14 horas
  - alerta se o cleanup diario nao tiver sucesso recente
  - facts horarias devem ser adicionadas com janela menor, por exemplo 120 minutos

## Pools recomendados

Para producao, criar estes pools no Airflow:

- `airbyte_sync_pool`
  - usado na task `sync_ds_airbyte`
  - recomendacao inicial: `1` slot quando houver uma unica `connection_id` por dominio
- `dbt_build_pool`
  - usado na task `run_dw_dbt`
  - recomendacao inicial: `2` a `4` slots, conforme capacidade do warehouse
- `retention_cleanup_pool`
  - usado nas tasks de cleanup tecnico
  - recomendacao inicial: `1` slot para evitar contenção com a carga principal

As DAGs de agendamento mais criticas foram configuradas com:

- `max_active_runs = 1`

Isso evita concorrencia desnecessaria do mesmo pipeline ao mesmo tempo.

## Fluxo operacional completo

O fluxo recomendado passa a ser:

1. `load_ds_airbyte_dimensions_dag`
   - grava `LAST_EXTRACT_STARTED_AT`
   - dispara e aguarda a sync do Airbyte
   - grava `LAST_EXTRACT_ENDED_AT`
2. `load_dw_dbt_dimensions_dag`
   - executa dbt incremental para `DS -> DW`
3. `orchestrate_ds_dw_dimensions_dag`
   - encadeia as duas DAGs acima quando o fluxo precisar ser completo
   - executa o cleanup do `RAW` ao final, apos sucesso completo, quando `cleanup_raw_specs` e informado

No caso de dimensoes compartilhadas:

1. `schedule_dimensions_incremental_dag`
   - dispara a orquestracao do dominio `dimensions`
2. `load_ds_airbyte_dimensions_dag`
   - sincroniza a connection compartilhada do Airbyte
3. `load_dw_dbt_dimensions_dag`
   - executa apenas os modelos do dominio listados no `conf`
4. `cleanup_raw_after_success`
   - limpa as RAWs pertencentes a mesma connection, quando configurado

Esse desenho preserva a separacao de responsabilidade:

- Airflow/Airbyte
  - metadados de extracao
- dbt
  - metadados de transformacao e carga

## Limites de responsabilidade

- Airflow nao implementa extracao customizada de fontes neste repositorio
- Airflow apenas integra com a API do Airbyte quando a sync precisa entrar no mesmo fluxo orquestrado
- transformacoes e modelagem continuam no `dbt`

## Retencao tecnica

O projeto usa duas abordagens complementares:

- `RAW`
  - cleanup tecnico por execucao da entidade
  - executado ao final da `orchestrate_ds_dw_dimensions_dag`, apos sucesso completo
- `DS`
  - cleanup tecnico em DAG separada

- `cleanup_dimensions_retention_dag`
  - `DS`: mantém apenas o dia atual com base em `cast(BI_UPDATED_AT as date)`
  - `schedule`: `50 23 * * *`
  - objetivo: executar apos a ultima janela principal do dia

Exemplo:

- se o cleanup rodar em `24/04/2026 01:00`
  - mantem tudo de `24/04/2026`
  - remove tudo de `23/04/2026` para tras

## Riscos Operacionais

Os principais riscos operacionais identificados hoje sao:

- comportamento de falha parcial ainda depende de disciplina operacional
- dependencia de configuracao correta do webhook de alerta
- timeout do Airbyte ainda exige acao operacional no Airbyte antes de rerun cego

Esses pontos nao invalidam a arquitetura, mas precisam de endurecimento antes de chamar a esteira de producao madura.

## Melhorias Recomendadas

### 1. Agenda da retencao do `DS`

Implementado:

- `cleanup_dimensions_retention_dag` com `schedule = 50 23 * * *`
- manter `max_active_runs = 1`
- manter `pool = retention_cleanup_pool`

Objetivo:

- garantir que o `DS` retenha apenas o dia calendario atual
- evitar crescimento silencioso de storage

### 2. Formalizar comportamento em falha parcial

O comportamento esperado da esteira deve ser este:

- falha em `load_ds_airbyte_dimensions_dag`
  - `load_dw_dbt_dimensions_dag` nao roda
  - cleanup do `RAW` nao roda
  - batch da orquestracao termina como `FAILED`

- falha em `load_dw_dbt_dimensions_dag`
  - cleanup do `RAW` nao roda
  - batch da orquestracao termina como `FAILED`
  - o `RAW` permanece disponivel para analise ou rerun

- falha em `cleanup_raw_after_success`
  - Airbyte e dbt ja concluiram
  - batch da orquestracao deve refletir falha
  - o dado analitico fica carregado, mas o `RAW` nao foi limpo

- falha em `cleanup_dimensions_retention_dag`
  - nao afeta a carga analitica ja concluida
  - afeta apenas a politica de retencao do `DS`
  - deve ser tratada como incidente operacional de manutencao

### 3. Alertas minimos

Implementado:

- callback de falha nas DAGs criticas
- callback de retry nas DAGs de `Airbyte`, `dbt` e cleanup
- alerta de ausencia de execucao esperada via `monitor_pipeline_execution_dag`
- envio para webhook quando `AIRFLOW_ALERT_WEBHOOK_URL` estiver configurada
- fallback para log quando o webhook nao estiver configurado

Cobertura atual:

- DAGs de agendamento criadas por `pipeline_patterns.py`
- `load_ds_airbyte_dimensions_dag`
- `load_dw_dbt_dimensions_dag`
- `orchestrate_ds_dw_dimensions_dag`
- `cleanup_dimensions_retention_dag`
- `monitor_pipeline_execution_dag`

### 4. Definir fallback para timeout do Airbyte

Implementado:

- mensagem de timeout com `connection_id`, `job_id`, `timeout_seconds` e `last_status`
- orientacao explicita para verificar o job no Airbyte antes de relancar a DAG

Quando a task do Airbyte estourar timeout, a operacao deve:

- verificar no Airbyte se o job ainda esta executando
- se o job ainda estiver rodando, nao relancar cegamente a DAG
- se o job tiver falhado, rerodar a esteira pelo Airflow
- registrar o `job_id`, `connection_id` e horario do incidente

Melhorias sugeridas:

- documentar timeout por entidade
- revisar se o timeout atual esta adequado ao volume real
- adicionar retry curto apenas para falhas transitorias de rede ou API

## Recomendacao Operacional

Para a POC operar mais proxima de producao, esta trilha agora conta com:

1. `schedule` ativo da `cleanup_dimensions_retention_dag`
2. callbacks minimos de alerta para falha e retry
3. fallback operacional explicito para timeout do Airbyte
4. watermark marcado como `FAILED` em falha operacional
5. `monitor_pipeline_execution_dag` para ausencia de execucao esperada
6. uso de `CTL_LOAD_AUDIT`, `CTL_BATCH_EXECUTION`, `CTL_PIPELINE_WATERMARK` e `VW_PIPELINE_RUN_STATUS` como base de troubleshooting

Com isso, o desenho fica mais robusto sem mudar a arquitetura principal:

- `Airbyte -> RAW`
- `RAW -> DS`
- `DS -> DW`
- cleanup do `RAW` por execucao
- cleanup do `DS` por agenda separada

## Estrutura de Auditoria

Na forma atual, as tabelas de auditoria seguem dois papeis:

- `CTL_LOAD_AUDIT`
  - event log append-only
  - varias linhas por `BATCH_ID`
  - cada step escreve `STARTED` e depois `SUCCESS` ou `FAILED`
- `CTL_BATCH_EXECUTION`
  - visao macro por execucao
  - uma linha por `BATCH_ID`
  - usada para status final, duracao e volume agregado
- `CTL_PIPELINE_WATERMARK`
  - estado operacional atual por pipeline
  - guarda ultimo sucesso, ultimo status e ultima mensagem de erro
- `VW_PIPELINE_RUN_STATUS`
  - view operacional para juntar batch, eventos e watermark
  - recomendada para troubleshooting rapido e dashboards simples

Esse desenho e o que normalmente se espera em um pipeline mais proximo de producao:

- uma tabela detalhada para troubleshooting por step
- uma tabela resumida para operacao e monitoracao macro
- uma tabela persistente para estado atual do pipeline
- uma view para consulta operacional consolidada
