# Operacao de Watermark no DW

Este documento descreve como operar a tabela `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK` no fluxo fim a fim.

Os pilotos atuais sao:

- `dim_sx_estado_d`
- `dim_sx_equipamento_d`

## Objetivo

Usar a tabela de controle para:

- registrar o inicio e o fim da extracao upstream
- executar incremental normal sem lookback arbitrario
- permitir reprocessamento parcial por data
- permitir reconstrucao total com `full-refresh`
- registrar o ultimo watermark processado com sucesso

## Tabela de controle

Tabela:

- `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK`

Coluna principal de watermark no fluxo `DS -> DW`:

- `LAST_BI_UPDATED_AT`

Para as dimensoes atuais, os pipelines registrados sao:

- `PIPELINE_NAME = 'dim_sx_estado_d'`
- `PIPELINE_NAME = 'dim_sx_equipamento_d'`

Convencao de chave:

- pipeline global:
  - `PIPELINE_NAME`

## Responsabilidade por coluna

Metadados de extracao:

- `LAST_EXTRACT_STARTED_AT`
- `LAST_EXTRACT_ENDED_AT`

Responsavel sugerido:

- Airflow / orquestrador da ingestao

Metadados da carga `DS -> DW`:

- `LAST_BI_UPDATED_AT`
- `LAST_SUCCESS_BATCH_ID`
- `LAST_LOAD_MODE`
- `LAST_RUN_BATCH_ID`
- `LAST_RUN_STARTED_AT`
- `LAST_RUN_COMMITTED_AT`
- `UPDATED_AT`

Responsavel:

- dbt

## Regra de watermark no dbt

Os modelos dimensionais usam:

- `BI_UPDATED_AT` vindo do staging `DS -> DW`

Regra:

- execucao incremental normal:
  - processa registros com `BI_UPDATED_AT > LAST_BI_UPDATED_AT`
- reprocessamento por data:
  - processa registros com `BI_UPDATED_AT >= <data informada>`
- full-refresh:
  - ignora watermark e reconstrói a tabela

Ao final da execucao com sucesso, o modelo atualiza a `CTL_PIPELINE_WATERMARK` com:

- `LAST_BI_UPDATED_AT`
- `LAST_SUCCESS_BATCH_ID`
- `LAST_LOAD_MODE`
- `LAST_RUN_BATCH_ID`
- `LAST_RUN_STARTED_AT`
- `LAST_RUN_COMMITTED_AT`
- `UPDATED_AT`

## Regra de extracao upstream

O Airbyte nao precisa escrever diretamente na `CTL_PIPELINE_WATERMARK`.

O desenho recomendado e:

1. o Airflow dispara a extracao
2. o Airflow grava `LAST_EXTRACT_STARTED_AT`
3. o Airflow aguarda a conclusao da extracao
4. o Airflow grava `LAST_EXTRACT_ENDED_AT`
5. depois o dbt executa a carga `DS -> DW`

Scripts padrao criados no repositorio:

- [merge_ctl_pipeline_watermark_extract_start.sql](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi_v2/sql/control/merge_ctl_pipeline_watermark_extract_start.sql)
- [merge_ctl_pipeline_watermark_extract_end.sql](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi_v2/sql/control/merge_ctl_pipeline_watermark_extract_end.sql)

Uso:

- substituir `<PIPELINE_NAME>`

Exemplos:

- pipeline global `sx_estado`:
  - `PIPELINE_NAME = 'dim_sx_estado_d'`
- pipeline global `sx_equipamento`:
  - `PIPELINE_NAME = 'dim_sx_equipamento_d'`

## Pre-requisito

Se a tabela de controle ainda estiver com o nome antigo da coluna, alinhar antes:

```sql
alter table SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
rename column LAST_SOURCE_UPDATED_AT to LAST_BI_UPDATED_AT;
```

## Cenarios de execucao

### 1. Incremental normal

Usa o watermark salvo na `CTL_PIPELINE_WATERMARK`.

Comando:

```powershell
docker compose -f docker-compose.local.yml exec airflow-worker-dbt bash -lc "cd /opt/airflow/project/dbt/solix_dbt && dbt run --select dim_sx_estado_d"
```

Comportamento:

- le `LAST_BI_UPDATED_AT` do pipeline `dim_sx_estado_d`
- processa apenas registros novos/alterados no `DW`
- atualiza o watermark no final

### 2. Reprocessamento por data

Usa uma data informada manualmente, ignorando temporariamente o watermark salvo.

Comando:

```powershell
docker compose -f docker-compose.local.yml exec airflow-worker-dbt bash -lc "cd /opt/airflow/project/dbt/solix_dbt && dbt run --select dim_sx_estado_d --vars '{\"dim_sx_estado_d_reprocess_from\": \"2026-04-01 00:00:00\"}'"
```

Comportamento:

- reprocessa tudo com `BI_UPDATED_AT >= data informada`
- sobrescreve o watermark com o novo maximo processado ao final

Uso recomendado:

- correcao localizada
- reprocessamento apos incidente
- reaproveitamento de uma janela especifica

### 3. Full-refresh

Reconstrói a dimensao inteira.

Comando:

```powershell
docker compose -f docker-compose.local.yml exec airflow-worker-dbt bash -lc "cd /opt/airflow/project/dbt/solix_dbt && dbt run --select dim_sx_estado_d --full-refresh"
```

Comportamento:

- ignora o watermark
- recria a tabela da dimensao
- grava um novo watermark ao final

Uso recomendado:

- mudanca estrutural relevante
- reconstrucao total da dimensao
- ajuste de regra de negocio que exige recomputo completo

## Reset controlado da tabela de controle

Se for necessario resetar manualmente o watermark do pipeline:

```sql
update SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
set LAST_BI_UPDATED_AT = null,
    LAST_SUCCESS_BATCH_ID = null,
    LAST_LOAD_MODE = 'MANUAL_RESET',
    LAST_EXTRACT_STARTED_AT = null,
    LAST_EXTRACT_ENDED_AT = null,
    LAST_RUN_BATCH_ID = null,
    LAST_RUN_STARTED_AT = null,
    LAST_RUN_COMMITTED_AT = null,
    UPDATED_AT = convert_timezone('UTC', current_timestamp())::timestamp_ntz
where PIPELINE_NAME = 'dim_sx_estado_d'
;
```

Depois disso, a proxima execucao incremental processara tudo que existir no `DS` para o pipeline global da dimensao.

## Consultas de validacao

Consultar watermark atual:

```sql
select *
from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
where PIPELINE_NAME = 'dim_sx_estado_d'
;
```

Consultar registros do DW:

```sql
select *
from SOLIX_BI.DW.SX_ESTADO_D
order by BI_UPDATED_AT desc;
```

Consultar base de origem para o DW:

```sql
select *
from SOLIX_BI.DW.STG_DS__SX_ESTADO_D
order by BI_UPDATED_AT desc;
```

## Recomendacao operacional

Usar o seguinte padrao:

- execucao diaria/recorrente:
  - incremental normal
- incidente localizado:
  - reprocessamento por data
- mudanca estrutural ou correcao ampla:
  - full-refresh

Esse e o padrao recomendado para o fluxo atual com Airbyte na ingestao e dbt controlando a transformacao `DS -> DW`.
