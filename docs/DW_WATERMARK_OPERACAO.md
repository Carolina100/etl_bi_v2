# Operacao de Watermark no DW

Este documento descreve como operar o fluxo `DS -> DW` usando watermark na tabela `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK`.

O piloto atual e a dimensao `dim_sx_estado_d`.

## Objetivo

Usar a tabela de controle para:

- executar incremental normal sem lookback arbitrario
- permitir reprocessamento parcial por data
- permitir reconstrucao total com `full-refresh`
- registrar o ultimo watermark processado com sucesso

## Tabela de controle

Tabela:

- `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK`

Coluna principal de watermark no fluxo `DS -> DW`:

- `LAST_BI_UPDATED_AT`

Para a dimensao `SX_ESTADO_D`, o pipeline registrado e:

- `PIPELINE_NAME = 'dim_sx_estado_d'`

## Regra de watermark

O modelo `dim_sx_estado_d` usa:

- `BI_UPDATED_AT` vindo da `stg_ds__sx_estado_d`

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
    LAST_RUN_BATCH_ID = null,
    LAST_RUN_STARTED_AT = null,
    LAST_RUN_COMMITTED_AT = null,
    UPDATED_AT = convert_timezone('UTC', current_timestamp())::timestamp_ntz
where PIPELINE_NAME = 'dim_sx_estado_d'
  and ID_CLIENTE = 0;
```

Depois disso, a proxima execucao incremental processara tudo que existir no `DS` para o pipeline global da dimensao.

## Consultas de validacao

Consultar watermark atual:

```sql
select *
from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
where PIPELINE_NAME = 'dim_sx_estado_d'
  and ID_CLIENTE = 0;
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
