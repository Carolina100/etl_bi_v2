# Operacao de Watermark no DW

Este documento descreve como operar a tabela `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK` no fluxo atual de `sx_equipamento_d`.

## Escopo atual

O pipeline operacional ativo neste repositorio e:

- `PIPELINE_NAME = 'dim_sx_equipamento_d'`

O uso atual da tabela e:

- registrar `LAST_EXTRACT_STARTED_AT`
- registrar `LAST_EXTRACT_ENDED_AT`
- registrar o ultimo `LAST_BI_UPDATED_AT` consolidado no `DW`
- registrar o ultimo batch executado com sucesso
- registrar o status macro da ultima execucao
- registrar a ultima mensagem de erro, quando houver

## Tabela de controle

Tabela:

- `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK`

Chave:

- `PIPELINE_NAME`

Coluna principal de incremental no `DS -> DW`:

- `LAST_BI_UPDATED_AT`

Metadados de extracao:

- `LAST_EXTRACT_STARTED_AT`
- `LAST_EXTRACT_ENDED_AT`

Metadados da carga `DS -> DW`:

- `LAST_SUCCESS_BATCH_ID`
- `LAST_LOAD_MODE`
- `LAST_RUN_BATCH_ID`
- `LAST_RUN_STATUS`
- `LAST_ERROR_MESSAGE`
- `LAST_RUN_STARTED_AT`
- `LAST_RUN_COMMITTED_AT`
- `UPDATED_AT`

## Responsabilidade por atualizacao

- Airflow / orquestracao:
  - `LAST_EXTRACT_STARTED_AT`
  - `LAST_EXTRACT_ENDED_AT`

- dbt:
  - `LAST_BI_UPDATED_AT`
  - `LAST_SUCCESS_BATCH_ID`
  - `LAST_LOAD_MODE`
  - `LAST_RUN_BATCH_ID`
  - `LAST_RUN_STATUS`
  - `LAST_ERROR_MESSAGE`
  - `LAST_RUN_STARTED_AT`
  - `LAST_RUN_COMMITTED_AT`
  - `UPDATED_AT`

## Regra de watermark no dbt

O modelo `dim_sx_equipamento_d` usa:

- `BI_UPDATED_AT` vindo do `stg_ds__sx_equipamento_d`

Regra:

- execucao incremental normal:
  - processa registros com `BI_UPDATED_AT > LAST_BI_UPDATED_AT`

Ao final da execucao com sucesso, o modelo atualiza:

- `LAST_BI_UPDATED_AT`
- `LAST_SUCCESS_BATCH_ID`
- `LAST_LOAD_MODE`
- `LAST_RUN_BATCH_ID`
- `LAST_RUN_STATUS`
- `LAST_ERROR_MESSAGE`
- `LAST_RUN_STARTED_AT`
- `LAST_RUN_COMMITTED_AT`
- `UPDATED_AT`

## Regra de extracao upstream

O Airflow registra:

1. `LAST_EXTRACT_STARTED_AT`
2. executa a sync do Airbyte
3. `LAST_EXTRACT_ENDED_AT`

No inicio da extracao, a tabela passa a refletir:

- `LAST_RUN_STATUS = 'RUNNING'`
- `LAST_ERROR_MESSAGE = null`

Esse metadado:

- nao substitui o cursor do Airbyte
- serve para rastreabilidade operacional

## Persistencia

A `CTL_PIPELINE_WATERMARK` deve ser tratada como tabela de controle persistente.

- nao deve ser `TRANSIENT`
- nao deve entrar em politica de cleanup tecnico curta

## Fluxo operacional atual

1. `load_ds_airbyte_dimensions_dag`
   - grava `LAST_EXTRACT_STARTED_AT`
   - executa Airbyte
   - grava `LAST_EXTRACT_ENDED_AT`
2. `load_dw_dbt_dimensions_dag`
   - roda `dbt build` para `ds_sx_equipamento_d`, `stg_ds__sx_equipamento_d` e `dim_sx_equipamento_d`
3. `orchestrate_ds_dw_dimensions_dag`
   - encadeia os dois passos acima

## Consultas de validacao

Consultar watermark atual:

```sql
select *
from SOLIX_BI.DS.CTL_PIPELINE_WATERMARK
where PIPELINE_NAME = 'dim_sx_equipamento_d'
;
```

Consultar dimensao final:

```sql
select *
from SOLIX_BI.DW.SX_EQUIPAMENTO_D
order by BI_UPDATED_AT desc
;
```

Consultar staging do DW:

```sql
select *
from SOLIX_BI.DW.STG_DS__SX_EQUIPAMENTO_D
order by BI_UPDATED_AT desc
;
```

## Recomendacao operacional

No desenho atual:

- usar apenas incremental normal no `DW`
- nao usar `full-refresh`
- nao usar reprocessamento por data como parte da rotina operacional

Esse e o padrao atual para manter a trilha alinhada com a operacao de `sx_equipamento_d`.
