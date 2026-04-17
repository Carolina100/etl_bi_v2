# Fluxo Fim a Fim `sx_equipamento`

Este documento descreve o fluxo completo do equipamento, desde a extracao no Airbyte Cloud ate a materializacao final no `DW` do Snowflake.

O objetivo e deixar claro:

- em que camada cada tabela existe
- quando o processamento e global
- onde acontece `merge`
- como o incremental funciona
- como `FG_ATIVO` e tratado
- como a retencao tecnica e aplicada

---

## 1. Visao Geral

O fluxo atual do equipamento e:

1. Airbyte extrai da origem e grava no `RAW`
2. dbt consolida o `RAW` em `DS`
3. dbt padroniza o `DS` em `STG`
4. dbt materializa a dimensao final no `DW`
5. o pipeline atualiza a `CTL_PIPELINE_WATERMARK`
6. o cleanup do `RAW` pode rodar ao final da execucao com sucesso
7. uma DAG separada pode limpar `DS` por retenção tecnica

Desenho atual:

- `Airbyte -> RAW`: global
- `RAW -> DS`: global
- `DS -> DW`: global

Ou seja:

- nao fazemos o fluxo por cliente no processamento operacional atual
- o equipamento segue a semantica global do legado
- `ID_CLIENTE` continua existindo como atributo do dado e como parte da chave natural, mas nao como particionamento da execucao

---

## 2. Camadas

### 2.1. `RAW`

Objetivo:

- armazenar o dado bruto trazido pelo Airbyte
- servir como entrada tecnica para o `DS`

Caracteristicas:

- carga feita pelo Airbyte Cloud
- tabelas de aterrissagem devem ser `TRANSIENT` no Snowflake
- granularidade bruta da origem
- o cursor incremental continua sendo controlado pelo Airbyte
- nao usamos `TEMPORARY TABLE`
- nao usamos `truncate`
- o `RAW` e limpo ao final da execucao bem-sucedida da entidade

Tabelas principais do equipamento no `RAW`:

- `SOLIX_BI.RAW.CDT_EQUIPAMENTO`
- `SOLIX_BI.RAW.CDT_MODELO_EQUIPAMENTO`
- `SOLIX_BI.RAW.CDT_TIPO_EQUIPAMENTO`
- `SOLIX_BI.RAW.CDT_EQUIPAMENTO_HISTORICO_MOV`

Comportamento:

- o Airbyte roda globalmente para a entidade
- o pipeline registra metadados de extracao em `CTL_PIPELINE_WATERMARK`
- a limpeza do `RAW` e feita ao final da execucao bem-sucedida da entidade

Decisao de arquitetura:

- o `RAW` e uma camada tecnica efemera, mas estavel o suficiente para a execucao do Airbyte e do dbt
- por isso usamos `TRANSIENT TABLE` com cleanup, e nao `TEMPORARY TABLE`
- o historico nao fica no `RAW`; a persistencia analitica fica no `DW`

---

### 2.2. `DS`

Objetivo:

- construir uma camada operacional consolidada
- manter o `current-state` da entidade equipamento
- servir de base para o `DW`

Tabela principal:

- `SOLIX_BI.DS.SX_EQUIPAMENTO_D`

Caracteristicas:

- materializacao incremental por `merge`
- uma linha por chave natural:
  - `ID_CLIENTE`
  - `CD_EQUIPAMENTO`
- `FG_ATIVO` e booleano
- `FG_ATIVO` e tratado como atributo mutavel
- nao existe delete fisico de negocio no `DS`

Comportamento logico:

- se o registro vier ativo e nao existir:
  - insere
- se o registro vier ativo e ja existir:
  - atualiza
- se o registro vier inativo e ja existir:
  - atualiza a mesma linha para inativo
- se o registro vier inativo e nao existir:
  - ignora

Em outras palavras:

- `FG_ATIVO` nao faz parte da chave
- `FG_ATIVO` nao cria uma nova linha
- o `DS` e `current-state`, nao historico de status

---

### 2.3. `STG`

Objetivo:

- padronizar o `DS` para leitura no `DW`
- expor o dado pronto para a dimensao final

Tabela/modelo:

- `stg_ds__sx_equipamento_d`

Caracteristicas:

- materializado como `view`
- le do `source('ds', 'sx_equipamento_d')`
- faz cast de tipos
- preserva uma unica linha mais recente por chave natural

---

### 2.4. `DW`

Objetivo:

- disponibilizar a dimensao final para consumo analitico
- manter o estado atual confiavel do equipamento

Tabela/modelo:

- `SOLIX_BI.DW.SX_EQUIPAMENTO_D`

Caracteristicas:

- materializacao incremental por `merge`
- `unique_key`:
  - `ID_CLIENTE`
  - `CD_EQUIPAMENTO`
- preserva a `surrogate key` existente
- cria nova `surrogate key` so para novos registros
- nao faz delete fisico
- `FG_ATIVO` continua como atributo mutavel do current-state

---

## 3. Extracao no Airbyte

### DAG envolvida

- `load_ds_airbyte_dimensions_dag`

### O que ela faz

1. registra `LAST_EXTRACT_STARTED_AT`
2. dispara a sync do Airbyte Cloud
3. aguarda a conclusao
4. registra `LAST_EXTRACT_ENDED_AT`

### Como e a execucao

- global para o equipamento
- usa a `connection_id` do Airbyte Cloud
- a connection atual do equipamento esta configurada no scheduler incremental

### Metadado de extracao

A DAG atualiza:

- `CTL_PIPELINE_WATERMARK`

Campos de extracao:

- `LAST_EXTRACT_STARTED_AT`
- `LAST_EXTRACT_ENDED_AT`

Importante:

- isso nao controla o cursor do Airbyte
- o cursor incremental continua sendo nativo do Airbyte
- essa tabela guarda metadado operacional da execucao

---

## 4. Merge no `DS`

### Modelo

- `dbt/solix_dbt/models/staging/ds/ds_sx_equipamento_d.sql`

### Onde o `merge` acontece

O `merge` e feito pelo proprio dbt porque o modelo esta configurado como:

- `materialized='incremental'`
- `incremental_strategy='merge'`
- `unique_key=['ID_CLIENTE', 'CD_EQUIPAMENTO']`

### Como o modelo e construido

O modelo segue estes passos:

1. le a tabela dirigente `CDT_EQUIPAMENTO`
2. deduplica a tabela dirigente por:
   - `ID_CLIENTE`
   - `CD_EQUIPAMENTO`
   - ordenando pela data mais recente
3. le e deduplica as tabelas auxiliares:
   - modelo
   - tipo
   - historico de status
4. consolida tudo em uma entidade unica
5. compara com o estado atual da tabela `DS`
6. envia para o `merge` somente novos ou alterados

### O que determina alteracao

O `DS` considera mudanca quando variar pelo menos um destes grupos:

- atributos descritivos
- codigos relacionados
- `DESC_STATUS`
- `TP_USO_EQUIPAMENTO`
- `FG_ATIVO`
- `SOURCE_UPDATED_AT`
- `AIRBYTE_EXTRACTED_AT`

### Como `FG_ATIVO` funciona

`FG_ATIVO` vem da tabela dirigente e e a regra principal de current-state.

A regra operacional e:

- `fg_ativo = true` e `dt_updated` maior:
  - atualiza ou insere
- `fg_ativo = false` e `dt_updated` maior:
  - atualiza a mesma linha para inativo

### Como `DESC_STATUS` funciona

`DESC_STATUS` nao define se o registro esta ativo ou nao.

Ele e um atributo de negocio independente, derivado da logica propria do historico de movimento.

Ou seja:

- `FG_ATIVO` governa a logica do current-state
- `DESC_STATUS` apenas enriquece o registro

---

## 5. Merge no `DW`

### Modelo

- `dbt/solix_dbt/models/marts/dimensions/dim_sx_equipamento_d.sql`

### Onde o `merge` acontece

Assim como no `DS`, o `merge` e feito pelo dbt com:

- `materialized='incremental'`
- `incremental_strategy='merge'`
- `unique_key=['ID_CLIENTE', 'CD_EQUIPAMENTO']`

### Como a dimensao funciona

1. le o watermark global da `CTL_PIPELINE_WATERMARK`
2. filtra o `STG` por `BI_UPDATED_AT > LAST_BI_UPDATED_AT`
3. le o estado atual da propria dimensao
4. preserva `SK_EQUIPAMENTO` quando a linha ja existe
5. gera `SK_EQUIPAMENTO` nova quando a linha nao existe
6. faz `merge` current-state

### Importante

No `DW` do equipamento:

- nao fazemos delete fisico
- nao tratamos inativacao como linha nova
- `FG_ATIVO` e atualizado na mesma linha

Isso vale tanto para dimensao quanto para o padrao que sera reaproveitado nas fatos `current-state`.

---

## 6. Watermark

### Tabela

- `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK`

### Chave atual

- `PIPELINE_NAME`

Ou seja:

- o watermark do equipamento e global
- nao usamos mais `ID_CLIENTE` na tabela de watermark

### Como a dimensao usa o watermark

No inicio:

- le `LAST_BI_UPDATED_AT` do pipeline `dim_sx_equipamento_d`

No fim:

- atualiza:
  - `LAST_BI_UPDATED_AT`
  - `LAST_SUCCESS_BATCH_ID`
  - `LAST_LOAD_MODE`
  - `LAST_RUN_BATCH_ID`
  - `LAST_RUN_STARTED_AT`
  - `LAST_RUN_COMMITTED_AT`
  - `UPDATED_AT`

### Significado pratico

O incremental do `DW` funciona em cima do:

- maior `BI_UPDATED_AT` ja consolidado com sucesso para a dimensao

---

## 7. DAGs do equipamento

### 7.1. Operacao normal

DAG:

- `schedule_sx_equipamento_d_incremental_dag`

Fluxo:

1. chama `orchestrate_ds_dw_dimensions_dag`
2. `orchestrate_ds_dw_dimensions_dag` chama `load_ds_airbyte_dimensions_dag`
3. `load_ds_airbyte_dimensions_dag` executa a sync do Airbyte
4. `orchestrate_ds_dw_dimensions_dag` chama `load_dw_dbt_dimensions_dag`
5. `load_dw_dbt_dimensions_dag` roda:
   - `ds_sx_equipamento_d`
   - `stg_ds__sx_equipamento_d`
   - `dim_sx_equipamento_d`

### 7.2. Cleanup tecnico do `RAW`

Fluxo:

1. `orchestrate_ds_dw_dimensions_dag` termina Airbyte e dbt com sucesso
2. executa `cleanup_raw_after_success`
3. limpa as tabelas `RAW` do equipamento

Regras:

- roda apenas com `ALL_SUCCESS`
- usa pool proprio de manutencao
- o objetivo e deixar o `RAW` efemero por execucao da entidade

### 7.3. Cleanup tecnico do `DS`

DAG:

- `cleanup_dimensions_retention_dag`

Fluxo:

1. limpa `DS`

Com `schedule` diario proprio em `50 23 * * *`.

---

## 8. Retencao tecnica

### `RAW`

Regra atual:

- manter apenas durante a execucao da entidade

Implementacao fisica recomendada:

- tabelas `RAW` criadas como `TRANSIENT`
- cleanup por janela tecnica ao inves de `truncate`

Implementacao:

- cleanup ao final da execucao bem-sucedida
- deletes por tabela do equipamento

### `DS`

Regra atual:

- manter apenas o dia atual

Implementacao:

- cleanup por dia calendario sobre `BI_UPDATED_AT`

Exemplo:

```sql
delete from SOLIX_BI.DS.SX_EQUIPAMENTO_D
where cast(BI_UPDATED_AT as date) < current_date()
```

### `DW`

Regra:

- sem cleanup
- persistencia principal do dado analitico atual

---

## 9. Auditoria

### Tabelas usadas

- `CTL_LOAD_AUDIT`
- `CTL_BATCH_EXECUTION`

Na estrutura atual, a `CTL_LOAD_AUDIT` ficou:

- global por pipeline e por step
- sem `ID_CLIENTE`
- sem `DT_INICIO`
- sem `DT_FIM`
- com granularidade por evento de execucao

Na estrutura atual, a `CTL_BATCH_EXECUTION` ficou:

- global por pipeline
- sem `ID_CLIENTE`
- sem `DT_INICIO`
- sem `DT_FIM`

Ou seja, o controle macro da execucao usa:

- `STARTED_AT`
- `ENDED_AT`
- `STATUS`
- `ROWS_EXTRACTED`
- `ROWS_LOADED`
- `ERROR_MESSAGE`
- `DURATION_SECONDS`
- `ORCHESTRATION_TYPE`

### No Airbyte

A extracao registra:

- `REGISTER_EXTRACT_START`
- `AIRBYTE_SYNC`
- `REGISTER_EXTRACT_END`

### No dbt

A DAG `load_dw_dbt_dimensions_dag` registra:

- `DBT_RUN`

### Na orquestracao

A DAG `orchestrate_ds_dw_dimensions_dag` registra:

- batch start
- batch end

### Na retencao

A DAG `cleanup_dimensions_retention_dag` registra:

- auditoria por task em `CTL_LOAD_AUDIT`
- batch macro em `CTL_BATCH_EXECUTION`
- quantidade de linhas deletadas

O cleanup do `RAW` dentro da orquestracao tambem registra:

- auditoria em `CTL_LOAD_AUDIT`
- quantidade de linhas deletadas por tabela

---

## 10. Decisoes de arquitetura

### O que esta padronizado hoje

- fluxo global para o equipamento
- `merge current-state`
- `FG_ATIVO` como atributo mutavel
- sem delete fisico no `DW`
- `RAW` efemero por execucao
- `DS` com retencao curta de 1 dia
- `DW` como camada principal
- o dbt operacional do equipamento roda apenas incremental

### O que nao estamos fazendo

- nao estamos historizando status ativo/inativo no `DW`
- nao estamos usando `DW` por cliente
- nao estamos controlando cursor do Airbyte fora do proprio Airbyte
- nao estamos fazendo cleanup acoplado ao dbt

---

## 11. Resumo executivo

Hoje o equipamento funciona assim:

- o Airbyte extrai globalmente e grava no `RAW`
- o dbt consolida o bruto em um `DS current-state`
- o dbt materializa o `DW current-state`
- o incremental do `DW` usa watermark global baseado em `BI_UPDATED_AT`
- `FG_ATIVO` atualiza a mesma linha, sem criar nova versao e sem delete fisico
- o `RAW` e uma camada tecnica efemera por execucao
- o `DS` e uma camada tecnica com retencao curta de 1 dia
- o `DW` e a persistencia principal do dado analitico
