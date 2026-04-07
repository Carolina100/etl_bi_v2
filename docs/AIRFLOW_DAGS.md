# DAGs do Airflow

Este documento explica, de forma didática, o que cada DAG do projeto faz e como o fluxo foi desenhado.

## Objetivo das DAGs

Cada DAG representa o fluxo completo de uma entidade no caminho:

- Oracle -> Python -> Snowflake DS
- Snowflake DS -> dbt -> Snowflake DW

Hoje existem estas DAGs:

- [load_sx_estado_d_dag.py](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dags/load_sx_estado_d_dag.py)
- [load_sx_operacao_d_dag.py](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dags/load_sx_operacao_d_dag.py)
- [load_sx_fazenda_d_dag.py](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dags/load_sx_fazenda_d_dag.py)
- [load_sx_equipamento_d_dag.py](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/dags/load_sx_equipamento_d_dag.py)

## O que todas as DAGs fazem

Todas seguem a mesma lógica:

1. validam os parâmetros da execução
2. resolvem quais clientes serão processados
3. montam uma requisição DS por cliente
4. executam a camada DS em paralelo por cliente
5. consolidam os resultados do DS
6. registram quais clientes falharam
7. executam o dbt uma única vez no final
8. registram auditoria do DW

## Explicando cada etapa

### 1. `validate_and_prepare_params`

Esta task:

- lê `dag_run.conf` ou os `params` da DAG
- aceita:
  - `id_cliente`
  - `id_clientes`
  - `data_inicio`
  - `data_fim`
- identifica o modo de carga:
  - `INCREMENTAL_WATERMARK`
  - `MANUAL_BACKFILL`

Se nenhuma data for informada, o padrão é incremental.

### 2. `resolve_clientes`

Esta task decide quais clientes serão executados.

Regras:

- se `id_cliente` ou `id_clientes` vier manualmente:
  - usa exatamente os clientes informados
- se nenhum cliente vier:
  - consulta `SOLIX_BI.DS.SX_CLIENTE_D`
  - busca todos os clientes ativos (`FL_ATIVO = TRUE`)

Objetivo:

- evitar cadastrar clientes manualmente em cada DAG
- permitir execução manual quando necessário

## 3. `build_ds_requests`

Transforma a lista de clientes em uma lista de requisições da camada DS.

Exemplo:

```python
[
  {"id_cliente": 7, "data_inicio": None, "data_fim": None},
  {"id_cliente": 8, "data_inicio": None, "data_fim": None},
]
```

Cada item dessa lista alimenta uma execução paralela da pipeline Python.

### 4. `run_ds_pipeline_task`

Executa a pipeline Python da entidade para um cliente por vez.

Exemplo:

- `load_sx_estado_d.py`
- `load_sx_operacao_d.py`
- `load_sx_fazenda_d.py`
- `load_sx_equipamento_d.py`

O que ela faz por cliente:

- lê o watermark daquele cliente
- extrai o delta no Oracle
- gera CSV temporário
- envia para o stage do Snowflake
- aplica `MERGE` na tabela DS
- atualiza watermark
- registra auditoria

### 5. `summarize_ds_results`

Depois que todas as execuções DS terminam, essa task consolida:

- clientes com sucesso
- clientes com falha
- resultados válidos para o DW

Ela produz algo como:

```python
{
  "successful_clientes": [7, 8],
  "failed_clientes": [9],
  "successful_results": [...],
  "failed_results": [...]
}
```

### 6. `report_failed_clients`

Esta task não interrompe o fluxo.

Ela serve para:

- deixar claro no log do Airflow quais clientes falharam
- facilitar acompanhamento operacional

Além disso, os clientes com falha também são gravados na auditoria.

### 7. `run_dw_dbt`

Executa o `dbt build` uma vez só no final.

Isso é importante porque:

- o DS é incremental por cliente
- o DW precisa rodar consolidado
- evita executar dbt uma vez por cliente
- reduz chamadas desnecessárias ao Snowflake

O DW roda:

- se pelo menos um cliente tiver sucesso no DS
- com os dados atualizados dos clientes bem-sucedidos

O DW não roda:

- se todos os clientes falharem no DS

## Por que o DW roda só uma vez no final

O filtro incremental das dimensões do DW usa:

```sql
BI_UPDATED_AT > max(BI_UPDATED_AT)
```

Esse corte é global na tabela do DW, não por cliente.

Por isso, o desenho mais seguro é:

- DS em paralelo por cliente
- DW único no final

Assim:

- clientes bem-sucedidos entram no DW
- clientes que falharam ficam para a próxima execução
- o incremental do DW continua consistente

## O que acontece se um cliente falhar

Hoje o comportamento é:

- o cliente que falhou fica fora da atualização do DW naquela execução
- os clientes que tiveram sucesso continuam para o DW
- a falha fica registrada:
  - no log do Airflow
  - na `CTL_LOAD_AUDIT`

Isso evita que um único cliente bloqueie todos os outros.

## Auditoria gravada pelas DAGs

As DAGs usam as tabelas atuais:

- `SOLIX_BI.DS.CTL_BATCH_EXECUTION`
- `SOLIX_BI.DS.CTL_LOAD_AUDIT`

Eventos importantes gravados no `DW`:

- `DS_SUCCESSFUL_CLIENTS`
- `DS_FAILED_CLIENTS`
- `DBT_BUILD`
- `DBT_MODEL_*`
- `DBT_ERROR`, se houver falha

## Execução manual

Você pode executar manualmente de duas formas.

### Um cliente

```json
{
  "id_cliente": 7
}
```

### Vários clientes

```json
{
  "id_clientes": [7, 8, 9]
}
```

### Backfill manual

```json
{
  "id_cliente": 7,
  "data_inicio": "2026-01-01",
  "data_fim": "2026-01-31"
}
```

### Execução automática

Se nenhum cliente for informado:

- a DAG busca todos os clientes ativos na `SX_CLIENTE_D`

## Filas

As DAGs usam duas filas:

- `ds`
  - tasks Python de ingestão Oracle -> DS
- `dbt`
  - task única do DW

Isso ajuda a separar:

- dependências Oracle/Python
- dependências dbt/Snowflake

## Resumo mental

Pense assim:

- DS = processamento por cliente
- DW = consolidação final

Ou seja:

- cada cliente atualiza sua própria parte da DS
- depois o dbt lê a DS consolidada e atualiza o DW uma única vez

Esse é o motivo principal do desenho atual das DAGs.
