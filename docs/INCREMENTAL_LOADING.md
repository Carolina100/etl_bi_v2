# Carga Incremental por Watermark

O fluxo de `SX_ESTADO_D` passou a operar em dois modos:

- `INCREMENTAL_WATERMARK`
- `MANUAL_BACKFILL`

## Comportamento padrão

Se a DAG ou a pipeline forem executadas sem `data_inicio` e `data_fim`, a carga roda em modo incremental.

Nesse modo:

1. a pipeline lê o último `LAST_SOURCE_UPDATED_AT` salvo em `SOLIX_BI.DS.CTL_PIPELINE_WATERMARK`
2. aplica um lookback técnico de 10 minutos
3. consulta no Oracle apenas registros com `UPDATED_ON` dentro da janela efetiva
4. faz `MERGE` no DS por chave natural
5. atualiza o watermark somente se a execução terminar com sucesso

## Backfill manual

Se `data_inicio` e `data_fim` forem informadas juntas, a pipeline ignora o watermark e executa um reprocessamento manual por janela.

Esse modo é o trilho operacional para:

- correção de dados
- falha na coluna `UPDATED_ON`
- reprocessamento histórico
- validação controlada por período

## Estratégia de carga no DS

A camada DS deixou de funcionar como `DELETE + COPY` por cliente e passou a funcionar como `PUT + COPY para tabela temporária + MERGE`.

Para `SX_ESTADO_D`, a chave natural usada no merge é:

- `ID_CLIENTE`
- `CD_ESTADO`

## Colunas técnicas no BI

Na tabela DS, a recomendação adotada passou a ser:

- `BI_CREATED_AT`
  - quando o registro entrou pela primeira vez no BI
- `BI_UPDATED_AT`
  - quando o registro foi atualizado pela última vez no BI

Regra aplicada no `MERGE`:

- insert novo:
  - `BI_CREATED_AT = agora`
  - `BI_UPDATED_AT = agora`
- update:
  - preserva `BI_CREATED_AT`
  - atualiza `BI_UPDATED_AT`

## Incremental no DW

As dimensoes no dbt tambem passam a trabalhar de forma incremental:

- na primeira execucao, o modelo materializa todo o conjunto e inclui o registro orfao
- nas execucoes seguintes, o `staged_source` filtra apenas linhas com `BI_UPDATED_AT`
  maior que o maximo ja existente no DW
- o `merge` do dbt atualiza ou insere apenas o delta da camada DS

Isso reduz:

- volume processado no DW
- tempo de execucao do `dbt`
- quantidade de linhas afetadas na auditoria

## Tabela de controle necessária

O controle incremental depende desta tabela no Snowflake:

```sql
create table if not exists SOLIX_BI.DS.CTL_PIPELINE_WATERMARK (
    PIPELINE_NAME varchar not null,
    ID_CLIENTE number(38, 0) not null,
    LAST_SOURCE_UPDATED_AT timestamp_ntz,
    LAST_SUCCESS_BATCH_ID varchar,
    LAST_LOAD_MODE varchar,
    LAST_EXTRACT_STARTED_AT timestamp_ntz,
    LAST_EXTRACT_ENDED_AT timestamp_ntz,
    UPDATED_AT timestamp_ntz,
    constraint PK_CTL_PIPELINE_WATERMARK primary key (PIPELINE_NAME, ID_CLIENTE)
);
```

Script no repositório:

- [create_ctl_pipeline_watermark.sql](/c:/Users/CarolinaIovanceGolfi/Desktop/etl_bi/sql/control/create_ctl_pipeline_watermark.sql)

Se a tabela `SOLIX_BI.DS.SX_ESTADO_D` ja existir, ela precisa conter tambem:

```sql
alter table SOLIX_BI.DS.SX_ESTADO_D add column if not exists BI_CREATED_AT timestamp_ntz;
alter table SOLIX_BI.DS.SX_ESTADO_D add column if not exists BI_UPDATED_AT timestamp_ntz;
alter table SOLIX_BI.DS.SX_ESTADO_D drop column if exists ETL_LOADED_AT;
```

## Como a janela é resolvida

### Incremental

- início:
  - `LAST_SOURCE_UPDATED_AT - 10 minutos`
  - se não existir watermark, começa em `1900-01-01 00:00:00`
- fim:
  - timestamp atual da execução
  - esse valor nao vem do Airflow, do Oracle ou do Snowflake
  - ele e obtido pelo Python no momento em que a pipeline roda
  - no codigo, isso acontece com `datetime.now(BRAZIL_TIMEZONE)`
  - depois esse valor vira o parametro `updated_on_end` enviado para a query Oracle

### Manual

- início:
  - `data_inicio 00:00:00`
- fim:
  - dia seguinte a `data_fim`, às `00:00:00`

Observação:
- a query usa intervalo semiaberto: `>= início` e `< fim`

## Query Oracle

O padrão novo de extração ficou assim:

```sql
WHERE e.UPDATED_ON >= TO_TIMESTAMP(:updated_on_start, 'DD/MM/YYYY HH24:MI:SS')
  AND e.UPDATED_ON < TO_TIMESTAMP(:updated_on_end, 'DD/MM/YYYY HH24:MI:SS')
```

## Padrão `base -> ranked -> select final`

O projeto passa a adotar como padrão estrutural nas queries Oracle da camada DS:

1. `base`
   - monta o dataset bruto da entidade
   - aplica joins, derivacoes e o `SOURCE_UPDATED_ON`
2. `ranked`
   - garante uma unica linha por chave natural
   - usa `ROW_NUMBER()` com `PARTITION BY` na chave natural
   - prioriza a linha mais recente por `SOURCE_UPDATED_ON DESC`
3. `select final`
   - aplica o filtro incremental da janela
   - filtra `RN = 1`
   - entrega apenas as colunas finais da carga

Por que isso existe:

- protege o `MERGE` no Snowflake contra duplicidade na origem
- evita que joins futuros quebrem a unicidade da chave natural
- padroniza a leitura das pipelines entre entidades simples e complexas
- reduz o risco de comportamento inconsistente quando a origem mudar no futuro

Mesmo em entidades simples, como `SX_ESTADO_D`, esse padrão pode ser mantido por consistencia arquitetural.

## Como usar

### Pipeline local incremental

```powershell
python src/pipelines/load_sx_estado_d.py --id_cliente 7
```

### Pipeline local com backfill

```powershell
python src/pipelines/load_sx_estado_d.py --id_cliente 7 --data_inicio 2026-03-01 --data_fim 2026-03-31
```

### Airflow incremental

No trigger da DAG, informe apenas:

```json
{
  "id_cliente": 7
}
```

### Airflow com backfill

```json
{
  "id_cliente": 7,
  "data_inicio": "2026-03-01",
  "data_fim": "2026-03-31"
}
```

## Auditoria

O modo de execução passa a aparecer nos detalhes dos eventos:

- `load_mode=INCREMENTAL_WATERMARK`
- `load_mode=MANUAL_BACKFILL`

Os resultados da DS também retornam:

- `window_start`
- `window_end`
- `max_source_updated_on`

Isso permite que o DW registre a janela real processada, mesmo quando cada cliente usa um watermark diferente.

Observacao de timezone:

- `LAST_SOURCE_UPDATED_AT` nao e convertido para horario de Brasilia
- ele e persistido como valor fiel retornado pela origem Oracle
- isso e importante porque esse campo dirige a logica incremental e precisa permanecer no mesmo referencial de tempo da origem
- `UPDATED_AT` em `CTL_PIPELINE_WATERMARK` e gravado em horario de Brasilia
- isso e feito no Snowflake com `CONVERT_TIMEZONE('America/Sao_Paulo', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ`
