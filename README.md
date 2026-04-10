# etl_bi

Projeto de ingestao e transformacao de dados com o fluxo:

- Oracle / PostgreSQL -> Airbyte -> Snowflake DS
- Snowflake DS -> dbt -> Snowflake DW
- Airflow orquestrando dbt localmente em Docker

## Estrutura principal

```text
etl_bi/
  dags/
  dbt/solix_dbt/
  docs/
  infra/airflow/
  sql/
  scripts/
  src/
```

## Padrão do projeto

- `dags/`: DAGs Airflow
- `dbt/solix_dbt/models/staging/`: staging a partir do DS
- `dbt/solix_dbt/models/marts/dimensions/`: dimensoes DW
- `src/utils/`: helpers Airflow e dbt
- `sql/control/`: scripts SQL de tabelas de controle
- `sql/ds/`: scripts SQL da camada DS por entidade
- `sql/dw/`: scripts SQL da camada DW por entidade

Modelo de referência para novas entidades:

- use o fluxo completo de dbt como base funcional
- guia de orquestração:
  - `docs/AIRFLOW_DAGS.md`
- guia de ingestão local:
  - `docs/AIRBYTE.md`

## Arquitetura de execução

- `dbt/solix_dbt/`: runtime dbt da camada DW
- `dags/`: orquestração Airflow
- `docker-compose.local.yml`: stack local de Airflow/dbt, Postgres e Redis
- `infra/airflow/`: imagem base do Airflow para scheduler/webserver/triggerer
- `infra/runtime/dbt/`: imagem do worker DW/dbt
- `.env.local` / `.env.docker`: configuracoes separadas para Windows local e Docker local

## Execução local

### Ingestão local via Airbyte

1. instalar e subir o Airbyte localmente via `abctl`
2. configurar Airbyte no UI em `http://localhost:8000`
3. criar conectores de origem para Oracle e PostgreSQL
4. escrever os dados no Snowflake DS apropriado
5. opcionalmente registrar o `connection_id` no trigger da DAG para o Airflow disparar a sync

### DW

```powershell
. .\scripts\load_local_env.ps1
cd dbt\solix_dbt
dbt build --select .
```

### Fluxo completo via Airflow

- copiar `.env.docker.example` para `.env.docker`
- preparar `.env.docker` e a chave em `secrets/snowflake/ETL_KEYPAIR.p8`
- subir o Airbyte localmente via `abctl`
- subir a stack do Airflow com `docker compose -f docker-compose.local.yml up --build -d`
- acessar Airflow em `http://localhost:8080`
- usar `load_dw_dbt_dag` para execucao manual sob demanda
- usar `schedule_sx_estado_d_incremental_dag` para o disparo frequente
- usar `schedule_sx_estado_d_full_dag` para a reconciliacao full diaria

### Fluxo local com Airbyte

- subir o Airbyte localmente via `abctl`
- abrir o Airbyte UI em `http://localhost:8000`
- configurar conectores Oracle e PostgreSQL para Snowflake DS
- usar o mesmo destino Snowflake DS que o dbt consome
- o código Python de ingestão foi removido nesta versão para privilegiar Airbyte

### Estratégia incremental

- padrão: configurado por stream no Airbyte
- transformação incremental: implementada no `dbt` a partir do `DS`
- para `SX_ESTADO_D`, usar modo hibrido:
  - sync incremental no dia a dia
  - reconciliacao full periodica para permitir `FG_ATIVO = 0` em ausentes
- documentação detalhada:
  - `docs/INCREMENTAL_LOADING.md`

### Filas e workers

- tasks da DAG atual rodam na fila `dbt`
- scheduler e webserver nao precisam carregar dependencias pesadas de dbt
- a extracao DS fica no Airbyte, fora deste runtime Python

## GitHub

O projeto ja esta preparado para publicacao em GitHub com separacao entre remoto pessoal e remoto corporativo:

- `origin`: repositorio pessoal
- `company`: repositorio da empresa

Arquivo de apoio:

- `docs/GITHUB_SETUP.md`
- `scripts/setup_git_remotes.ps1`

## Versionamento recomendado

Use versionamento semantico:

- `0.x.y`: fase de POC / estabilizacao
- `1.0.0`: primeira versao pronta para producao

Sugestao de tags:

- `v0.1.0`: primeira versao funcional DS + DW
- `v0.2.0`: auditoria DW
- `v0.3.0`: templates e padronizacao

## Empacotamento para produção

O projeto ja esta preparado para:

- virar pacote Python via `pyproject.toml`
- subir em imagem base de Airflow separada dos workers especializados
- usar worker especializado para a orquestracao `dbt` e integracao leve com Airbyte API
- usar variaveis de ambiente e secret files em vez de credenciais fixas

## Próximos passos naturais

- agendamento produtivo das DAGs
- politica de retencao de auditoria
- CI/CD para imagem do Airflow
- separacao de ambientes dev/hml/prd

## Ajustes de schema do DS

Para `SX_ESTADO_D`, o padrão técnico atual no BI considera:

- `BI_CREATED_AT`
- `BI_UPDATED_AT`

A coluna `ETL_LOADED_AT` deixou de ser usada nesse fluxo.
