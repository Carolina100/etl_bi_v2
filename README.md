# etl_bi

Projeto de ingestao e transformacao de dados com o fluxo:

- Oracle -> Python -> Snowflake DS
- Snowflake DS -> dbt -> Snowflake DW
- Airflow orquestrando localmente em Docker

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

- `src/pipelines/`: pipelines Python da camada DS
- `dags/`: DAGs Airflow
- `dbt/solix_dbt/models/staging/`: staging a partir do DS
- `dbt/solix_dbt/models/marts/dimensions/`: dimensoes DW
- `src/audit/`: auditoria DS e DW
- `sql/control/`: scripts SQL de tabelas de controle
- `sql/ds/`: scripts SQL da camada DS por entidade
- `sql/dw/`: scripts SQL da camada DW por entidade

Modelo de referência para novas entidades:

- use o fluxo completo de `SX_ESTADO_D` como base funcional
- guia de replicação:
  - `docs/TEMPLATES.md`

## Arquitetura de execução

- `src/pipelines/`: runtime Python da camada DS
- `dbt/solix_dbt/`: runtime dbt da camada DW
- `dags/`: orquestração Airflow
- `docker-compose.local.yml`: stack local com Airflow + Postgres + Redis + workers separados
- `infra/airflow/`: imagem base do Airflow para scheduler/webserver/triggerer
- `infra/runtime/python/`: imagem do worker DS
- `infra/runtime/dbt/`: imagem do worker DW/dbt
- `.env.local` / `.env.docker`: configuracoes separadas para Windows local e Docker local

## Execução local

### DS

```powershell
python src/pipelines/load_sx_estado_d.py --id_cliente 7
```

Backfill manual por janela:

```powershell
python src/pipelines/load_sx_estado_d.py --id_cliente 7 --data_inicio 2018-01-01 --data_fim 2026-03-31
```

### DW

```powershell
. .\scripts\load_local_env.ps1
cd dbt\solix_dbt
dbt build --select stg_ds__sx_estado_d dim_sx_estado_d
```

### Fluxo completo via Airflow

- preparar `.env.docker` e a chave em `secrets/snowflake/ETL_KEYPAIR.p8`
- subir a stack local com `docker compose -f docker-compose.local.yml up --build`
- acessar Airflow em `http://localhost:8080`
- disparar a DAG `load_sx_estado_d_dag`

### Estratégia incremental

- padrão: `INCREMENTAL_WATERMARK`
- exceção operacional: `MANUAL_BACKFILL`
- documentação detalhada:
  - `docs/INCREMENTAL_LOADING.md`

### Filas e workers

- tasks DS rodam na fila `ds`
- tasks dbt/DW rodam na fila `dbt`
- scheduler e webserver nao precisam carregar dependencias pesadas de Oracle/dbt

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
- usar workers especializados por fila para DS e dbt
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
