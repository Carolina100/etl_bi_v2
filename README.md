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
  scripts/
  src/
```

## Padrão do projeto

- `src/pipelines/`: pipelines Python da camada DS
- `dags/`: DAGs Airflow
- `dbt/solix_dbt/models/staging/`: staging a partir do DS
- `dbt/solix_dbt/models/marts/dimensions/`: dimensoes DW
- `src/audit/`: auditoria DS e DW

## Execução local

### DS

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

- subir Airflow local em `C:\airflow-local`
- disparar a DAG `load_sx_estado_d_dag`

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
- subir em imagem customizada do Airflow
- usar variaveis de ambiente em vez de credenciais fixas

## Próximos passos naturais

- agendamento produtivo das DAGs
- politica de retencao de auditoria
- CI/CD para imagem do Airflow
- separacao de ambientes dev/hml/prd
