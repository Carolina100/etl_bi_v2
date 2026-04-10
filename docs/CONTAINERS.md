# Containers e Execucao

## Objetivo da nova estrutura

Deixar a execucao local mais proxima do desenho de producao, separando:

- orquestracao Airflow
- ingestão via Airbyte
- runtime dbt/DW

## Desenho atual da stack local

Arquivo principal do projeto:

- [docker-compose.local.yml](../docker-compose.local.yml)

Servicos:

- `postgres`: metadados do Airflow
- `redis`: broker do Celery
- `airflow-apiserver`: interface e API do Airflow
- `airflow-scheduler`: scheduler da DAG
- `airflow-triggerer`: triggerer
- `airflow-worker-dbt`: worker dedicado a tasks da fila `dbt`

Servico externo esperado no ambiente local:

- Airbyte rodando localmente via `abctl`
- UI disponivel em `http://localhost:8000`
- API acessivel para o Airflow por `http://host.docker.internal:8000`

## Imagens

- [infra/airflow/Dockerfile](../infra/airflow/Dockerfile)
  Base do Airflow para webserver, scheduler e triggerer.

- [infra/runtime/dbt/Dockerfile](../infra/runtime/dbt/Dockerfile)
  Worker com dependencias de DW: dbt-snowflake e Snowflake connector.

## Como as tasks sao roteadas

Na DAG, as tasks usam filas:

- `dbt` para orquestracao da sync Airbyte via API e execucao da camada DW/dbt

Isso permite que cada worker carregue apenas o que precisa.

## Segredos e key pair no container

O caminho da chave privada no seu Windows nao funciona dentro de container Linux.
Por isso, para a stack Docker local, o caminho usado no runtime precisa ser Linux, por exemplo:

- `/opt/airflow/project/secrets/snowflake/ETL_KEYPAIR.p8`

Recomendacao:

1. criar a pasta `secrets/snowflake/` fora do Git
2. copiar a chave `.p8` para la
3. garantir que `.gitignore` continue protegendo essa pasta

## Arquivos de ambiente

Para evitar trocar caminhos manualmente entre Windows e Docker:

- use `.env.local` para execucao local fora de container
- use `.env.docker` para a stack Docker local

Arquivos de exemplo:

- [.env.local.example](../.env.local.example)
- [.env.docker.example](../.env.docker.example)

Scripts de apoio:

- [switch_env_local.ps1](../scripts/switch_env_local.ps1)
- [switch_env_docker.ps1](../scripts/switch_env_docker.ps1)

Uso recomendado:

1. copiar `.env.local.example` para `.env.local`
2. copiar `.env.docker.example` para `.env.docker`
3. preencher os valores reais
4. usar o arquivo certo para cada tipo de execucao

Exemplo:

- Windows local:
  `. .\scripts\switch_env_local.ps1`
- Docker local:
  `. .\scripts\switch_env_docker.ps1`

## Como subir

Airflow e dbt:

- `docker compose -f docker-compose.local.yml up --build -d`

Dependencia externa:

- subir primeiro o Airbyte via `abctl`
- depois subir Airflow e dbt

## Nivel de producao

Esta stack fica mais proxima de producao do que o desenho anterior, porque:

- separa workers por tipo de carga
- reduz acoplamento entre Airflow e bibliotecas de execucao
- prepara o projeto para migracao futura a Kubernetes

Ainda assim, para producao real, o caminho mais comum e:

- Airflow em Kubernetes ou servico gerenciado
- Airbyte gerenciado ou Airbyte em Kubernetes com persistencia dedicada
- imagens publicadas em registry
- secrets via secret manager ou volumes protegidos
- pipelines promovidas entre dev, hml e prd

## Observacoes importantes

- O compose local e uma aproximacao de ambiente real, nao a versao final de producao.
- Para Airflow com Celery, os workers precisam compartilhar DAGs e configuracao homogenea.
- A documentacao oficial do Airflow recomenda cautela ao usar Docker Compose como base de producao e indica Kubernetes/Helm para ambientes realmente produtivos.
