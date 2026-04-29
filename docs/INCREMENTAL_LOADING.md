# Estrategia Incremental

Este projeto deixou de tratar o incremental da camada `DS` com pipelines Python customizadas.

O desenho atual separa a responsabilidade assim:

- Airbyte controla a estrategia de sincronizacao da origem para `DS`
- dbt controla o incremental da camada analitica de `DS` para `DW`

## Objetivo

Padronizar o incremental em duas camadas com responsabilidades claras:

- replicacao operacional no Airbyte
- transformacao analitica no dbt

## Camada DS

Na camada `DS`, o incremental deve ser definido no Airbyte conforme a capacidade da origem.

Preferencia operacional:

1. `CDC`, quando suportado e viavel
2. `incremental` baseado em coluna de atualizacao confiavel
3. `full refresh` apenas para tabelas pequenas ou cargas controladas

### Recomendacoes para DS

- preservar chaves naturais
- preservar colunas de atualizacao da origem
- evitar regras de negocio no Airbyte
- manter nomenclatura estavel para as tabelas consumidas pelo dbt

## Camada DW

Na camada `DW`, o incremental continua sendo responsabilidade do `dbt`.

Padrao esperado:

- `staging` lendo tabelas do `DS`
- modelos incrementais em `marts` quando houver ganho operacional
- regras de negocio e reconciliacao analitica implementadas no SQL do `dbt`

## O que mudou

Este repositorio nao assume mais:

- controle manual de watermark em Python para extracao DS
- janelas de `data_inicio` e `data_fim` aplicadas por script local
- logica de `MERGE` de DS mantida por pipeline Python deste projeto

Essas decisoes passam a ficar:

- no conector e na configuracao do Airbyte
- ou no desenho do destino Snowflake DS

## Relacao com Airflow

O Airflow pode participar de duas formas:

- executar apenas o `dbt run`, assumindo que o `DS` ja esta sincronizado
- disparar a sync do Airbyte via API e, depois, executar o `dbt`

O Airflow nao deve reimplementar a logica incremental da extracao.

## Compatibilidade com producao

Para manter compatibilidade com producao:

- documentar por stream qual estrategia de sync foi adotada no Airbyte
- validar custo e volume antes de usar `full refresh`
- garantir contrato estavel entre tabelas `DS` e modelos `staging`
- monitorar lag entre sync do Airbyte e execucao do `dbt`

## Resumo

O incremental do projeto agora e dividido assim:

- `origem -> DS`: Airbyte
- `DS -> DW`: dbt
