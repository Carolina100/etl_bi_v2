# Airbyte

Este documento define o uso do Airbyte como camada oficial de ingestao do projeto.

O desenho alvo e:

- Oracle / PostgreSQL -> Airbyte -> Snowflake RAW/landing
- Snowflake RAW/landing -> dbt -> Snowflake DS -> Snowflake DW
- Airflow orquestrando o `dbt` e, quando necessario, coordenando a execucao dos jobs do Airbyte

O repositorio nao deve mais concentrar logica Python de extracao DS.
A responsabilidade da ingestao fica no Airbyte.

## Objetivo

Padronizar a ingestao de dados em `DS` com uma ferramenta propria para replicacao, reduzindo acoplamento entre:

- codigo da pipeline
- conectividade com fontes Oracle/PostgreSQL
- controle de carga
- manutencao operacional

Com isso:

- Airbyte cuida da extracao e escrita na camada tecnica de aterrissagem
- dbt cuida das transformacoes para `DS` e `DW`
- Airflow coordena a ordem de execucao do fluxo analitico

## Escopo deste projeto

No contexto deste repositorio:

- Airbyte e a rota principal de ingestao da camada tecnica de aterrissagem
- a DAG local principal continua sendo `load_dw_dbt_dimensions_dag`
- o `dbt` consome tabelas aterrissadas pelo Airbyte e consolida `DS` e `DW`
- qualquer logica de filtro incremental, CDC ou refresh deve ser preferencialmente configurada no proprio Airbyte

Este documento cobre:

- uso local para desenvolvimento
- diretrizes para compatibilidade com producao
- limites de responsabilidade entre Airbyte, Airflow e dbt

## Arquitetura recomendada

### Responsabilidades

- Airbyte
  - conecta nas fontes Oracle e PostgreSQL
  - extrai dados
  - aplica estrategia de sincronizacao definida no conector
  - grava no Snowflake na camada tecnica de aterrissagem

- Snowflake RAW / landing
  - recebe a camada tecnica aterrissada pelo Airbyte
  - deve ser `TRANSIENT`
  - nao deve ser `TEMPORARY TABLE`
  - deve ter retencao curta e cleanup tecnico
  - nao e camada historica

- Snowflake DS
  - recebe a camada consolidada pelo `dbt`
  - concentra o current-state operacional de curto prazo
  - nao e camada historica

- dbt
  - trata padronizacao, limpeza e regras de negocio
  - constroi `staging`, `dimensions` e `facts` na camada `DW`

- Airflow
  - executa `dbt build`
  - opcionalmente dispara e monitora syncs do Airbyte via API
  - nao deve reimplementar a extracao das fontes neste repositorio

### Fronteira entre DS e DW

Para manter um desenho compativel com producao:

- `DS` deve representar a replicacao operacional da origem
- regras de negocio e modelagem analitica devem ficar no `dbt`
- reconciliacoes, refreshes e estrategia de sincronizacao devem nascer da configuracao do Airbyte, nao de scripts Python ad hoc

## Ambiente local atual

O Airbyte deve rodar localmente como servico externo do projeto, preferencialmente via `abctl`.

Os componentes do Airflow e do runtime dbt ficam em:

- [docker-compose.local.yml](../docker-compose.local.yml)

### Como subir localmente

1. instalar e subir o Airbyte localmente via `abctl`
2. ajustar `.env.docker`
3. executar `docker compose -f docker-compose.local.yml up --build -d`
4. acessar Airbyte em `http://localhost:8000`
5. acessar Airflow em `http://localhost:8080`

### Papel do ambiente local

Essa stack existe para:

- validar conectividade
- montar conectores
- testar sincronizacao DS
- rodar o `dbt` localmente apos a carga

Ela nao representa, por si so, a arquitetura final de producao.

## Padrao recomendado de configuracao no Airbyte

### Fontes

Criar conectores separados para cada sistema de origem relevante, por exemplo:

- Oracle como fonte transacional
- PostgreSQL como fonte operacional complementar

### Destino

Usar Snowflake como destino da camada tecnica de aterrissagem, apontando para:

- mesma conta Snowflake usada pelo `dbt`
- database e schema de `RAW` / landing consumidos por `dbt/solix_dbt`
- tabela tecnica de aterrissagem por entidade quando houver merge semantico antes do `DW`

### Nomeacao

Adotar uma convencao previsivel para conexoes e streams, por exemplo:

- `oracle_solix_to_snowflake_ds`
- `postgres_operacao_to_snowflake_ds`

### Estrategia de sincronizacao

A escolha depende da capacidade da origem:

- preferir `CDC` quando a fonte e o conector suportarem com maturidade operacional
- usar `incremental append + deduped` quando houver coluna confiavel de atualizacao
- usar `full refresh overwrite` apenas para tabelas pequenas ou cargas pontuais

Para producao, a estrategia deve ser documentada por entidade critica.

### Chaves e colunas tecnicas

Sempre que possivel:

- preservar chaves naturais da origem
- preservar colunas de atualizacao da origem, como `UPDATED_ON` ou equivalentes
- evitar transformacoes complexas dentro do Airbyte
- deixar derivacoes analiticas para o `dbt`

## Padrao de aterrissagem recomendado

Quando a entidade exigir merge semantico antes do `DW`, o padrao recomendado e:

- Airbyte escreve em uma `TRANSIENT TABLE` de aterrissagem
- a tabela nao deve ser `TEMPORARY`
- a reducao de storage deve vir de cleanup por janela tecnica
- o dbt faz o merge para a tabela final do `DS`

Exemplo adotado para a entidade piloto `SX_ESTADO_D`:

- `DS.STG_SX_ESTADO_D`
  - aterrissagem tecnica do Airbyte
- `DS.SX_ESTADO_D`
  - tabela curada final do `DS`
- `DW.SX_ESTADO_D`
  - dimensao analitica

Esse desenho evita manter uma camada `RAW` historica separada e preserva a logica de:

- merge por chave natural
- preservacao de `BI_CREATED_AT`
- atualizacao de `BI_UPDATED_AT` quando houver mudanca real
- inativacao de registros ausentes com `FG_ATIVO = 0`

### Estrategia hibrida recomendada

Para `SX_ESTADO_D`, o caminho recomendado e:

- sync incremental no dia a dia
- sync full periodica de reconciliacao
- inativacao de ausentes apenas na rodada full

Exemplo operacional:

- incremental de hora em hora
- full refresh overwrite 1x por dia

Importante:

- essa inativacao de ausentes so faz sentido se `DS.STG_SX_ESTADO_D` representar a foto completa da rodada
- por isso, o modelo da `DS` so inativa ausentes quando executado com modo de reconciliacao `full`
- nas execucoes incrementais, o modelo apenas insere novos registros e atualiza alterados

## Orquestracao com Airflow

O desenho minimo e:

1. Airbyte sincroniza a camada tecnica de aterrissagem
2. Airflow executa `dbt build`
3. `DS` e atualizado a partir da aterrissagem
4. `DW` e atualizado a partir do `DS`

No estado atual do repositorio:

- a DAG [load_dw_dbt_dimensions_dag.py](../dags/load_dw_dbt_dimensions_dag.py) e a DAG principal de execucao
- as DAGs de agendamento disparam essa DAG principal com `conf` diferente para incremental e full

Para um desenho mais proximo de producao, o recomendado e evoluir para uma destas abordagens:

- Airbyte agenda as sincronizacoes e Airflow roda depois, em janela compativel
- Airflow dispara o job Airbyte via API, aguarda conclusao e depois executa o `dbt`
- uma camada externa de orquestracao dispara ambos de forma coordenada

### Recomendacao pratica

Se o objetivo e rastreabilidade operacional fim a fim, a melhor opcao tende a ser:

- Airflow disparando a sync do Airbyte
- Airflow aguardando sucesso da sync
- Airflow executando `dbt build`
- agendas separadas no Airflow para incremental e reconciliacao full

Assim o estado da execucao fica concentrado num unico orquestrador, sem trazer a logica de extracao de volta para Python.

## Requisitos para producao

### Segredos

Nao manter credenciais dentro do repositorio ou da UI sem governanca.

Preferir:

- secret manager corporativo
- credenciais injetadas por ambiente
- key pair e secrets montados por volume seguro ou integracao nativa da plataforma

### Persistencia

Em producao, o Airbyte precisa de persistencia duravel para:

- banco de metadados
- estado dos conectores
- logs
- arquivos temporarios e workspace, quando aplicavel

O `docker-compose.local.yml` serve apenas como referencia local para Airflow e dbt.
O Airbyte local via `abctl` e apenas uma conveniencia de desenvolvimento.

### Deploy

Para producao, evitar depender de `docker compose` como modelo final.

Preferir:

- Airbyte gerenciado, quando disponivel
- ou Airbyte em Kubernetes / ambiente corporativo equivalente
- com banco externo e armazenamento persistente gerenciado

### Observabilidade

Operacionalmente, o minimo esperado e:

- historico de syncs
- alertas em falha
- visibilidade de throughput e latencia
- rastreabilidade entre execucao Airbyte e execucao dbt

### Isolamento de ambientes

Separar claramente:

- dev
- hml
- prd

Cada ambiente deve ter:

- conectores proprios
- credenciais proprias
- destinos Snowflake proprios ou schemas segregados

## Boas praticas de modelagem operacional

- manter `DS` o mais proximo possivel da origem
- evitar embutir regra de negocio no Airbyte
- documentar por stream qual estrategia de sync foi adotada
- validar impacto de `full refresh` antes de usar em producao
- garantir que tabelas consumidas pelo `dbt` tenham contrato de nome e granularidade estaveis

## Compatibilidade com este repositorio

Para este projeto funcionar bem com Airbyte:

- o Airbyte deve escrever no mesmo `DS` lido por `dbt/solix_dbt`
- os modelos `staging` devem assumir o schema e o contrato das tabelas replicadas
- o Airflow deve continuar restrito a orquestracao do `dbt` ou, no maximo, ao disparo da API do Airbyte
- dependencias de extracao Python devem ser tratadas como legado e removidas conforme a migracao for consolidada

## Fluxo operacional recomendado

### Desenvolvimento local

1. subir a stack local
2. configurar fonte Oracle/PostgreSQL no Airbyte
3. configurar destino Snowflake DS
4. executar sync manual no Airbyte
5. validar dados no `DS`
6. rodar `dbt build`
7. validar tabelas `DW`

### Producao

1. iniciar sync do Airbyte por agenda ou API
2. aguardar conclusao com sucesso
3. executar `dbt build` no `DW`
4. registrar logs, alertas e auditoria operacional

## Checklist operacional de `SX_ESTADO_D`

### 1. Preparar tabelas no Snowflake

Garantir a existencia das tabelas abaixo:

- `DS.STG_SX_ESTADO_D`
- `DS.SX_ESTADO_D`
- `DW.SX_ESTADO_D`

Script base:

- [create_ds_sx_estado_d.sql](../sql/ds/create_ds_sx_estado_d.sql)

### 2. Criar as duas connections no Airbyte

Connection incremental:

- nome sugerido: `sx_estado_d_incremental`
- origem: Oracle da entidade `SX_ESTADO_D`
- destino: Snowflake
- tabela destino: `SOLIX_BI.DS.STG_SX_ESTADO_D`
- estrategia: incremental por cursor
- frequencia: controlada pelo Airflow

Connection full:

- nome sugerido: `sx_estado_d_full_reconciliation`
- origem: Oracle da entidade `SX_ESTADO_D`
- destino: Snowflake
- tabela destino: `SOLIX_BI.DS.STG_SX_ESTADO_D`
- estrategia: `full refresh overwrite`
- frequencia: controlada pelo Airflow

### 3. Alinhar o contrato da tabela de aterrissagem

O modelo dbt atual espera em `DS.STG_SX_ESTADO_D` as colunas:

- `ID_CLIENTE`
- `CD_ESTADO`
- `DESC_ESTADO`
- `SOURCE_UPDATED_AT`
- `AIRBYTE_SYNCED_AT`

Se o Airbyte entregar nomes diferentes, ha duas opcoes aceitaveis:

- ajustar o mapeamento no Airbyte para gravar com esses nomes
- ou ajustar o modelo [ds_sx_estado_d.sql](../dbt/solix_dbt/models/staging/ds/ds_sx_estado_d.sql) para ler os nomes reais

### 4. Configurar as DAGs no Airflow

As DAGs de agendamento ja esperam estes nomes de connection:

- incremental:
  - `sx_estado_d_incremental`
- full:
  - `sx_estado_d_full_reconciliation`

Arquivos:

- [schedule_sx_estado_d_incremental_dag.py](../dags/schedule_sx_estado_d_incremental_dag.py)
- [schedule_sx_estado_d_full_dag.py](../dags/schedule_sx_estado_d_full_dag.py)

Se os nomes das connections no Airbyte forem diferentes, atualize as DAGs ou os nomes das connections para manter consistencia.

### 5. Teste ponta a ponta

Teste incremental:

1. executar a connection incremental no Airbyte ou disparar `schedule_sx_estado_d_incremental_dag`
2. validar se `DS.STG_SX_ESTADO_D` recebeu dados
3. validar se `DS.SX_ESTADO_D` inseriu novos registros e atualizou alterados
4. validar se `FG_ATIVO` nao foi zerado por ausentes nessa rodada
5. validar se `DW.SX_ESTADO_D` refletiu o resultado

Teste full:

1. executar a connection full no Airbyte ou disparar `schedule_sx_estado_d_full_dag`
2. validar se `DS.STG_SX_ESTADO_D` recebeu a foto completa
3. validar se `DS.SX_ESTADO_D` inativou ausentes com `FG_ATIVO = 0`
4. validar se `DW.SX_ESTADO_D` refletiu as inativacoes

### 6. Validacoes criticas

- a connection incremental nao deve sobrescrever a `STG` ao mesmo tempo que a full estiver rodando
- a connection full precisa entregar foto completa, nao apenas delta
- o cursor incremental da origem precisa estar alinhado com `SOURCE_UPDATED_AT`
- a coluna tecnica de tempo do Airbyte precisa ser validada para preencher `AIRBYTE_SYNCED_AT` ou equivalente

## Resumo

O posicionamento oficial deste projeto passa a ser:

- Airbyte para ingestao DS
- dbt para transformacao DW
- Airflow para orquestracao

Esse desenho e mais compativel com producao do que manter extracao customizada em Python dentro do repositorio.
