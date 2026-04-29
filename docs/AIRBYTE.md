# Airbyte

Este documento define o uso do Airbyte como camada oficial de ingestao do projeto.

O desenho alvo e:

- fontes externas -> Airbyte -> Snowflake RAW/landing
- Snowflake RAW/landing -> dbt -> Snowflake DS -> Snowflake DW
- Airflow orquestrando o `dbt` e, quando necessario, coordenando a execucao dos jobs do Airbyte

O repositorio nao deve mais concentrar logica Python de extracao DS.
A responsabilidade da ingestao fica no Airbyte.

## Objetivo

Padronizar a ingestao de dados em `DS` com uma ferramenta propria para replicacao, reduzindo acoplamento entre:

- codigo da pipeline
- conectividade com fontes externas
- controle de carga
- manutencao operacional

Com isso:

- Airbyte cuida da extracao e escrita na camada tecnica de aterrissagem
- dbt cuida das transformacoes para `DS` e `DW`
- Airflow coordena a ordem de execucao do fluxo analitico

## Escopo deste projeto

No contexto deste repositorio:

- Airbyte e a rota principal de ingestao da camada tecnica de aterrissagem
- a trilha operacional ativa e a de `sx_equipamento_d`
- o `dbt` consome tabelas aterrissadas pelo Airbyte e consolida `DS` e `DW`
- qualquer logica de filtro incremental, CDC ou refresh deve ser preferencialmente configurada no proprio Airbyte

Este documento cobre:

- uso local para desenvolvimento
- diretrizes para compatibilidade com producao
- limites de responsabilidade entre Airbyte, Airflow e dbt

## Arquitetura recomendada

### Responsabilidades

- Airbyte
  - conecta nas fontes configuradas no ambiente
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
  - executa `dbt run` em producao
  - pode executar `dbt build` em HML/validacao quando `dbt_command=build`
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

- uma fonte transacional principal
- uma fonte operacional complementar, quando existir

### Destino

Usar Snowflake como destino da camada tecnica de aterrissagem, apontando para:

- mesma conta Snowflake usada pelo `dbt`
- database e schema de `RAW` / landing consumidos por `dbt/solix_dbt`
- tabela tecnica de aterrissagem por entidade quando houver merge semantico antes do `DW`

### Nomeacao

Adotar uma convencao previsivel para conexoes e streams, por exemplo:

- `fonte_principal_to_snowflake_raw`
- `fonte_operacional_to_snowflake_raw`

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

Exemplo adotado para a entidade piloto `SX_EQUIPAMENTO_D`:

- `RAW.VW_SX_EQUIPAMENTO_D`
  - aterrissagem tecnica principal do Airbyte
- `DS.SX_EQUIPAMENTO_D`
  - tabela curada final do `DS`
- `DW.SX_EQUIPAMENTO_D`
  - dimensao analitica

Esse desenho preserva a logica de:

- `RAW` tecnico e efemero
- merge current-state no `DS`
- incremental no `DW` a partir de `BI_UPDATED_AT`
- cleanup do `RAW` por execucao e cleanup do `DS` por agenda separada

## Orquestracao com Airflow

O desenho minimo e:

1. Airbyte sincroniza a camada tecnica de aterrissagem
2. Airflow executa `dbt build`
3. `DS` e atualizado a partir da aterrissagem
4. `DW` e atualizado a partir do `DS`

No estado atual do repositorio:

- a DAG [orchestrate_ds_dw_dimensions_dag.py](../dags/orchestrate_ds_dw_dimensions_dag.py) e a DAG principal de orquestracao fim a fim
- a DAG [schedule_dimensions_incremental_dag.py](../dags/schedule_dimensions_incremental_dag.py) dispara o dominio `dimensions`
- o Airbyte de dimensoes deve ser tratado como connection compartilhada do dominio, nao como connection por entidade

Para um desenho mais proximo de producao, o recomendado e evoluir para uma destas abordagens:

- Airbyte agenda as sincronizacoes e Airflow roda depois, em janela compativel
- Airflow dispara o job Airbyte via API, aguarda conclusao e depois executa o `dbt`
- uma camada externa de orquestracao dispara ambos de forma coordenada

### Recomendacao pratica

Se o objetivo e rastreabilidade operacional fim a fim, a melhor opcao tende a ser:

- Airflow disparando a sync do Airbyte
- Airflow aguardando sucesso da sync
- Airflow executando `dbt build`
- agenda operacional incremental no Airflow para o dominio de dimensoes

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
2. configurar as fontes necessarias no Airbyte
3. configurar destino Snowflake `RAW`
4. executar sync manual no Airbyte
5. validar dados no `RAW`
6. rodar a trilha `Airbyte -> RAW -> DS -> DW`
7. validar tabelas `DS` e `DW`

### Producao

1. iniciar sync do Airbyte por agenda ou API
2. aguardar conclusao com sucesso
3. executar `dbt build` no `DW`
4. registrar logs, alertas e auditoria operacional

## Checklist operacional de `SX_EQUIPAMENTO_D`

### 1. Preparar tabelas no Snowflake

Garantir a existencia das tabelas abaixo:

- `RAW.VW_SX_EQUIPAMENTO_D`
- `DS.SX_EQUIPAMENTO_D`
- `DW.SX_EQUIPAMENTO_D`

Script base:

- [create_ds_sx_equipamento_d.sql](../sql/ds/create_ds_sx_equipamento_d.sql)

### 2. Criar a connection de dimensoes no Airbyte

Connection operacional:

- origem: stream `bi.vw_sx_equipamento_d` dentro da connection de `dimensions`
- destino: Snowflake
- tabela destino principal: `SOLIX_BI.RAW.VW_SX_EQUIPAMENTO_D`
- estrategia: incremental por cursor
- frequencia: controlada pelo Airflow

### 3. Alinhar o contrato da tabela de aterrissagem

O modelo dbt atual espera no `RAW` e no `DS` os contratos usados pela trilha de equipamento.

Na camada final `DS.SX_EQUIPAMENTO_D`, as colunas tecnicas principais sao:

- `ID_CLIENTE`
- `CD_EQUIPAMENTO`
- `FG_ATIVO`
- `SOURCE_UPDATED_AT`
- `AIRBYTE_EXTRACTED_AT`

Se o Airbyte entregar nomes diferentes, ha duas opcoes aceitaveis:

- ajustar o mapeamento no Airbyte para gravar com esses nomes
- ou ajustar o modelo [ds_sx_equipamento_d.sql](../dbt/solix_dbt/models/staging/ds/ds_sx_equipamento_d.sql) para ler os nomes reais

### 4. Configurar as DAGs no Airflow

Na estrutura atual, a trilha operacional de dimensoes e mantida por dominio compartilhado.

Arquivo:

- [schedule_dimensions_incremental_dag.py](../dags/schedule_dimensions_incremental_dag.py)

O `connection_id` do Airbyte fica configurado dentro da DAG de agendamento ou pode ser sobrescrito via `dag_run.conf`.

### 5. Teste ponta a ponta

Teste incremental:

1. executar a connection do Airbyte ou disparar `schedule_dimensions_incremental_dag`
2. validar se `RAW.VW_SX_EQUIPAMENTO_D` recebeu dados
3. validar se `DS.SX_EQUIPAMENTO_D` inseriu novos registros e atualizou alterados
4. validar se `FG_ATIVO` refletiu o current-state recebido
5. validar se `DW.SX_EQUIPAMENTO_D` refletiu o resultado

### 6. Validacoes criticas

- a connection do Airbyte nao deve concorrer com outra execucao do mesmo dominio
- o cursor incremental da origem precisa estar alinhado com `SOURCE_UPDATED_AT`
- a coluna tecnica de tempo do Airbyte precisa ser validada para preencher `AIRBYTE_EXTRACTED_AT` ou equivalente

## Resumo

O posicionamento oficial deste projeto passa a ser:

- Airbyte para ingestao tecnica em `RAW`
- dbt para transformacao `RAW -> DS -> DW`
- Airflow para orquestracao

Esse desenho e mais compativel com producao do que manter extracao customizada em Python dentro do repositorio.
