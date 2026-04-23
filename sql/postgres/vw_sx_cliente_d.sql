create or replace view bi.vw_sx_cliente_d as

/*
Objetivo:
- entregar para o Airbyte uma unica visao curada da dimensao de cliente
- evitar que o Airbyte leia o schema operacional inteiro
- manter 1 linha por cliente

Grao da view:
- cd_id

Uso no Airbyte:
- stream: bi.vw_sx_cliente_d
- cursor incremental: source_updated_at

Observacao importante:
- a origem nao possui dt_updated
- o campo source_updated_at abaixo e tecnico e estavel, apenas para padronizar
  o contrato entre as views de dimensao
- se a extracao de cliente precisar refletir mudanca de FG_ATIVO com confiabilidade,
  o modo mais seguro no Airbyte continua sendo full refresh
*/

with cliente_base as (

    /*
    Tabela principal da entidade.
    Ela define:
    - identificador do cliente
    - nome do cliente
    - owner do ambiente
    - servidor associado
    - flags operacionais
    */

    select
        c.cd_id,
        c.nome,
        c.owner,
        c.server,
        c.fg_ativo
    from sgpa_map.cdt_cliente c
),

cliente_latest as (

    /*
    Garante 1 linha por cliente.
    Como a origem nao tem dt_updated, mantemos uma linha arbitraria por cd_id.
    */

    select distinct on (
        c.cd_id
    )
        c.cd_id,
        c.nome,
        c.owner,
        c.server,
        c.fg_ativo
    from cliente_base c
    order by
        c.cd_id
)

select
    c.cd_id::numeric(38, 0) as id_cliente,
    upper(c.nome)::varchar(255) as nome_cliente,
    upper(c.owner)::varchar(255) as owner_cliente,
    upper(c.server)::varchar(255) as server_cliente,
    c.fg_ativo as fg_ativo,
    /*
    Como a origem nao possui dt_updated confiavel para incremental,
    mantemos um timestamp tecnico estavel para preservar o mesmo contrato
    das demais dimensoes no pipeline.
    */

    timestamp '1900-01-01 00:00:00' as source_updated_at
from cliente_latest c;
