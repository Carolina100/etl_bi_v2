    create or replace view bi.vw_sx_fazenda_d as

/*
Objetivo:
- entregar para o Airbyte uma unica visao curada da dimensao de fazenda
- evitar que o Airbyte leia o schema operacional inteiro
- manter 1 linha por cliente, fazenda, talhao e zona = unique key 

Grao da view:
    - cd_cliente, cd_fazenda,cd_talhao,cd_zona = unique key

Uso no Airbyte:
- stream: bi.vw_sx_fazenda_d
- primary key: cd_cliente, cd_fazenda,cd_talhao,cd_zona
- cursor incremental: source_updated_at
*/

with fazenda_base as (

    /*
    Tabela principal da entidade.
    Ela define:
    - cliente
    - fazenda
    - talhao
    - zona
    - area total
    - descricao do produtor
    - flag de ativo
    - data de atualizacao da origem
    */

    select
        e.cd_cliente,
        e.cd_fazenda,
        e.desc_fazenda,
        e.cd_talhao,
        e.desc_talhao,
        e.cd_zona,
        e.area_metro,
        e.cd_produtor,
        e.active,
        e.dt_updated
    from sgpa_map.cdt_talhao e

),

fazenda_latest as (

    /*
    Garante 1 linha por cliente + fazenda.
    Se a origem tiver duplicidade, fica a linha mais recente por dt_updated.
    */

    select distinct on (
        e.cd_cliente,
        e.cd_fazenda,
        e.cd_zona,
        e.cd_talhao
    )
        e.cd_cliente,
        e.cd_fazenda,
        e.desc_fazenda,
        e.cd_talhao,
        e.desc_talhao,
        e.cd_zona,
        e.area_metro,
        e.cd_produtor,
        e.active,
        e.dt_updated
    from fazenda_base e
    order by
        e.cd_cliente,
        e.cd_fazenda,
        e.cd_zona,
        e.cd_talhao,
        e.dt_updated desc nulls last
),

produtor_base as (

    /*
    Cadastro de produtor.
    Usado apenas para enriquecer a descricao do produtor.
    */

    select
        m.cd_cliente,
        m.cd_produtor,
        m.desc_nome,
        m.dt_updated
    from sgpa_map.cdt_produtor m
),

produtor_latest as (

    /*
    Garante 1 linha por cliente + produtor.
    Evita multiplicar produtores no join com a tabela de produtor.
    */

    select distinct on (
        m.cd_cliente,
        m.cd_produtor
    )
        m.cd_cliente,
        m.cd_produtor,
        m.desc_nome,
        m.dt_updated
    from produtor_base m
    order by
        m.cd_cliente,
        m.cd_produtor,
        m.dt_updated desc nulls last
)


select
    /*
    Chave natural da dimensao.
    Essas colunas devem ser a primary key configurada no Airbyte.
    */

    e.cd_cliente::numeric(38, 0) as id_cliente,
    e.cd_fazenda::varchar(20) as cd_fazenda,
    e.cd_talhao::varchar(20) as cd_talhao,
    e.cd_zona::varchar(20) as cd_zona,

    /*
    Atributos principais do fazenda.
    */

    e.desc_fazenda::varchar(255) as desc_fazenda,  
    e.desc_talhao::varchar(255) as desc_talhao,
    e.area_metro::numeric(38, 8) as area_total,
    e.cd_produtor::numeric(38, 0) as cd_produtor,       
    m.desc_nome::varchar(255) as desc_produtor,

    /*
    Flag oficial de ativo/inativo.
    Mantemos a regra principal vindo da tabela de fazenda.
    */

    e.active as fg_ativo,

    /*              
    Cursor incremental para o Airbyte.
    */

    greatest(
        coalesce(e.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(m.dt_updated, timestamp '1900-01-01 00:00:00')
    ) as source_updated_at
from fazenda_latest e
left join produtor_latest m
    on e.cd_cliente = m.cd_cliente
   and e.cd_produtor = m.cd_produtor        