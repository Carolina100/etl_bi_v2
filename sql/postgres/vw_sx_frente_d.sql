create or replace view bi.vw_sx_frente_d as

/*
Objetivo:
- entregar para o Airbyte uma unica visao curada da dimensao de frente
- evitar que o Airbyte leia o schema operacional inteiro
- manter 1 linha por cliente + frente

Grao da view:
- cd_cliente + cd_corporativo, cd_regional, cd_unidade, cd_frente

Uso no Airbyte:
- stream: bi.vw_sx_frente_d
- primary key: id_cliente, cd_corporativo, cd_regional, cd_unidade, cd_frente
- cursor incremental: source_updated_at
*/

with unidade_base as (

    /*
    Tabela principal da entidade.
    Ela define:
    - cliente
    - unidade
    - descricao
    - flag de ativo
    - data de atualizacao da origem
    */
    
    select
        u.cd_cliente,
        u.cd_unidade,
        u.cd_regional,
        u.desc_unidade,
        u.fg_ativo,
        u.dt_updated
    from sgpa_map.cdt_unidade u
),

unidade_latest as (

    /*
    Garante 1 linha por cliente + unidade.
    Se a origem tiver duplicidade, fica a linha mais recente por dt_updated.
    */

    select distinct on (
        u.cd_cliente,
        u.cd_unidade,
        u.cd_regional
    )
        u.cd_cliente,
        u.cd_unidade,
        u.cd_regional,
        u.desc_unidade,
        u.fg_ativo,
        u.dt_updated
    from unidade_base u
    order by
        u.cd_cliente,   
        u.cd_unidade,
        u.cd_regional,
        u.dt_updated desc nulls last    
),

grupo_equipamento_base as (

    /*
    Cadastro de grupo de equipamento.
    Usado apenas para enriquecer a descricao do grupo de equipamento.
    */

    select
        m.cd_cliente,
        m.cd_grupo_equipamento,
        m.desc_grupo_equipamento,
        m.dt_updated
    from sgpa_map.cdt_grupo_equipamento m
),

grupo_equipamento_latest as (

    /*
    Garante 1 linha por cliente + grupo de equipamento.
    Evita multiplicar equipamentos no join com a tabela de grupo de equipamento.
    */

    select distinct on (
        m.cd_cliente,
        m.cd_grupo_equipamento
    )
        m.cd_cliente,
        m.cd_grupo_equipamento,
        m.desc_grupo_equipamento,
        m.dt_updated
    from grupo_equipamento_base m
    order by
        m.cd_cliente,
        m.cd_grupo_equipamento,
        m.dt_updated desc nulls last
),

regional_base as (

    /*
    Cadastro de regional.
    Usado apenas para enriquecer a descricao do regional.
    */

    select
        t.cd_cliente,
        t.cd_regional,
        t.cd_corporativo,
        t.desc_regional,
        t.dt_updated
    from sgpa_map.cdt_regional t
),

regional_latest as (

    /*
    Garante 1 linha por cliente + regional.
    Evita multiplicar unidades no join com a tabela de regional.
    */

    select distinct on (
        t.cd_cliente,
        t.cd_regional,
        t.cd_corporativo
    )
        t.cd_cliente,
        t.cd_regional,
        t.cd_corporativo,
        t.desc_regional,
        t.dt_updated
    from regional_base t
    order by
        t.cd_cliente,
        t.cd_regional,
        t.cd_corporativo,
        t.dt_updated desc nulls last
),

corporativo_base as (

    /*
    Cadastro de corporativo.
    Usado para enriquecer a descricao do corporativo.
    */

    select
        h.cd_cliente,
        h.cd_corporativo,
        h.desc_corporativo,
        h.dt_updated
    from sgpa_map.cdt_corporativo h
),

corporativo_latest as (

    /*
    Garante 1 linha por cliente + corporativo.
    Evita multiplicar unidades no join com a tabela de corporativo.
    */

    select distinct on (
        s.cd_cliente,
        s.cd_corporativo
    )
        s.cd_cliente,
        s.cd_corporativo,
        s.desc_corporativo,
        s.dt_updated    
    from corporativo_base s
    order by
        s.cd_cliente,
        s.cd_corporativo,
        s.dt_updated desc nulls last
)

select
    /*
    Chave natural da dimensao.
    Essas colunas devem ser a primary key configurada no Airbyte.
    */

    u.cd_cliente::numeric(38, 0) as id_cliente,
    c.cd_corporativo::numeric(38, 0) as cd_corporativo,
    r.cd_regional::numeric(38, 0) as cd_regional,
    u.cd_unidade::numeric(38, 0) as cd_unidade,
    g.cd_grupo_equipamento::numeric(38, 0) as cd_frente,

    /*
    Atributos principais do equipamento.
    */

    c.desc_corporativo::varchar(1000) as desc_corporativo,
    r.desc_regional::varchar(1000) as desc_regional,
    u.desc_unidade::varchar(1000) as desc_unidade,
    g.desc_grupo_equipamento::varchar(1000) as desc_frente,


    /*
    Flag oficial de ativo/inativo.
    Mantemos a regra principal vindo da tabela de frente.
    */

    u.fg_ativo as fg_ativo,

    /*
    Cursor incremental para o Airbyte.
    Importante:
    se mudar frente, regional, unidade ou corporativo, a linha precisa ser reextraida.
    */
    greatest(
        coalesce(u.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(g.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(r.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(c.dt_updated, timestamp '1900-01-01 00:00:00')
    ) as source_updated_at

from unidade_latest u

cross join grupo_equipamento_latest g

left join regional_latest r
    on u.cd_cliente = r.cd_cliente
   and u.cd_regional = r.cd_regional

left join corporativo_latest c
    on r.cd_cliente = c.cd_cliente
   and r.cd_corporativo = c.cd_corporativo