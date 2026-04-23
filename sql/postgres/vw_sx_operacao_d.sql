create or replace view bi.vw_sx_operacao_d as

/*
Objetivo:
- entregar para o Airbyte uma unica visao curada da dimensao de operacao
- evitar que o Airbyte leia o schema operacional inteiro
- manter 1 linha por cliente + operacao

Grao da view:
- cd_cliente + cd_operacao

Uso no Airbyte:
- stream: bi.vw_sx_operacao_d
- primary key: id_cliente, cd_operacao
- cursor incremental: source_updated_at
*/

with operacao_base as (

    /*
    Tabela principal da entidade.
    Ela define:
    - cliente
    - operacao
    - grupo de operacao
    - grupo de parada
    - tipo de operacao
    - processo
    - ativo/inativo principal
    */
    select 
        o.cd_cliente,
        o.cd_operacao,
        o.desc_operac,
        o.cd_grupo_operac,
        o.cd_grupo_parada,
        o.fg_tipo_operac,
        o.cd_processo,
        o.fg_ativo,
        o.dt_updated
    from sgpa_map.cdt_operacao o 
),

operacao_latest as (

    /*
    Garante 1 linha por cliente + operacao.
    Se a origem tiver duplicidade, fica a linha mais recente por dt_updated.
    */

    select distinct on (
        o.cd_cliente,
        o.cd_operacao
    )
        o.cd_cliente,
        o.cd_operacao,
        o.desc_operac,
        o.cd_grupo_operac,
        o.cd_grupo_parada,
        o.fg_tipo_operac,
        o.cd_processo,
        o.fg_ativo,
        o.dt_updated
    from operacao_base o
    order by
        o.cd_cliente,
        o.cd_operacao,
        o.dt_updated desc nulls last
),

grupo_operacao_base as (

    /*
    Cadastro de grupo de operacao.
    Usado apenas para enriquecer a descricao do grupo.
    */

    select
        m.cd_cliente,
        m.cd_grupo_operac,
        m.desc_grupo_operac,
        m.dt_updated
    from sgpa_map.cdt_grupo_operacao m
),

grupo_operacao_latest as (

    /*
    Garante 1 linha por cliente + grupo de operacao.
    Evita multiplicar operacoes no join com a tabela de grupo de operacao.
    */

    select distinct on (
        m.cd_cliente,
        m.cd_grupo_operac
    )
        m.cd_cliente,
        m.cd_grupo_operac,
        m.desc_grupo_operac,
        m.dt_updated
    from grupo_operacao_base m
    order by
        m.cd_cliente,
        m.cd_grupo_operac,
        m.dt_updated desc nulls last
),

processo_base as (

    /*
    Cadastro de processo.
    Usado apenas para enriquecer a descricao do processo.
    */

    select
        t.cd_cliente,
        t.cd_processo,
        t.desc_processo,
        t.dt_updated
    from sgpa_map.cdt_processos_talhao t
),

processo_latest as (

    /*
    Garante 1 linha por cliente + processo.
    Evita multiplicar operacoes no join com a tabela de processo.
    */

    select distinct on (
        t.cd_cliente,
        t.cd_processo
    )
        t.cd_cliente,
        t.cd_processo,
        t.desc_processo,
        t.dt_updated
    from processo_base t
    order by
        t.cd_cliente,
        t.cd_processo,
        t.dt_updated desc nulls last
)


select
    /*
    Chave natural da dimensao.
    Essas colunas devem ser a primary key configurada no Airbyte.
    */

    o.cd_cliente::numeric(38, 0) as id_cliente,
    o.cd_operacao::numeric(38, 0) as cd_operacao,

    /*
    Atributos principais do operacao.
    */

    upper(o.desc_operac)::varchar(1000) as desc_operac,

    coalesce(o.cd_grupo_operac, -1)::numeric(38, 0) as cd_grupo_operacao,
    coalesce(upper(m.desc_grupo_operac), 'UNDEFINED')::varchar(1000) as desc_grupo_operac,
    coalesce(o.cd_grupo_parada, -1)::numeric(38, 0) as desc_grupo_parada,
    coalesce(o.cd_processo, -1)::numeric(38, 0) as cd_processo,
    coalesce(upper(t.desc_processo), 'UNDEFINED')::varchar(1000) as desc_processo,
    o.fg_tipo_operac::varchar(1000) as fg_tipo_operacao,

    /*
    Flag oficial de ativo/inativo.
    Mantemos a regra principal vindo da tabela de equipamento.
    */

    o.fg_ativo as fg_ativo,

    /*
    Cursor incremental para o Airbyte.
    Importante:
    se mudar operacao, grupo de operacao e processo, a linha precisa ser reextraida.
    */

    greatest(
        coalesce(o.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(m.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(t.dt_updated, timestamp '1900-01-01 00:00:00')
    ) as source_updated_at

from operacao_latest o

left join grupo_operacao_latest m
    on o.cd_cliente = m.cd_cliente
   and o.cd_grupo_operac = m.cd_grupo_operac

left join processo_latest t
    on o.cd_cliente = t.cd_cliente
   and o.cd_processo = t.cd_processo