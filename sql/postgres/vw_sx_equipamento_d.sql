create or replace view bi.vw_sx_equipamento_d as

/*
Objetivo:
- entregar para o Airbyte uma unica visao curada da dimensao de equipamento
- evitar que o Airbyte leia o schema operacional inteiro
- manter 1 linha por cliente + equipamento

Grao da view:
- cd_cliente + cd_equipamento

Uso no Airbyte:
- stream: bi.vw_sx_equipamento_d
- primary key: id_cliente, cd_equipamento
- cursor incremental: source_updated_at
*/

with equipamento_base as (

    /*
    Tabela principal da entidade.
    Ela define:
    - cliente
    - equipamento
    - modelo
    - tipo
    - descricao
    - ativo/inativo principal
    */

    select
        e.cd_cliente,
        e.cd_equipamento,
        e.desc_equipamento,
        e.cd_modelo_equipamento,
        e.cd_tp_equipamento,
        e.tp_uso_equipamento,
        e.fg_ativo,
        e.dt_updated
    from sgpa_map.cdt_equipamento e

    -- Filtro de tipos de equipamento usados na dimensao.
    where e.cd_tp_equipamento in (
        '101', '103', '104', '105', '106',
        '107', '108', '120', '121', '122'
    )
),

equipamento_latest as (

    /*
    Garante 1 linha por cliente + equipamento.
    Se a origem tiver duplicidade, fica a linha mais recente por dt_updated.
    */

    select distinct on (
        e.cd_cliente,
        e.cd_equipamento
    )
        e.cd_cliente,
        e.cd_equipamento,
        e.desc_equipamento,
        e.cd_modelo_equipamento,
        e.cd_tp_equipamento,
        e.tp_uso_equipamento,
        e.fg_ativo,
        e.dt_updated
    from equipamento_base e
    order by
        e.cd_cliente,
        e.cd_equipamento,
        e.dt_updated desc nulls last
),

modelo_base as (

    /*
    Cadastro de modelo do equipamento.
    Usado apenas para enriquecer a descricao do modelo.
    */

    select
        m.cd_cliente,
        m.cd_modelo_equipamento,
        m.desc_modelo_equipamento,
        m.dt_updated
    from sgpa_map.cdt_modelo_equipamento m
),

modelo_latest as (

    /*
    Garante 1 linha por cliente + modelo.
    Evita multiplicar equipamentos no join com a tabela de modelo.
    */

    select distinct on (
        m.cd_cliente,
        m.cd_modelo_equipamento
    )
        m.cd_cliente,
        m.cd_modelo_equipamento,
        m.desc_modelo_equipamento,
        m.dt_updated
    from modelo_base m
    order by
        m.cd_cliente,
        m.cd_modelo_equipamento,
        m.dt_updated desc nulls last
),

tipo_base as (

    /*
    Cadastro de tipo do equipamento.
    Usado apenas para enriquecer a descricao do tipo.
    */

    select
        t.cd_cliente,
        t.cd_tp_equipamento,
        t.desc_tp_equipamento,
        t.dt_updated
    from sgpa_map.cdt_tipo_equipamento t
),

tipo_latest as (

    /*
    Garante 1 linha por cliente + tipo.
    Evita multiplicar equipamentos no join com a tabela de tipo.
    */

    select distinct on (
        t.cd_cliente,
        t.cd_tp_equipamento
    )
        t.cd_cliente,
        t.cd_tp_equipamento,
        t.desc_tp_equipamento,
        t.dt_updated
    from tipo_base t
    order by
        t.cd_cliente,
        t.cd_tp_equipamento,
        t.dt_updated desc nulls last
),

status_base as (

    /*
    Historico de movimento do equipamento.
    Usado para buscar o ultimo status conhecido.
    */

    select
        h.cd_cliente,
        h.cd_equipamento,
        h.fg_ativo as fg_ativo_status,
        h.dt_hr_utc_movimento,
        h.dt_updated
    from sgpa_map.cdt_equipamento_historico_mov h
),

ultimo_status as (

    /*
    Mantem apenas o ultimo evento de status por cliente + equipamento.
    */

    select distinct on (
        s.cd_cliente,
        s.cd_equipamento
    )
        s.cd_cliente,
        s.cd_equipamento,
        s.fg_ativo_status,
        s.dt_hr_utc_movimento,
        s.dt_updated
    from status_base s
    order by
        s.cd_cliente,
        s.cd_equipamento,
        s.dt_hr_utc_movimento desc nulls last,
        s.dt_updated desc nulls last
)

select
    /*
    Chave natural da dimensao.
    Essas colunas devem ser a primary key configurada no Airbyte.
    */

    e.cd_cliente::numeric(38, 0) as id_cliente,
    e.cd_equipamento::varchar(20) as cd_equipamento,

    /*
    Atributos principais do equipamento.
    */

    e.desc_equipamento::varchar(1000) as desc_equipamento,

    coalesce(m.cd_modelo_equipamento, -1)::numeric(38, 0) as cd_modelo_equipamento,
    coalesce(m.desc_modelo_equipamento, 'UNDEFINED')::varchar(1000) as desc_modelo_equipamento,

    coalesce(t.cd_tp_equipamento, -1)::numeric(38, 0) as cd_tipo_equipamento,
    coalesce(t.desc_tp_equipamento, 'UNDEFINED')::varchar(1000) as desc_tipo_equipamento,

    /*
    Status textual derivado do ultimo movimento conhecido.
    */

    coalesce(
        case
            when upper(us.fg_ativo_status::text) in ('FALSE', '0', 'N') then 'I'
            when upper(us.fg_ativo_status::text) in ('TRUE', '1', 'S') then 'A'
            else null
        end,
        'UNDEFINED'
    )::varchar(20) as desc_status,

    coalesce(e.tp_uso_equipamento, -1)::numeric(38, 0) as tp_uso_equipamento,

    /*
    Flag oficial de ativo/inativo.
    Mantemos a regra principal vindo da tabela de equipamento.
    */

    case
        when upper(e.fg_ativo::text) in ('TRUE', '1', 'S') then true
        when upper(e.fg_ativo::text) in ('FALSE', '0', 'N') then false
        else null
    end as fg_ativo,

    /*
    Cursor incremental para o Airbyte.
    Importante:
    se mudar equipamento, modelo, tipo ou status, a linha precisa ser reextraida.
    */

    greatest(
        coalesce(e.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(m.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(t.dt_updated, timestamp '1900-01-01 00:00:00'),
        coalesce(us.dt_hr_utc_movimento, timestamp '1900-01-01 00:00:00'),
        coalesce(us.dt_updated, timestamp '1900-01-01 00:00:00')
    ) as source_updated_at

from equipamento_latest e

left join modelo_latest m
    on e.cd_cliente = m.cd_cliente
   and e.cd_modelo_equipamento = m.cd_modelo_equipamento

left join tipo_latest t
    on e.cd_cliente = t.cd_cliente
   and e.cd_tp_equipamento = t.cd_tp_equipamento

left join ultimo_status us
    on e.cd_cliente = us.cd_cliente
   and e.cd_equipamento = us.cd_equipamento;
