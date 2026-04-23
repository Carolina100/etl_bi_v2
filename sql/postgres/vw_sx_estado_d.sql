create or replace view bi.vw_sx_estado_d as

/*
Objetivo:
- entregar para o Airbyte uma unica visao curada da dimensao de estado
- evitar que o Airbyte leia o schema operacional inteiro
- manter 1 linha por estado

Grao da view:
- cd_estado

Uso no Airbyte:
- stream: bi.vw_sx_estado_d
- cursor incremental: source_updated_at
*/

with estado_base as (

    /*
    Tabela principal da entidade.
    Ela define:
    - codigo do estado
    - descricao do estado
    - flag de ativo
    - data de atualizacao da origem
    */

    select
        e.cd_estado,
        e.desc_estado,
        e.estado_json,
        e.dt_updated
    from sgpa_map.cdt_estado e
),

estado_latest as (

    /*
    Garante 1 linha por estado.
    Se a origem tiver duplicidade, fica a linha mais recente por dt_updated.
    */

    select distinct on (
        e.cd_estado
    )
        e.cd_estado,
        e.desc_estado,
        e.estado_json,
        e.dt_updated
    from estado_base e
    order by
        e.cd_estado,
        e.dt_updated desc nulls last
)

select
    /*
    Chave natural da dimensao.
    */

    e.cd_estado::varchar(50) as cd_estado,

    /*
    Atributos principais do estado.
    */

    e.desc_estado::varchar(255) as desc_estado,

    /*
    Traducoes vindas do JSON da origem.
    As colunas ficam explicitas para facilitar Airbyte, DS e consumo analitico.
    */

    upper(e.estado_json::jsonb ->> 'en-us')::varchar(255) as desc_estado_en_us,
    upper(e.estado_json::jsonb ->> 'pt-br')::varchar(255) as desc_estado_pt_br,
    upper(e.estado_json::jsonb ->> 'es-es')::varchar(255) as desc_estado_es_es,

    /*
    Cursor incremental para o Airbyte.
    */

    coalesce(
        e.dt_updated,
        timestamp '1900-01-01 00:00:00'
    ) as source_updated_at

from estado_latest e;
