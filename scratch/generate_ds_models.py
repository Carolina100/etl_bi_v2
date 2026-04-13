import os
import re

with open(r"c:\Users\CarolinaIovanceGolfi\Desktop\etl_bi_v2\dbt\solix_dbt\models\staging\ds\ds_sx_estado_d.sql", "r", encoding="utf-8") as f:
    template = f.read()

models = [
    {
        "alias": "SX_EQUIPAMENTO_D",
        "raw_table": "CDT_EQUIPAMENTO",
        "keys": ["ID_CLIENTE", "CD_EQUIPAMENTO"],
        "cols": [
            ("ID_CLIENTE", "number(38, 0)", "-999"),
            ("CD_EQUIPAMENTO", "varchar", "''"),
            ("DESC_EQUIPAMENTO", "varchar", "''"),
            ("CD_MODELO_EQUIPAMENTO", "number(38, 0)", "-999"),
            ("DESC_MODELO_EQUIPAMENTO", "varchar", "''"),
            ("CD_TIPO_EQUIPAMENTO", "number(38, 0)", "-999"),
            ("DESC_TIPO_EQUIPAMENTO", "varchar", "''"),
            ("DESC_STATUS", "varchar", "''"),
            ("TP_USO_EQUIPAMENTO", "number(38, 0)", "-999")
        ]
    },
    {
        "alias": "SX_FAZENDA_D",
        "raw_table": "CDT_FAZENDA",
        "keys": ["ID_CLIENTE", "CD_FAZENDA", "CD_ZONA", "CD_TALHAO"],
        "cols": [
            ("ID_CLIENTE", "number(38, 0)", "-999"),
            ("CD_FAZENDA", "varchar", "''"),
            ("DESC_FAZENDA", "varchar", "''"),
            ("CD_TALHAO", "varchar", "''"),
            ("DESC_TALHAO", "varchar", "''"),
            ("CD_ZONA", "varchar", "''"),
            ("AREA_TOTAL", "number(38, 8)", "-999.99"),
            ("DESC_PRODUTOR", "varchar", "''")
        ]
    },
    {
        "alias": "SX_FRENTE_D",
        "raw_table": "CDT_FRENTE",
        "keys": ["ID_CLIENTE", "CD_CORPORATIVO", "CD_REGIONAL", "CD_UNIDADE", "CD_FRENTE"],
        "cols": [
            ("ID_CLIENTE", "number(38, 0)", "-999"),
            ("CD_CORPORATIVO", "number(38, 0)", "-999"),
            ("CD_REGIONAL", "number(38, 0)", "-999"),
            ("CD_UNIDADE", "number(38, 0)", "-999"),
            ("CD_FRENTE", "number(38, 0)", "-999"),
            ("DESC_CORPORATIVO", "varchar", "''"),
            ("DESC_REGIONAL", "varchar", "''"),
            ("DESC_UNIDADE", "varchar", "''"),
            ("DESC_FRENTE", "varchar", "''"),
            ("FG_FRENTE_TRABALHO", "number(38, 0)", "-999")
        ]
    },
    {
        "alias": "SX_OPERACAO_D",
        "raw_table": "CDT_OPERACAO",
        "keys": ["ID_CLIENTE", "CD_OPERACAO"],
        "cols": [
            ("ID_CLIENTE", "number(38, 0)", "-999"),
            ("CD_OPERACAO", "number(38, 0)", "-999"),
            ("DESC_OPERACAO", "varchar", "''"),
            ("CD_GRUPO_OPERACAO", "number(38, 0)", "-999"),
            ("DESC_GRUPO_OPERACAO", "varchar", "''"),
            ("CD_GRUPO_PARADA", "number(38, 0)", "-999"),
            ("DESC_GRUPO_PARADA", "varchar", "''"),
            ("FG_TIPO_OPERACAO", "varchar", "''"),
            ("CD_PROCESSO_TALHAO", "number(38, 0)", "-999"),
            ("DESC_PROCESSO_TALHAO", "varchar", "''")
        ]
    },
    {
        "alias": "SX_ORDEM_SERVICO_D",
        "raw_table": "CDT_ORDEM_SERVICO",
        "keys": ["ID_CLIENTE", "CD_ORDEM_SERVICO"],
        "cols": [
            ("ID_CLIENTE", "number(38, 0)", "-999"),
            ("CD_ORDEM_SERVICO", "number(38, 0)", "-999"),
            ("CD_ORD_STATUS", "number(38, 0)", "-999"),
            ("DT_CRIADO_EM", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DT_ABERTURA", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DT_ENCERRAMENTO", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DT_INICIO_EXEC", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DT_TERMINO_EXEC", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DT_INICIO_PLAN_EXEC", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DT_TERMINO_PLAN_EXEC", "timestamp_ntz", "'1900-01-01'::timestamp_ntz"),
            ("DESC_OS", "varchar", "''"),
            ("DESC_ORD_STATUS", "varchar", "''"),
            ("VL_LATITUDE", "number(23, 15)", "-999"),
            ("VL_LONGITUDE", "number(23, 15)", "-999"),
            ("FG_ORIGEM", "varchar(5)", "''"),
            ("FG_STATUS", "number(38, 0)", "-999"),
            ("VL_ORDEM_SERVICO", "varchar", "''"),
            ("TICKET_NUMBER", "number(38, 0)", "-999")
        ]
    }
]

for m in models:
    content = template[:]
    # Settings block
    content = re.sub(r"{% set ENTITY_RAW_TABLE = '.*?' %}", f"{{% set ENTITY_RAW_TABLE = '{m['raw_table']}' %}}", content)
    content = re.sub(r"{% set MODEL_ALIAS = '.*?' %}", f"{{% set MODEL_ALIAS = '{m['alias']}' %}}", content)
    content = re.sub(r"{% set NATURAL_KEY_COLUMNS = \[.*?\] %}", f"{{% set NATURAL_KEY_COLUMNS = [{', '.join([repr(k) for k in m['keys']])}] %}}", content)
    content = re.sub(r"{% set HAS_FG_ATIVO = false %}", f"{{% set HAS_FG_ATIVO = true %}}", content)

    # 1. Staged airbyte columns
    stgd = "\n".join([f"        cast({c[0]:25} as {c[1]:15}) as {c[0]}," for c in m['cols']])
    content = re.sub(r"-- ▼ Colunas de negócio da entidade.*?-- ▲ Fim das colunas de negócio", f"-- ▼ Colunas de negócio da entidade\n{stgd}\n        -- ▲ Fim das colunas de negócio", content, flags=re.DOTALL)

    # 2. qualify partition by
    content = re.sub(r"qualify row_number\(\) over \(\n        partition by .*?\n", f"qualify row_number() over (\n        partition by {', '.join(m['keys'])}\n", content)

    # 3. current_target block
    tgt_cols = "\n".join([f"        {c[0]}," for c in m['cols']])
    tgt_nul = "\n".join([f"        cast(null as {c[1]:15}) as {c[0]}," for c in m['cols']])
    
    pat_target = r"current_target as \(\n    {% if is_incremental\(\) %}\n    select.*?\n        FG_ATIVO"
    content = re.sub(pat_target, f"current_target as (\n    {{% if is_incremental() %}}\n    select\n{tgt_cols},\n        FG_ATIVO", content, flags=re.DOTALL)

    pat_target_null = r"{% else %}\n    select.*?\n        cast\(null as number\(1, 0\)\)\s+as FG_ATIVO"
    content = re.sub(pat_target_null, f"{{% else %}}\n    select\n{tgt_nul},\n        cast(null as number(1, 0))   as FG_ATIVO", content, flags=re.DOTALL)

    # 4. changed_or_new block
    cnh_cols = "\n".join([f"        s.{c[0]}," for c in m['cols']])
    pat_changed = r"changed_or_new as \(\n    select.*?\n        s\.FG_ATIVO"
    content = re.sub(pat_changed, f"changed_or_new as (\n    select\n{cnh_cols},\n        s.FG_ATIVO", content, flags=re.DOTALL)
    
    first_key = m['keys'][0]
    content = re.sub(r"when t\.CD_ESTADO is null", f"when t.{first_key} is null", content)
    
    # changed cases
    c_case = []
    for c in m['cols']:
        if c[0] not in m['keys']:
            c_case.append(f"            when coalesce(t.{c[0]}, {c[2]}) <> coalesce(s.{c[0]}, {c[2]})\n                then convert_timezone('UTC', current_timestamp())::timestamp_ntz")
    rpl_case = "\n".join(c_case)
    
    content = re.sub(r"when coalesce\(t\.DESC_ESTADO.*?:timestamp_ntz  -- descricao mudou", rpl_case, content, flags=re.DOTALL)
    
    # join cond
    j_cond = " and\n           ".join([f"s.{k} = t.{k}" for k in m['keys']])
    content = re.sub(r"on s\.CD_ESTADO = t\.CD_ESTADO", f"on {j_cond}", content)
    
    # where cond
    content = re.sub(r"where t\.CD_ESTADO is null.*?\n       or coalesce\(t\.DESC_ESTADO.*?mudou", f"where t.{first_key} is null", content, flags=re.DOTALL)
    w_cond = "\n".join([f"       or coalesce(t.{c[0]}, {c[2]}) <> coalesce(s.{c[0]}, {c[2]})" for c in m['cols'] if c[0] not in m['keys']])
    content = re.sub(r"where t\." + first_key + r" is null", f"where t.{first_key} is null\n{w_cond}", content)

    # 5. final select
    fnl_cols = ",\n    ".join([c[0] for c in m['cols']])
    pat_final = r"select\n    CD_ESTADO.*?FG_ATIVO"
    content = re.sub(pat_final, f"select\n    {fnl_cols},\n    FG_ATIVO", content, flags=re.DOTALL)
    
    
    outname = f"c:\\users\\carolinaiovancegolfi\\desktop\\etl_bi_v2\\dbt\\solix_dbt\\models\\staging\\ds\\ds_{m['alias'].lower()}.sql"
    with open(outname, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Generated {outname}")
