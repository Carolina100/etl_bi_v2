alter table SOLIX_BI.DS.SX_ESTADO_D add column if not exists FG_ATIVO number(1, 0) not null default 1;
alter table SOLIX_BI.DS.SX_OPERACAO_D add column if not exists FG_ATIVO number(1, 0) not null default 1;
alter table SOLIX_BI.DS.SX_FAZENDA_D add column if not exists FG_ATIVO number(1, 0) not null default 1;
alter table SOLIX_BI.DS.SX_EQUIPAMENTO_D add column if not exists FG_ATIVO number(1, 0) not null default 1;
alter table SOLIX_BI.DS.SX_FRENTE_D add column if not exists FG_ATIVO number(1, 0) not null default 1;
alter table SOLIX_BI.DS.SX_ORDEM_SERVICO_D add column if not exists FG_ATIVO number(1, 0) not null default 1;
