CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.anac_voos (
    id_basica               TEXT,
    id_empresa              INTEGER,
    nm_empresa              TEXT,
    sg_empresa_iata         TEXT,
    nm_pais                 TEXT,
    id_aerodromo_origem     INTEGER,
    nm_aerodromo_origem     TEXT,
    nm_municipio_origem     TEXT,
    sg_uf_origem            TEXT,
    nm_regiao_origem        TEXT,
    id_aerodromo_destino    INTEGER,
    nm_aerodromo_destino    TEXT,
    nm_municipio_destino    TEXT,
    sg_uf_destino           TEXT,
    nm_regiao_destino       TEXT,
    dt_referencia           DATE,
    nr_ano_referencia       INTEGER,
    nr_trimestre_referencia INTEGER,
    nr_mes_referencia       INTEGER,
    nr_decolagem            INTEGER,
    nr_passag_pagos         FLOAT,
    kg_carga_paga           FLOAT,
    nr_horas_voadas         FLOAT,
    km_distancia            FLOAT,
    nr_assentos_ofertados   FLOAT
);
