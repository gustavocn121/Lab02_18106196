{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    id_basica,
    id_empresa,
    nm_empresa,
    sg_empresa_iata,
    nm_pais,

    id_aerodromo_origem,
    nm_aerodromo_origem,
    nm_municipio_origem,
    sg_uf_origem,
    nm_regiao_origem,

    id_aerodromo_destino,
    nm_aerodromo_destino,
    nm_municipio_destino,
    sg_uf_destino,
    nm_regiao_destino,

    dt_referencia,
    nr_ano_referencia                          AS ano,
    nr_trimestre_referencia                    AS trimestre,
    nr_mes_referencia                          AS mes,

    COALESCE(nr_decolagem, 0)                  AS quantidade_voos,
    COALESCE(CAST(nr_passag_pagos AS INTEGER), 0) AS passageiros,
    COALESCE(kg_carga_paga, 0)                 AS carga_kg,
    COALESCE(nr_horas_voadas, 0)               AS horas_voadas,
    COALESCE(km_distancia, 0)                  AS distancia_km,
    COALESCE(nr_assentos_ofertados, 0)         AS assentos_ofertados,

    {{ classify_period('nr_trimestre_referencia') }} AS periodo_sazonal

FROM {{ source('silver', 'anac_voos') }}
WHERE id_empresa IS NOT NULL
  AND id_basica  IS NOT NULL
  AND dt_referencia IS NOT NULL
