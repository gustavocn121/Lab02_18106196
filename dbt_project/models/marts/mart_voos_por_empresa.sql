{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    id_empresa,
    nm_empresa,
    sg_empresa_iata,
    nm_pais,
    ano,
    trimestre,
    periodo_sazonal,

    COUNT(*)                        AS registros,
    SUM(quantidade_voos)            AS total_voos,
    SUM(passageiros)                AS total_passageiros,
    SUM(carga_kg)                   AS total_carga_kg,
    SUM(horas_voadas)               AS total_horas_voadas,
    ROUND(AVG(cast(distancia_km AS NUMERIC)), 2)  AS media_distancia_km,
    SUM(assentos_ofertados)         AS total_assentos_ofertados,

    CASE
        WHEN SUM(assentos_ofertados) > 0
        THEN ROUND(
            (SUM(passageiros)::numeric / NULLIF(SUM(assentos_ofertados), 0)) * 100,
            2
        )
        ELSE 0
    END AS taxa_ocupacao_pct

FROM {{ ref('stg_voos') }}
GROUP BY
    id_empresa,
    nm_empresa,
    sg_empresa_iata,
    nm_pais,
    ano,
    trimestre,
    periodo_sazonal
