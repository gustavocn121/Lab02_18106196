{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
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

    ano,

    COUNT(*)                              AS registros,
    SUM(quantidade_voos)                  AS total_voos,
    SUM(passageiros)                      AS total_passageiros,
    ROUND(AVG(cast(distancia_km as numeric)), 2)  AS distancia_media_km,
    SUM(carga_kg)                         AS total_carga_kg

FROM {{ ref('stg_voos') }}
WHERE id_aerodromo_origem  IS NOT NULL
  AND id_aerodromo_destino IS NOT NULL
  AND id_aerodromo_origem  <> 0
  AND id_aerodromo_destino <> 0
GROUP BY
    id_aerodromo_origem, nm_aerodromo_origem, nm_municipio_origem,
    sg_uf_origem, nm_regiao_origem,
    id_aerodromo_destino, nm_aerodromo_destino, nm_municipio_destino,
    sg_uf_destino, nm_regiao_destino,
    ano
