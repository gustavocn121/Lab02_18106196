SELECT
    id_aerodromo_origem,
    id_aerodromo_destino,
    ano,
    total_voos,
    distancia_media_km
FROM {{ ref('mart_voos_por_rota') }}
WHERE total_voos <= 0
   OR distancia_media_km <= 0
