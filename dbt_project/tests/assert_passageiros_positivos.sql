SELECT
    id_basica,
    id_empresa,
    dt_referencia,
    passageiros
FROM {{ ref('stg_voos') }}
WHERE passageiros < 0
