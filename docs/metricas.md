# Métricas de Negócio

## 1. Empresa com mais passageiros por ano
```sql
WITH passageiros_por_empresa AS (
    SELECT
        d.ano,
        e.nome_empresa,
        SUM(f.passageiros) AS total_passageiros
    FROM fato_voos f
    JOIN dim_data d ON f.data_id = d.data_id
    JOIN dim_empresa e ON f.empresa_id = e.empresa_id
    GROUP BY d.ano, e.nome_empresa
)
SELECT ano, nome_empresa, total_passageiros
FROM (
    SELECT
        ano,
        nome_empresa,
        total_passageiros,
        ROW_NUMBER() OVER (PARTITION BY ano ORDER BY total_passageiros DESC) AS rn
    FROM passageiros_por_empresa
) t
WHERE rn = 1
ORDER BY ano;
```

## 2. Total de passageiros por região por ano
```sql
SELECT
    d.ano,
    a.regiao,
    SUM(f.passageiros) AS total_passageiros
FROM fato_voos f
JOIN dim_aeroporto a ON f.aeroporto_origem_id = a.aeroporto_id
JOIN dim_data d ON f.data_id = d.data_id
WHERE a.regiao IS NOT NULL
  AND TRIM(a.regiao) != ''
GROUP BY d.ano, a.regiao
ORDER BY d.ano, total_passageiros DESC;
```

## 3. Receita total por trimestre
```sql
SELECT
    d.ano,
    d.trimestre  AS trimestre,
    SUM(f.passageiros) AS total_passageiros
FROM fato_voos f
JOIN dim_data d ON f.data_id = d.data_id
GROUP BY d.ano, d.trimestre
ORDER BY d.ano, trimestre;
```

## 4. Média de passageiros por voo por empresa
```sql
SELECT
    e.nome_empresa,
    SUM(f.passageiros) / NULLIF(SUM(f.quantidade_voos),0) AS media_passageiros_por_voo
FROM fato_voos f
JOIN dim_empresa e ON f.empresa_id = e.empresa_id
GROUP BY e.nome_empresa
ORDER BY media_passageiros_por_voo DESC;
```

## 5. Quantidade de passageiros por região por ano
```sql
SELECT
    a.regiao,
    d.ano,
    SUM(f.passageiros) AS total_passageiros
FROM fato_voos f
JOIN dim_aeroporto a ON f.aeroporto_origem_id = a.aeroporto_id
JOIN dim_data d ON f.data_id = d.data_id
where regiao is not null and trim(regiao) != ''
GROUP BY a.regiao, d.ano
ORDER BY a.regiao, d.ano;
```
