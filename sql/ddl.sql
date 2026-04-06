CREATE TABLE dim_data (
    data_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data DATE,
    ano INTEGER,
    trimestre INTEGER,
    mes INTEGER
);

CREATE TABLE dim_aeroporto (
    aeroporto_id INT PRIMARY KEY,
    nome TEXT,
    cidade TEXT,
    estado TEXT,
    regiao TEXT
);

CREATE TABLE dim_empresa (
    empresa_id INT PRIMARY KEY,
    nome_empresa TEXT,
    sigla_iata TEXT,
    pais TEXT
);

CREATE TABLE fato_voos (
    id SERIAL PRIMARY KEY,
    data_id INT,
    empresa_id INT,
    aeroporto_origem_id INT,
    aeroporto_destino_id INT,
    quantidade_voos INT,
    passageiros INT,
    carga_kg TEXT,
    receita NUMERIC
);
