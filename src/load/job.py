import io
import logging
from pathlib import Path

import polars as pl

from src.utils import get_db_connection


def copy_lazyframe(
    cursor, lazy_df: pl.LazyFrame, table: str, columns: list[str]
):

    buffer = io.StringIO()

    lazy_df.collect(streaming=True).write_csv(buffer, include_header=True)

    buffer.seek(0)

    cols = ",".join(columns)
    cursor.copy_expert(
        f"COPY {table} ({cols}) FROM STDIN WITH CSV HEADER", buffer
    )


def load_dim_data(df_lazy, cursor):
    logging.info("Loading dim_data")

    query = df_lazy.select(
        [
            pl.col("dt_referencia").cast(pl.Date).alias("data"),
            pl.col("nr_ano_referencia").alias("ano"),
            pl.col("nr_trimestre_referencia").alias("trimestre"),
            pl.col("nr_mes_referencia").alias("mes"),
        ]
    ).unique()

    copy_lazyframe(
        cursor, query, "dim_data", ["data", "ano", "trimestre", "mes"]
    )


def load_dim_empresa(df_lazy, cursor):
    logging.info("Loading dim_empresa")

    query = df_lazy.select(
        [
            pl.col("id_empresa").alias("empresa_id"),
            pl.col("nm_empresa").alias("nome_empresa"),
            pl.col("sg_empresa_iata").alias("sigla_iata"),
            pl.col("nm_pais").alias("pais"),
        ]
    ).unique(subset=["empresa_id"])

    copy_lazyframe(
        cursor,
        query,
        "dim_empresa",
        ["empresa_id", "nome_empresa", "sigla_iata", "pais"],
    )


def load_dim_aeroporto(df_lazy, cursor):
    logging.info("Loading dim_aeroporto")

    origem = df_lazy.select(
        [
            pl.col("id_aerodromo_origem").alias("aeroporto_id"),
            pl.col("nm_aerodromo_origem").alias("nome"),
            pl.col("nm_municipio_origem").alias("cidade"),
            pl.col("sg_uf_origem").alias("estado"),
            pl.col("nm_regiao_origem").alias("regiao"),
        ]
    )

    destino = df_lazy.select(
        [
            pl.col("id_aerodromo_destino").alias("aeroporto_id"),
            pl.col("nm_aerodromo_destino").alias("nome"),
            pl.col("nm_municipio_destino").alias("cidade"),
            pl.col("sg_uf_destino").alias("estado"),
            pl.col("nm_regiao_destino").alias("regiao"),
        ]
    )

    query = pl.concat([origem, destino])
    query = query.filter(pl.col("aeroporto_id") != 0).unique(
        subset=["aeroporto_id"]
    )

    copy_lazyframe(
        cursor,
        query,
        "dim_aeroporto",
        ["aeroporto_id", "nome", "cidade", "estado", "regiao"],
    )


def load_fato_voos(df_lazy, cursor):
    logging.info("Loading fato_voos")

    dim_data = pl.read_database(
        "SELECT data_id, data FROM dim_data", cursor.connection
    ).lazy()

    query = (
        df_lazy.join(
            dim_data, left_on="dt_referencia", right_on="data", how="left"
        )
        .select(
            [
                "data_id",
                pl.col("id_empresa").alias("empresa_id"),
                pl.col("id_aerodromo_origem").alias("aeroporto_origem_id"),
                pl.col("id_aerodromo_destino").alias("aeroporto_destino_id"),
                pl.col("nr_decolagem").alias("quantidade_voos"),
                pl.col("nr_passag_pagos")
                .cast(pl.Float16)
                .cast(pl.Int16)
                .alias("passageiros"),
                pl.col("kg_carga_paga").alias("carga_kg"),
                pl.lit(None).alias("receita"),
            ]
        )
        .unique(
            subset=[
                "data_id",
                "empresa_id",
                "aeroporto_origem_id",
                "aeroporto_destino_id",
            ],
            maintain_order=False,
        )
    )

    copy_lazyframe(
        cursor,
        query,
        "fato_voos",
        [
            "data_id",
            "empresa_id",
            "aeroporto_origem_id",
            "aeroporto_destino_id",
            "quantidade_voos",
            "passageiros",
            "carga_kg",
            "receita",
        ],
    )


def run(config: dict):
    logging.info("Starting data load job...")

    silver_path = Path(config["silver"]["path"]) / "data/"
    logging.info(f"Reading data from silver path: {silver_path.absolute()}")

    df_lazy = pl.scan_parquet(silver_path, low_memory=True, parallel="auto")

    cursor, conn = get_db_connection(**config["postgres"])

    try:
        cursor.execute("BEGIN")

        load_dim_data(df_lazy, cursor)
        load_dim_empresa(df_lazy, cursor)
        load_dim_aeroporto(df_lazy, cursor)
        load_fato_voos(df_lazy, cursor)

        conn.commit()

        logging.info("Gold load finished")

    except Exception as e:
        logging.error(f"Error: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()
