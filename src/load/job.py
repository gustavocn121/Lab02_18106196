import io
import logging
from pathlib import Path

import polars as pl

from src.utils import get_db_connection

COLUMNS = [
    "id_basica", "id_empresa", "nm_empresa", "sg_empresa_iata", "nm_pais",
    "id_aerodromo_origem", "nm_aerodromo_origem", "nm_municipio_origem",
    "sg_uf_origem", "nm_regiao_origem",
    "id_aerodromo_destino", "nm_aerodromo_destino", "nm_municipio_destino",
    "sg_uf_destino", "nm_regiao_destino",
    "dt_referencia", "nr_ano_referencia", "nr_trimestre_referencia",
    "nr_mes_referencia", "nr_decolagem", "nr_passag_pagos",
    "kg_carga_paga", "nr_horas_voadas", "km_distancia", "nr_assentos_ofertados",
]


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([
        pl.col("id_basica").cast(pl.Utf8),
        pl.col("id_empresa").cast(pl.Int32),
        pl.col("nm_empresa").cast(pl.Utf8),
        pl.col("sg_empresa_iata").cast(pl.Utf8),
        pl.col("nm_pais").cast(pl.Utf8),
        pl.col("id_aerodromo_origem").cast(pl.Int32),
        pl.col("nm_aerodromo_origem").cast(pl.Utf8),
        pl.col("nm_municipio_origem").cast(pl.Utf8),
        pl.col("sg_uf_origem").cast(pl.Utf8),
        pl.col("nm_regiao_origem").cast(pl.Utf8),
        pl.col("id_aerodromo_destino").cast(pl.Int32),
        pl.col("nm_aerodromo_destino").cast(pl.Utf8),
        pl.col("nm_municipio_destino").cast(pl.Utf8),
        pl.col("sg_uf_destino").cast(pl.Utf8),
        pl.col("nm_regiao_destino").cast(pl.Utf8),
        pl.col("dt_referencia").cast(pl.Date),
        pl.col("nr_ano_referencia").cast(pl.Utf8).cast(pl.Int32, strict=False),
        pl.col("nr_trimestre_referencia").cast(pl.Int32),
        pl.col("nr_mes_referencia").cast(pl.Int32),
        pl.col("nr_decolagem").cast(pl.Int32),
        pl.col("nr_passag_pagos").cast(pl.Utf8).cast(pl.Float32, strict=False),
        pl.col("kg_carga_paga").cast(pl.Float32),
        pl.col("nr_horas_voadas").cast(pl.Float32),
        pl.col("km_distancia").cast(pl.Float32),
        pl.col("nr_assentos_ofertados").cast(pl.Float32),
    ])


def copy_df(cursor, df: pl.DataFrame) -> None:
    buffer = io.StringIO()
    df.write_csv(buffer, include_header=True)
    buffer.seek(0)
    cols = ",".join(COLUMNS)
    cursor.copy_expert(
        f"COPY silver.anac_voos ({cols}) FROM STDIN WITH CSV HEADER", buffer
    )


def run(config: dict):
    logging.info("Starting silver load job...")

    silver_path = Path(config["silver"]["path"]) / "data/"
    parquet_files = sorted(silver_path.glob("**/*.parquet"))
    total = len(parquet_files)
    logging.info(f"Found {total} parquet partitions in {silver_path.absolute()}")

    cursor, conn = get_db_connection(**config["postgres"])

    try:
        cursor.execute("TRUNCATE silver.anac_voos")

        for i, pf in enumerate(parquet_files, 1):
            df = pl.read_parquet(pf)
            df = transform(df)
            copy_df(cursor, df)
            if i % 500 == 0 or i == total:
                conn.commit()
                logging.info(f"  [{i}/{total}] committed")

        logging.info("Silver load finished.")

    except Exception as e:
        logging.error(f"Error during silver load: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()
