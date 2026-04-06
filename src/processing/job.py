import logging
import os
from pathlib import Path

import polars as pl

from src.processing.report import run as run_report
from src.processing.schema import SCHEMA
from src.processing.visualization import run as run_visualization

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def clean_data(df: pl.LazyFrame) -> pl.LazyFrame:
    logging.info("Cleaning data...")

    return df.with_columns(
        pl.col(
            [
                "nr_horas_voadas",
                "nr_velocidade_media",
                "nr_carga_paga_km",
                "nr_bagagem_gratis_km",
                "nr_assentos_ofertados",
                "kg_payload",
                "kg_carga_paga",
                "km_distancia",
            ]
        )
        .cast(pl.Utf8)
        .str.replace(",", ".")
        .cast(pl.Float16),
        pl.col("nr_decolagem")
        .cast(pl.Utf8)
        .str.replace(",", ".")
        .cast(pl.Float16)
        .cast(pl.Int8),
        pl.col("id_empresa").cast(pl.Int32),
        pl.col("id_aerodromo_origem").cast(pl.Int16),
        pl.col("id_aerodromo_destino").cast(pl.Int16),
        pl.col("dt_referencia")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, strict=False),
        *[
            pl.col(c).alias(c.replace("nr", "nm"))
            for c in [
                "nr_ano_mes_referencia",
                "nr_chave",
                "nr_singular",
            ]
        ],
    ).drop_nulls(subset=["id_basica", "id_empresa"])


def export_data(df, output_path, partition_by=None) -> None:
    output_path = Path(output_path)
    logging.info(f"Exporting data to `{output_path.absolute()}`...")
    Path(output_path).mkdir(parents=True, exist_ok=True)
    df.collect(streaming=True).write_parquet(
        output_path,
        compression="zstd",
        partition_by=partition_by,
    )


def run(config: dict):
    logging.info("Starting data processing job...")
    raw_path = config["raw"]["path"]
    silver_path = config["silver"]["path"]

    df = pl.scan_csv(
        f"{raw_path}/*.csv",
        dtypes=SCHEMA,
        infer_schema_length=0,
        ignore_errors=False,
        low_memory=True,
    )
    df = df.collect(streaming=True)
    df = clean_data(df)

    export_data(df.lazy(), silver_path + "data", partition_by="dt_referencia")
    run_report(df.lazy(), config)
    run_visualization(df.lazy(), config)
