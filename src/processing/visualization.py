import logging
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl

FIG_SIZE = (10, 6)


def _save(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_voos_por_empresa(df: pl.LazyFrame, output_path: Path):
    logging.info("Plotting plot_voos_por_empresa...")

    data = (
        df.group_by("nm_empresa")
        .len()
        .sort("len", descending=True)
        .limit(10)
        .collect()
    )

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.barh(data["nm_empresa"][::-1], data["len"][::-1])

    ax.set_title("Top 10 Empresas por Volume de Voos")
    ax.set_xlabel("Quantidade de Voos")
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    _save(fig, output_path / "top_airlines.png")


def plot_voos_por_mes(df: pl.LazyFrame, output_path: Path):
    logging.info("Plotting plot_voos_por_mes...")

    data = (
        df.with_columns(pl.col("nr_mes_referencia").cast(pl.Int32))
        .group_by("nr_mes_referencia")
        .len()
        .sort("nr_mes_referencia")
        .collect()
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(data["nr_mes_referencia"], data["len"])

    ax.set_title("Volume de Voos por Mês")
    ax.set_xlabel("Mês")
    ax.set_ylabel("Quantidade de Voos")

    fig.tight_layout()
    fig.savefig(output_path / "flights_per_month.png")
    plt.close(fig)


def plot_voos_por_ano(df: pl.LazyFrame, output_path: Path):
    logging.info("Plotting plot_voos_por_ano...")

    data = (
        df.with_columns(pl.col("nr_ano_referencia").cast(pl.Int32))
        .group_by("nr_ano_referencia")
        .len()
        .sort("nr_ano_referencia")
        .collect()
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(data["nr_ano_referencia"], data["len"])

    ax.set_title("Volume de Voos por Ano")
    ax.set_xlabel("Ano")
    ax.set_ylabel("Quantidade de Voos")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    fig.savefig(output_path / "flights_per_year.png", dpi=120)
    plt.close(fig)


def plot_top_paises(df: pl.LazyFrame, output_path: Path):
    logging.info("Plotting plot_top_paises...")

    data = (
        df.group_by("nm_pais")
        .len()
        .sort("len", descending=True)
        .limit(10)
        .collect()
    )

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.barh(data["nm_pais"][::-1], data["len"][::-1])

    ax.set_title("Top 10 Países")
    ax.set_xlabel("Quantidade de Voos")
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    _save(fig, output_path / "flights_by_country.png")


def plot_top_rotas(df: pl.LazyFrame, output_path: Path):
    logging.info("Plotting plot_top_rotas...")

    data = (
        df.with_columns(
            (
                pl.col("nm_municipio_origem")
                + " -> "
                + pl.col("nm_municipio_destino")
            ).alias("rota")
        )
        .group_by("rota")
        .len()
        .sort("len", descending=True)
        .limit(10)
        .collect()
    )

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.barh(data["rota"][::-1], data["len"][::-1])

    ax.set_title("Top 10 Rotas Mais Frequentes")
    ax.set_xlabel("Quantidade de Voos")
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    _save(fig, output_path / "top_routes.png")


def run(df: pl.LazyFrame, config: dict):
    output_path = Path(config["visualization"]["output_path"])
    output_path.mkdir(parents=True, exist_ok=True)

    plot_voos_por_empresa(df, output_path)
    plot_voos_por_mes(df, output_path)
    plot_voos_por_ano(df, output_path)
    plot_top_paises(df, output_path)
    plot_top_rotas(df, output_path)
