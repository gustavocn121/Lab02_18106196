import logging
from datetime import datetime
from pathlib import Path

import polars as pl
from deltalake import write_deltalake


def generate_report(df: pl.LazyFrame) -> pl.DataFrame:
    logging.info("Generating profiling report...")

    numeric_types = (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.Float32,
        pl.Float64,
    )

    numeric_cols = [
        col
        for col, dtype in df.collect_schema().items()
        if dtype in numeric_types
    ]

    metrics = [
        pl.len().alias("total_rows"),
        *[
            pl.col(c).null_count().alias(f"{c}_nulls")
            for c in df.collect_schema().names()
        ],
        *[pl.col(c).mean().alias(f"{c}_mean") for c in numeric_cols],
        *[pl.col(c).std().alias(f"{c}_stddev") for c in numeric_cols],
    ]
    report = (
        df.select(metrics)
        .with_columns(pl.lit(datetime.now()).alias("run_ts"))
        .with_columns(pl.col("run_ts").dt.date().alias("run_date"))
        .collect()
    )
    return report


def export_report(report: pl.DataFrame, output_path: str) -> None:
    output = Path(output_path)
    output.mkdir(parents=True, exist_ok=True)

    write_deltalake(output, report.to_arrow(), mode="append")


def run(df_input: pl.LazyFrame, config: dict):
    logging.info("Running report")
    report = generate_report(
        df_input,
    )
    export_report(report, config["silver"]["path"] + "/report")
