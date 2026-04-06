import logging
from pathlib import Path

from src.ingestion.kaggle_client import download_dataset


def run(config: dict):
    logging.info("Starting dataset ingestion...")
    dataset = config["kaggle"]["dataset"]
    raw_path = config["raw"]["path"]
    force_download = config["kaggle"]["force_download"]

    try:
        Path(raw_path).mkdir(parents=True, exist_ok=True)
        dataset_path = download_dataset(
            dataset, output_dir=raw_path, force_download=force_download
        )
        logging.info(f"Downloaded dataset to: `{dataset_path}`")
    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        raise
