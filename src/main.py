import logging
import os

from src.ingestion.job import run as ingestion_run
from src.load.job import run as load_run
from src.processing.job import run as processing_run
from src.utils import read_config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    config = read_config(f"{BASE_DIR}/config.yaml")
    ingestion_run(config)
    processing_run(config)
    load_run(config)


if __name__ == "__main__":
    main()
