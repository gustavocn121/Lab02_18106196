import os
from pathlib import Path

import kagglehub
from dotenv import load_dotenv


def load_kaggle_credentials():
    repo_root = Path(__file__).resolve().parents[2]
    dotenv_path = repo_root / ".env"
    load_dotenv(dotenv_path=dotenv_path)

    token_str = os.getenv("KAGGLE_API_TOKEN")
    if token_str is None:
        raise ValueError(f"KAGGLE_API_TOKEN should be set in {dotenv_path}")

    os.environ["KAGGLE_API_TOKEN"] = token_str
    return token_str


def download_dataset(
    dataset: str, output_dir: str, force_download: bool = False
) -> str:
    load_kaggle_credentials()
    return kagglehub.dataset_download(
        dataset, output_dir=output_dir, force_download=force_download
    )
