import logging

import psycopg2
import yaml


def get_db_connection(
    host: str, database: str, user: str, password: str, port: int
):
    conn = psycopg2.connect(
        host=host, database=database, user=user, password=password, port=port
    )
    cursor = conn.cursor()
    return cursor, conn


def read_config(config_path: str) -> dict:
    logging.info(f"Reading config from {config_path}...")
    with open(config_path) as f:
        return yaml.safe_load(f)
