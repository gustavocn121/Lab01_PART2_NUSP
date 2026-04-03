import logging

import yaml


def read_config(config_path: str) -> dict:
    logging.info(f"Reading config from {config_path}...")
    with open(config_path) as f:
        return yaml.safe_load(f)
