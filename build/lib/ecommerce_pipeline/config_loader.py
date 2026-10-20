import tomllib
from pathlib import Path
import os

def load_config() -> dict:
    config_path = os.environ.get("ECOMMERCE_CONFIG_PATH")

    if not config_path:
        raise RuntimeError(
            "ECOMMERCE_CONFIG_PATH is not configured"
        )

    with Path(config_path).open("rb") as file:
        all_config = tomllib.load(file)

    environment = spark.conf.get(
        "ecommerce.environment", "dev"
    )

    if environment not in all_config:
        raise ValueError(
            f"Unknown environment: {environment}"
        )

    return all_config[environment]