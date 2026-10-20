import tomllib
from pathlib import Path


import tomllib
from pathlib import Path


def load_config(spark) -> dict:
    #
    config_path = Path(
        spark.conf.get("ecommerce.config_path")
    )
    with config_path.open("rb") as file:
        all_config = tomllib.load(file)

    environment = spark.conf.get(
        "app.environment", "dev"
    )

    if environment not in all_config:
        raise ValueError(
            f"Unknown environment: {environment}"
        )

    return all_config[environment]