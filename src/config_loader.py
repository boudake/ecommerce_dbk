import tomllib
from pathlib import Path


def load_config(config_path: Path, environment: str) -> dict:
    #
    with config_path.open("rb") as file:
        all_config = tomllib.load(file)

    if environment not in all_config:
        raise ValueError(
            f"Unknown environment: {environment}"
        )

    return all_config[environment]