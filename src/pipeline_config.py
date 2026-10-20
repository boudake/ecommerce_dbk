from pathlib import Path

from pyspark.sql import SparkSession

from config_loader import load_config


def get_pipeline_config() -> dict:
    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError("No active Spark session available")

    config_path = Path(
        spark.conf.get("ecommerce.config_path")
    )
    environment = spark.conf.get(
        "app.environment", "dev"
    )

    return load_config(config_path, environment)