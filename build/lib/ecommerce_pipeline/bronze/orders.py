from pyspark import pipelines as dp

from src.config_loader import load_config


config = load_config()


@dp.table(name="orders_bronze")
def orders_bronze():
    return spark.readStream.table(
        config["orders_source"]
    )