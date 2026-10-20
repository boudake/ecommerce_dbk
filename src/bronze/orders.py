from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from config_loader import load_config


spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("No active Spark session available")

config = load_config(spark)
orders_path = config["orders_source"]


catalog = spark.conf.get("ecommerce.catalog")
schema = spark.conf.get("ecommerce.bronze.schema")
@dp.table(name=f"{catalog}.{schema}.orders_stream")
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(orders_path)
    )
    
