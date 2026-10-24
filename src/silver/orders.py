from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from silver.clean_order import clean_orders

from pipeline_config import get_pipeline_config

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("No active Spark session available")

config = get_pipeline_config()
catalog = config["catalog"]
schema = config["silver_schema"]
bronze_schema = config["bronze_schema"]


@dp.table(name=f"{catalog}.{schema}.orders")
def orders_bronze():
    return clean_orders(
        spark.read.table(f"{catalog}.{bronze_schema}.orders_stream")
    )
