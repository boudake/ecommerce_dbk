from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from pipeline_config import get_pipeline_config

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("No active Spark session available")

config = get_pipeline_config()


orders_path = config["orders_source"]


@dp.table(name=f"{config['catalog']}.{config['bronze_schema']}.orders_stream")
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(orders_path)
    )
    
