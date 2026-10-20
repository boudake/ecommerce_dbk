from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date
from pyspark.sql import SparkSession

from pipeline_config import get_pipeline_config

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("No active Spark session available")

config = get_pipeline_config()
catalog = config["catalog"]
schema = config["silver_schema"]
bronze_schema = config["bronze_schema"]

orders_path = config["orders_source"]


@dp.table(name=f"{catalog}.{schema}.orders")
def orders_bronze():
  return (
        spark.read.table(f"{catalog}.{bronze_schema}.orders_stream")
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
        .withColumn("order_date", to_date(col("order_date")))
    )
     