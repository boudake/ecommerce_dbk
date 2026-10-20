from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("order_id").isNotNull())
          .dropDuplicates(["order_id"])
          .withColumn("order_date", F.expr("try_cast(order_date AS DATE)"))
    )
    