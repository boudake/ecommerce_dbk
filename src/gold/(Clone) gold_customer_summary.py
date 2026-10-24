from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_customer_summary",
    comment="Customer-level order summary"
)
def gold_customer_summary():
    orders = spark.read.table("silver_orders")
    customers = spark.read.table("silver_customers")

    return (
        orders
        .join(customers, orders.customer_id == customers.customer_id, "left")
        .groupBy(
            customers["customer_id"],
            customers["first_name"],
            customers["last_name"],
            customers["country"],
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .orderBy(F.col("total_spend").desc())
    )
