from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_order_summary",
    comment="Monthly order summary by category and status"
)
def gold_order_summary():
    orders = spark.read.table("silver_orders")
    products = spark.read.table("silver_products")
    categories = spark.read.table("silver_categories")

    return (
        orders
        .join(products, orders.product_id == products.product_id, "left")
        .join(categories, products.category_id == categories.category_id, "left")
        .groupBy(
            F.date_format("order_date", "yyyy-MM").alias("order_month"),
            F.col("category_name"),
            F.col("status"),
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .orderBy("order_month", "category_name", "status")
    )
