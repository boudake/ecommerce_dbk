from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_product_summary",
    comment="Product-level sales summary"
)
def gold_product_summary():
    orders = spark.read.table("silver_orders")
    products = spark.read.table("silver_products")

    return (
        orders
        .join(products, orders.product_id == products.product_id, "left")
        .groupBy(
            products["product_id"],
            products["product_name"],
            products["brand"],
            products["category_id"],
            products["stock_quantity"],
            products["is_active"],
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("quantity").alias("total_units_sold"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .orderBy(F.col("total_revenue").desc())
    )
