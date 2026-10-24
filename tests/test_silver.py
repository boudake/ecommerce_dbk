import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def test_silver_categories_dedup_and_validation(test_spark):
    """Silver categories: dedup by category_id, drop nulls, cast types."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.bronze_categories (
            category_id STRING,
            category_name STRING,
            created_at STRING,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.bronze_categories VALUES
        ('1', 'Electronics', '2023-10-17', '2024-01-01 00:00:00'),
        ('2', 'Computers', '2022-01-26', '2024-01-01 00:00:00'),
        ('1', 'Electronics', '2023-10-17', '2024-01-01 00:00:00'),
        (NULL, 'Gaming', '2023-11-24', '2024-01-01 00:00:00'),
        ('3', NULL, '2022-09-08', '2024-01-01 00:00:00')
    """)

    status = test_pipeline.run(test_spark, set(["silver_categories"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM workspace.default.silver_categories ORDER BY category_id"
    )
    rows = result.collect()

    # Dedup by category_id removes duplicate of (1, Electronics)
    # Null category_id and null category_name are dropped by expectations
    assert len(rows) == 2
    assert {r.category_id for r in rows} == {1, 2}
    assert rows[0].category_name == "Electronics"
    assert rows[1].category_name == "Computers"

    # Verify type casting
    schema = result.schema
    assert schema["category_id"].dataType.typeName() == "integer"
    assert schema["category_name"].dataType.typeName() == "string"
    assert schema["created_at"].dataType.typeName() == "date"


def test_silver_customers_dedup_and_validation(test_spark):
    """Silver customers: dedup by customer_id, drop null IDs and invalid emails, cast types."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.bronze_customers (
            customer_id STRING,
            first_name STRING,
            last_name STRING,
            email STRING,
            country STRING,
            created_at STRING,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.bronze_customers VALUES
        ('1', 'Michael', 'Scott', 'michael@example.com', 'Netherlands', '2024-04-09', '2024-01-01 00:00:00'),
        ('2', 'Daniel', 'Smith', 'daniel@example.com', 'Germany', '2024-02-14', '2024-01-01 00:00:00'),
        ('1', 'Michael', 'Scott', 'michael@example.com', 'Netherlands', '2024-04-09', '2024-01-01 00:00:00'),
        (NULL, 'James', 'Bond', 'james@example.com', 'Italy', '2024-03-12', '2024-01-01 00:00:00'),
        ('3', 'Lucas', 'Brown', 'invalid-email', 'Germany', '2026-05-25', '2024-01-01 00:00:00')
    """)

    status = test_pipeline.run(test_spark, set(["silver_customers"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM workspace.default.silver_customers ORDER BY customer_id"
    )
    rows = result.collect()

    # Dedup by customer_id removes duplicate of customer 1
    # Null customer_id and invalid email (no '@') are dropped by expectations
    assert len(rows) == 2
    assert {r.customer_id for r in rows} == {1, 2}
    assert all("@" in r.email for r in rows)

    schema = result.schema
    assert schema["customer_id"].dataType.typeName() == "integer"
    assert schema["email"].dataType.typeName() == "string"
    assert schema["created_at"].dataType.typeName() == "date"


def test_silver_orders_dedup_and_validation(test_spark):
    """Silver orders: dedup by order_id, drop invalid records, cast types."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.bronze_orders (
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            order_date STRING,
            quantity STRING,
            unit_price STRING,
            total_amount STRING,
            status STRING,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.bronze_orders VALUES
        ('1', '100', '200', '2024-01-15', '2', '2303.05', '4606.10', 'SHIPPED', '2024-01-01 00:00:00'),
        ('2', '101', '201', '2024-02-20', '3', '1608.30', '4824.90', 'COMPLETED', '2024-01-01 00:00:00'),
        ('1', '100', '200', '2024-01-15', '2', '2303.05', '4606.10', 'SHIPPED', '2024-01-01 00:00:00'),
        (NULL, '102', '202', '2024-03-10', '1', '79.27', '79.27', 'COMPLETED', '2024-01-01 00:00:00'),
        ('3', NULL, '203', '2024-03-15', '1', '32.71', '32.71', 'CANCELLED', '2024-01-01 00:00:00'),
        ('4', '103', NULL, '2024-04-01', '1', '50.00', '50.00', 'COMPLETED', '2024-01-01 00:00:00'),
        ('5', '104', '204', '2024-04-10', '0', '100.00', '0.00', 'COMPLETED', '2024-01-01 00:00:00'),
        ('6', '105', '205', '2024-04-15', '1', '100.00', '-50.00', 'COMPLETED', '2024-01-01 00:00:00')
    """)

    status = test_pipeline.run(test_spark, set(["silver_orders"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM workspace.default.silver_orders ORDER BY order_id"
    )
    rows = result.collect()

    # Valid rows: order_id 1, 2 (dedup removes duplicate of 1)
    # Dropped: null order_id, null customer_id, null product_id, zero quantity, negative total_amount
    assert len(rows) == 2
    assert {r.order_id for r in rows} == {1, 2}
    assert all(r.quantity > 0 for r in rows)
    assert all(r.total_amount >= 0 for r in rows)

    schema = result.schema
    assert schema["order_id"].dataType.typeName() == "integer"
    assert schema["quantity"].dataType.typeName() == "integer"
    assert schema["unit_price"].dataType.typeName().startswith("decimal")
    assert schema["total_amount"].dataType.typeName().startswith("decimal")
    assert schema["order_date"].dataType.typeName() == "date"


def test_silver_products_dedup_and_validation(test_spark):
    """Silver products: dedup by product_id, drop invalid records, cast types."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.bronze_products (
            product_id STRING,
            product_name STRING,
            category_id STRING,
            brand STRING,
            price STRING,
            stock_quantity STRING,
            created_at STRING,
            is_active STRING,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.bronze_products VALUES
        ('1', 'Apple Home Product 1', '1', 'Apple', '299.99', '100', '2023-10-20', 'True', '2024-01-01 00:00:00'),
        ('2', 'Bosch Kitchen Product 2', '2', 'Bosch', '199.99', '50', '2024-11-04', 'True', '2024-01-01 00:00:00'),
        ('1', 'Apple Home Product 1', '1', 'Apple', '299.99', '100', '2023-10-20', 'True', '2024-01-01 00:00:00'),
        (NULL, 'Logitech Product', '3', 'Logitech', '49.99', '200', '2024-09-01', 'True', '2024-01-01 00:00:00'),
        ('3', NULL, '3', 'Sony', '99.99', '75', '2024-01-03', 'True', '2024-01-01 00:00:00'),
        ('4', 'Dell Product', '4', 'Dell', '-10.00', '30', '2024-01-03', 'False', '2024-01-01 00:00:00')
    """)

    status = test_pipeline.run(test_spark, set(["silver_products"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql(
        "SELECT * FROM workspace.default.silver_products ORDER BY product_id"
    )
    rows = result.collect()

    # Valid rows: product_id 1, 2 (dedup removes duplicate of 1)
    # Dropped: null product_id, null product_name, negative price
    assert len(rows) == 2
    assert {r.product_id for r in rows} == {1, 2}
    assert all(r.price >= 0 for r in rows)

    schema = result.schema
    assert schema["product_id"].dataType.typeName() == "integer"
    assert schema["price"].dataType.typeName().startswith("decimal")
    assert schema["is_active"].dataType.typeName() == "boolean"
    assert schema["created_at"].dataType.typeName() == "date"
