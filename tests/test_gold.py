import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def _seed_silver_tables_for_gold(test_spark):
    """Create mock silver tables with known data for gold layer tests."""
    # Mock silver_categories
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.silver_categories (
            category_id INT,
            category_name STRING,
            created_at DATE,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.silver_categories VALUES
        (1, 'Electronics', '2023-10-17', '2024-01-01 00:00:00'),
        (2, 'Computers', '2022-01-26', '2024-01-01 00:00:00')
    """)

    # Mock silver_products
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.silver_products (
            product_id INT,
            product_name STRING,
            category_id INT,
            brand STRING,
            price DECIMAL(10,2),
            stock_quantity INT,
            created_at DATE,
            is_active BOOLEAN,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.silver_products VALUES
        (1, 'Apple Home Product 1', 1, 'Apple', 299.99, 100, '2023-10-20', true, '2024-01-01 00:00:00'),
        (2, 'Bosch Kitchen Product 2', 2, 'Bosch', 199.99, 50, '2024-11-04', true, '2024-01-01 00:00:00')
    """)

    # Mock silver_customers
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.silver_customers (
            customer_id INT,
            first_name STRING,
            last_name STRING,
            email STRING,
            country STRING,
            created_at DATE,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.silver_customers VALUES
        (1, 'Michael', 'Scott', 'michael@example.com', 'Netherlands', '2024-04-09', '2024-01-01 00:00:00'),
        (2, 'Daniel', 'Smith', 'daniel@example.com', 'Germany', '2024-02-14', '2024-01-01 00:00:00')
    """)

    # Mock silver_orders
    test_spark.sql("""
        CREATE OR REPLACE TABLE workspace.default.silver_orders (
            order_id INT,
            customer_id INT,
            product_id INT,
            order_date DATE,
            quantity INT,
            unit_price DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            status STRING,
            _ingested_at TIMESTAMP
        ) USING DELTA
    """)
    test_spark.sql("""
        INSERT INTO workspace.default.silver_orders VALUES
        (1, 1, 1, '2024-01-15', 2, 299.99, 599.98, 'COMPLETED', '2024-01-01 00:00:00'),
        (2, 1, 2, '2024-01-20', 1, 199.99, 199.99, 'SHIPPED', '2024-01-01 00:00:00'),
        (3, 2, 1, '2024-02-10', 3, 299.99, 899.97, 'COMPLETED', '2024-01-01 00:00:00'),
        (4, 2, 2, '2024-02-15', 1, 199.99, 199.99, 'CANCELLED', '2024-01-01 00:00:00')
    """)


def test_gold_order_summary(test_spark):
    """Gold order summary: monthly aggregation by category and status."""
    _seed_silver_tables_for_gold(test_spark)

    status = test_pipeline.run(test_spark, set(["gold_order_summary"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("""
        SELECT * FROM workspace.default.gold_order_summary
        ORDER BY order_month, category_name, status
    """)
    rows = result.collect()

    # Expected 4 groups:
    # (2024-01, Electronics, COMPLETED): order 1, qty=2, revenue=599.98
    # (2024-01, Computers, SHIPPED):     order 2, qty=1, revenue=199.99
    # (2024-02, Electronics, COMPLETED): order 3, qty=3, revenue=899.97
    # (2024-02, Computers, CANCELLED):   order 4, qty=1, revenue=199.99
    assert len(rows) == 4

    assert rows[0].order_month == "2024-01"
    assert rows[0].category_name == "Electronics"
    assert rows[0].status == "COMPLETED"
    assert rows[0].order_count == 1
    assert rows[0].total_quantity == 2
    assert float(rows[0].total_revenue) == pytest.approx(599.98)

    assert rows[2].order_month == "2024-02"
    assert rows[2].category_name == "Electronics"
    assert rows[2].total_quantity == 3
    assert float(rows[2].total_revenue) == pytest.approx(899.97)


def test_gold_customer_summary(test_spark):
    """Gold customer summary: per-customer aggregation."""
    _seed_silver_tables_for_gold(test_spark)

    status = test_pipeline.run(test_spark, set(["gold_customer_summary"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("""
        SELECT * FROM workspace.default.gold_customer_summary
        ORDER BY customer_id
    """)
    rows = result.collect()

    # Customer 1: 2 orders, total_spend = 599.98 + 199.99 = 799.97
    # Customer 2: 2 orders, total_spend = 899.97 + 199.99 = 1099.96
    assert len(rows) == 2

    assert rows[0].customer_id == 1
    assert rows[0].order_count == 2
    assert float(rows[0].total_spend) == pytest.approx(799.97)
    assert str(rows[0].first_order_date) == "2024-01-15"
    assert str(rows[0].last_order_date) == "2024-01-20"

    assert rows[1].customer_id == 2
    assert rows[1].order_count == 2
    assert float(rows[1].total_spend) == pytest.approx(1099.96)
    assert str(rows[1].first_order_date) == "2024-02-10"


def test_gold_product_summary(test_spark):
    """Gold product summary: per-product aggregation."""
    _seed_silver_tables_for_gold(test_spark)

    status = test_pipeline.run(test_spark, set(["gold_product_summary"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("""
        SELECT * FROM workspace.default.gold_product_summary
        ORDER BY product_id
    """)
    rows = result.collect()

    # Product 1: orders 1 (qty 2) + 3 (qty 3) = 5 units, revenue = 599.98 + 899.97 = 1499.95
    # Product 2: orders 2 (qty 1) + 4 (qty 1) = 2 units, revenue = 199.99 + 199.99 = 399.98
    assert len(rows) == 2

    assert rows[0].product_id == 1
    assert rows[0].order_count == 2
    assert rows[0].total_units_sold == 5
    assert float(rows[0].total_revenue) == pytest.approx(1499.95)

    assert rows[1].product_id == 2
    assert rows[1].order_count == 2
    assert rows[1].total_units_sold == 2
    assert float(rows[1].total_revenue) == pytest.approx(399.98)
