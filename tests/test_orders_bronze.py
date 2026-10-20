# tests/test_orders_bronze.py
import datetime as dt
from src.silver.clean_order import clean_orders

SCHEMA = "order_id string, customer string, order_date string"

def test_removes_null_order_ids(spark):
    df = spark.createDataFrame(
        [("1", "A", "2026-01-01"), (None, "B", "2026-01-02")], SCHEMA
    )
    out = clean_orders(df)
    assert [r.order_id for r in out.collect()] == ["1"]

def test_deduplicates_on_order_id(spark):
    df = spark.createDataFrame(
        [("1", "A", "2026-01-01"), ("1", "A", "2026-01-01"), ("2", "B", "2026-01-02")],
        SCHEMA,
    )
    out = clean_orders(df)
    assert out.count() == 2
    assert out.select("order_id").distinct().count() == 2

def test_order_date_is_cast_to_date(spark):
    df = spark.createDataFrame([("1", "A", "2026-03-15")], SCHEMA)
    out = clean_orders(df)
    assert dict(out.dtypes)["order_date"] == "date"
    assert out.collect()[0].order_date == dt.date(2026, 3, 15)

def test_invalid_date_becomes_null(spark):
    df = spark.createDataFrame([("1", "A", "not-a-date")], SCHEMA)
    out = clean_orders(df)
    assert out.collect()[0].order_date is None

def test_empty_input_returns_empty(spark):
    df = spark.createDataFrame([], SCHEMA)
    assert clean_orders(df).count() == 0

def test_schema_is_preserved(spark):
    df = spark.createDataFrame([("1", "A", "2026-01-01")], SCHEMA)
    assert clean_orders(df).columns == ["order_id", "customer", "order_date"]