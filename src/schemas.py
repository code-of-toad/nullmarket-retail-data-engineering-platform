"""Explicit PySpark schemas for NullMarket operational source datasets."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# Grain: one row per order
# Primary key: order_id
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("store_id", StringType(), nullable=False),
        StructField("order_timestamp", TimestampType(), nullable=False),
        StructField("order_status", StringType(), nullable=False),
        StructField("payment_method", StringType(), nullable=False),
        StructField("channel", StringType(), nullable=False),
    ]
)


# Grain: one product line within an order
# Composite primary key: (order_id, line_number)
ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("line_number", IntegerType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(12, 2), nullable=False),
        StructField("discount_amount", DecimalType(12, 2), nullable=False),
    ]
)


# Grain: one row per product
# Primary key: product_id
PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("subcategory", StringType(), nullable=False),
        StructField("brand", StringType(), nullable=False),
        StructField("unit_cost", DecimalType(12, 2), nullable=False),
        StructField("list_price", DecimalType(12, 2), nullable=False),
        StructField("active_flag", BooleanType(), nullable=False),
    ]
)


# Grain: one row per store
# Primary key: store_id
STORES_SCHEMA = StructType(
    [
        StructField("store_id", StringType(), nullable=False),
        StructField("store_name", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("province", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("store_type", StringType(), nullable=False),
        StructField("open_date", DateType(), nullable=False),
    ]
)


# Grain: one product, at one store, on one snapshot date
# Composite primary key: (snapshot_date, store_id, product_id)
INVENTORY_SNAPSHOTS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("store_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity_on_hand", IntegerType(), nullable=False),
        StructField("reorder_level", IntegerType(), nullable=False),
    ]
)


SOURCE_SCHEMAS = {
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "stores": STORES_SCHEMA,
    "inventory_snapshots": INVENTORY_SNAPSHOTS_SCHEMA,
}
