"""Behavioral tests for NullMarket Phase 9 transformation functions."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.schemas import INVENTORY_SNAPSHOTS_SCHEMA, ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA
from src.transformations import (
    build_daily_revenue_trend,
    build_dim_date,
    build_dim_product,
    build_dim_store,
    build_fact_inventory_snapshot,
    build_fact_sales,
    build_inventory_dataset,
    build_latest_inventory,
    build_product_rankings,
    build_sales_dataset,
)


def _sales_order_items(make_df):
    """Create three valid lines with exact expected sales arithmetic."""

    return make_df(
        ORDER_ITEMS_SCHEMA,
        [
            {
                "order_id": "O000001",
                "line_number": 1,
                "product_id": "P00001",
                "quantity": 2,
                "unit_price": Decimal("10.00"),
                "discount_amount": Decimal("1.00"),
            },
            {
                "order_id": "O000001",
                "line_number": 2,
                "product_id": "P00002",
                "quantity": 1,
                "unit_price": Decimal("5.00"),
                "discount_amount": Decimal("0.00"),
            },
            {
                "order_id": "O000002",
                "line_number": 1,
                "product_id": "P00001",
                "quantity": 3,
                "unit_price": Decimal("10.00"),
                "discount_amount": Decimal("2.00"),
            },
        ],
    )


def _inventory_snapshots(make_df):
    """Create deterministic inventory rows at the documented snapshot grain."""

    return make_df(
        INVENTORY_SNAPSHOTS_SCHEMA,
        [
            {
                "snapshot_date": date(2026, 1, 1),
                "store_id": "S0001",
                "product_id": "P00001",
                "quantity_on_hand": 3,
                "reorder_level": 5,
            },
            {
                "snapshot_date": date(2026, 1, 3),
                "store_id": "S0002",
                "product_id": "P00002",
                "quantity_on_hand": 10,
                "reorder_level": 5,
            },
        ],
    )


def test_sales_join_preserves_order_line_grain_and_calculates_metrics(
    make_df,
    valid_orders,
    valid_products,
    valid_stores,
) -> None:
    """Many-to-one enrichment must not multiply rows or change sales formulas."""

    order_items = _sales_order_items(make_df)

    sales = build_sales_dataset(
        valid_orders,
        order_items,
        valid_products,
        valid_stores,
    )

    # One input order line must produce exactly one enriched sales row.
    assert sales.count() == order_items.count() == 3
    duplicate_keys = (
        sales.groupBy("order_id", "line_number")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    assert duplicate_keys == 0

    # Index results by the business grain instead of trusting collect() order.
    actual = {
        (row.order_id, row.line_number): (
            row.gross_sales,
            row.net_sales,
            row.gross_margin,
        )
        for row in sales.select(
            "order_id",
            "line_number",
            "gross_sales",
            "net_sales",
            "gross_margin",
        ).collect()
    }

    assert actual == {
        ("O000001", 1): (
            Decimal("20.00"),
            Decimal("19.00"),
            Decimal("11.00"),
        ),
        ("O000001", 2): (
            Decimal("5.00"),
            Decimal("5.00"),
            Decimal("3.00"),
        ),
        ("O000002", 1): (
            Decimal("30.00"),
            Decimal("28.00"),
            Decimal("16.00"),
        ),
    }


def test_fact_sales_preserves_row_count_and_maps_dimension_keys(
    make_df,
    valid_orders,
    valid_products,
    valid_stores,
) -> None:
    """Warehouse-key lookups must remain many-to-one at order-line grain."""

    order_items = _sales_order_items(make_df)
    inventory = _inventory_snapshots(make_df)

    dim_product = build_dim_product(valid_products)
    dim_store = build_dim_store(valid_stores)
    dim_date = build_dim_date(valid_orders, inventory)
    sales = build_sales_dataset(valid_orders, order_items, valid_products, valid_stores)
    fact_sales = build_fact_sales(sales, dim_date, dim_product, dim_store)

    assert fact_sales.count() == order_items.count() == 3
    assert (
        fact_sales.groupBy("order_id", "line_number")
        .count()
        .filter(F.col("count") > 1)
        .count()
        == 0
    )
    # Every fact row must successfully resolve all three conformed dimensions.
    assert (
        fact_sales.filter(
            F.col("date_key").isNull()
            | F.col("store_key").isNull()
            | F.col("product_key").isNull()
        ).count()
        == 0
    )


def test_inventory_fact_preserves_snapshot_grain_and_low_stock_logic(
    make_df,
    valid_orders,
    valid_products,
    valid_stores,
) -> None:
    """Inventory remains a separate date/store/product snapshot fact."""

    snapshots = _inventory_snapshots(make_df)
    dim_product = build_dim_product(valid_products)
    dim_store = build_dim_store(valid_stores)
    dim_date = build_dim_date(valid_orders, snapshots)

    inventory = build_inventory_dataset(snapshots, valid_products, valid_stores)
    fact_inventory = build_fact_inventory_snapshot(
        inventory,
        dim_date,
        dim_product,
        dim_store,
    )

    assert fact_inventory.count() == snapshots.count() == 2
    assert (
        fact_inventory.groupBy("date_key", "store_key", "product_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
        == 0
    )

    # Compare key-independent business values as a set; Spark row order is not
    # part of the data contract.
    actual = {
        (row.quantity_on_hand, row.reorder_level, row.is_low_stock)
        for row in fact_inventory.select(
            "quantity_on_hand",
            "reorder_level",
            "is_low_stock",
        ).collect()
    }
    assert actual == {(3, 5, True), (10, 5, False)}


def test_product_rankings_handle_ties_with_expected_window_semantics(spark) -> None:
    """row_number, rank, and dense_rank must differ correctly when revenue ties."""

    fact_schema = StructType(
        [
            StructField("product_key", IntegerType(), False),
            StructField("net_sales", DecimalType(18, 2), False),
        ]
    )
    fact_sales = spark.createDataFrame(
        [
            (1, Decimal("100.00")),
            (2, Decimal("100.00")),
            (3, Decimal("50.00")),
        ],
        schema=fact_schema,
    )
    dim_product = spark.createDataFrame(
        [
            (1, "P00001", "A"),
            (2, "P00002", "B"),
            (3, "P00003", "C"),
        ],
        ["product_key", "product_id", "product_name"],
    )

    rankings = build_product_rankings(fact_sales, dim_product)
    actual = {
        row.product_id: (row.row_number, row.rank, row.dense_rank)
        for row in rankings.select(
            "product_id",
            "row_number",
            "rank",
            "dense_rank",
        ).collect()
    }

    # row_number uses product_id as a deterministic tiebreaker; rank/dense_rank
    # intentionally preserve the revenue tie.
    assert actual == {
        "P00001": (1, 1, 1),
        "P00002": (2, 1, 1),
        "P00003": (3, 3, 2),
    }


def test_daily_revenue_trend_uses_calendar_days_for_rolling_and_lag(spark) -> None:
    """A zero-sales calendar day must remain inside rolling and lag windows."""

    fact_sales = spark.createDataFrame(
        [
            (20260101, Decimal("10.00")),
            (20260103, Decimal("30.00")),
        ],
        StructType(
            [
                StructField("date_key", IntegerType(), False),
                StructField("net_sales", DecimalType(18, 2), False),
            ]
        ),
    )
    dim_date = spark.createDataFrame(
        [
            (20260101, date(2026, 1, 1)),
            (20260102, date(2026, 1, 2)),
            (20260103, date(2026, 1, 3)),
        ],
        StructType(
            [
                StructField("date_key", IntegerType(), False),
                StructField("full_date", DateType(), False),
            ]
        ),
    )

    trend = build_daily_revenue_trend(fact_sales, dim_date)
    actual = {
        row.full_date: (
            row.daily_net_sales,
            row.rolling_7_day_net_sales,
            row.previous_day_net_sales,
            row.day_over_day_change,
        )
        for row in trend.select(
            "full_date",
            "daily_net_sales",
            "rolling_7_day_net_sales",
            "previous_day_net_sales",
            "day_over_day_change",
        ).collect()
    }

    assert actual == {
        date(2026, 1, 1): (
            Decimal("10.00"),
            Decimal("10.00"),
            None,
            None,
        ),
        date(2026, 1, 2): (
            Decimal("0.00"),
            Decimal("10.00"),
            Decimal("10.00"),
            Decimal("-10.00"),
        ),
        date(2026, 1, 3): (
            Decimal("30.00"),
            Decimal("40.00"),
            Decimal("0.00"),
            Decimal("30.00"),
        ),
    }


def test_latest_inventory_selects_newest_snapshot_per_store_product(spark) -> None:
    """Latest-record selection must partition by store/product and keep the newest date."""

    inventory = spark.createDataFrame(
        [
            (date(2026, 1, 1), "S0001", "P00001", 5),
            (date(2026, 1, 15), "S0001", "P00001", 9),
            (date(2026, 1, 8), "S0002", "P00001", 7),
        ],
        StructType(
            [
                StructField("snapshot_date", DateType(), False),
                StructField("store_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity_on_hand", IntegerType(), False),
            ]
        ),
    )

    latest = build_latest_inventory(inventory)
    actual = {
        (row.store_id, row.product_id): (row.snapshot_date, row.quantity_on_hand)
        for row in latest.collect()
    }

    assert actual == {
        ("S0001", "P00001"): (date(2026, 1, 15), 9),
        ("S0002", "P00001"): (date(2026, 1, 8), 7),
    }


def test_dim_date_deduplicates_observed_dates_and_builds_continuous_calendar(
    make_df,
) -> None:
    """Repeated observed dates should still produce one calendar row per date."""

    orders = make_df(
        ORDERS_SCHEMA,
        [
            {
                "order_id": "O100001",
                "customer_id": "C00001",
                "store_id": "S0001",
                "order_timestamp": datetime(2026, 1, 1, 9, 0, 0),
                "order_status": "Completed",
                "payment_method": "Credit Card",
                "channel": "Store",
            },
            {
                "order_id": "O100002",
                "customer_id": "C00002",
                "store_id": "S0001",
                # A second order on the same date is expected source behavior,
                # not a duplicate calendar row in dim_date.
                "order_timestamp": datetime(2026, 1, 1, 12, 0, 0),
                "order_status": "Completed",
                "payment_method": "Debit Card",
                "channel": "Online",
            },
        ],
    )
    inventory = make_df(
        INVENTORY_SNAPSHOTS_SCHEMA,
        [
            {
                "snapshot_date": date(2026, 1, 3),
                "store_id": "S0001",
                "product_id": "P00001",
                "quantity_on_hand": 5,
                "reorder_level": 2,
            }
        ],
    )

    dim_date = build_dim_date(orders, inventory)

    dates = {row.full_date for row in dim_date.select("full_date").collect()}
    assert dates == {
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
    }
    assert dim_date.count() == 3
    assert dim_date.select("full_date").distinct().count() == 3
