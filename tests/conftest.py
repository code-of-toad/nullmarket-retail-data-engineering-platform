"""Shared pytest fixtures for NullMarket Phase 12 Spark tests."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Callable, Iterable

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from src.schemas import ORDERS_SCHEMA, PRODUCTS_SCHEMA, STORES_SCHEMA


@pytest.fixture(scope="session")
def spark() -> Iterable[SparkSession]:
    """Create one small local SparkSession and reuse it across the test suite."""

    # Starting Spark is relatively expensive compared with these tiny unit tests.
    # A session-scoped fixture starts it once, while each test still receives new
    # immutable DataFrames so test data remains isolated.
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("NullMarket-Phase12-Tests")
        # One local worker and one shuffle partition keep small tests fast and
        # predictable without pretending to benchmark distributed performance.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session

    # The fixture owns the Spark lifecycle, so the session is always released
    # after pytest finishes the suite.
    session.stop()


def _nullable_copy(schema: StructType) -> StructType:
    """Copy a production schema while allowing tests to construct invalid nulls."""

    # Production source schemas correctly declare required fields non-nullable.
    # createDataFrame() would refuse None in those fields before the NullMarket
    # validators ever see the record. Tests intentionally need to create rows
    # that represent malformed/permissively parsed source input, so only the
    # fixture's construction schema is relaxed; data types remain unchanged.
    return StructType(
        [
            StructField(
                field.name,
                field.dataType,
                nullable=True,
                metadata=field.metadata,
            )
            for field in schema.fields
        ]
    )


@pytest.fixture
def make_df(spark: SparkSession) -> Callable[[StructType, list[dict]], DataFrame]:
    """Return a small DataFrame factory that preserves production Spark types."""

    def _make(schema: StructType, rows: list[dict]) -> DataFrame:
        # Tests use explicit schemas instead of inference so Decimal, Date, and
        # Timestamp behavior matches the real NullMarket source contracts.
        return spark.createDataFrame(rows, schema=_nullable_copy(schema))

    return _make


@pytest.fixture
def valid_stores(make_df) -> DataFrame:
    """Provide two valid stores for foreign-key and transformation tests."""

    return make_df(
        STORES_SCHEMA,
        [
            {
                "store_id": "S0001",
                "store_name": "NullMarket Toronto",
                "city": "Toronto",
                "province": "Ontario",
                "region": "Central",
                "store_type": "Urban",
                "open_date": date(2020, 1, 1),
            },
            {
                "store_id": "S0002",
                "store_name": "NullMarket Vancouver",
                "city": "Vancouver",
                "province": "British Columbia",
                "region": "West",
                "store_type": "Urban",
                "open_date": date(2021, 1, 1),
            },
        ],
    )


@pytest.fixture
def valid_products(make_df) -> DataFrame:
    """Provide two valid products with exact Decimal prices and costs."""

    return make_df(
        PRODUCTS_SCHEMA,
        [
            {
                "product_id": "P00001",
                "product_name": "Test Product A",
                "category": "Grocery",
                "subcategory": "Pantry",
                "brand": "Northline",
                "unit_cost": Decimal("4.00"),
                "list_price": Decimal("10.00"),
                "active_flag": True,
            },
            {
                "product_id": "P00002",
                "product_name": "Test Product B",
                "category": "Home",
                "subcategory": "Kitchen",
                "brand": "MapleWorks",
                "unit_cost": Decimal("2.00"),
                "list_price": Decimal("5.00"),
                "active_flag": True,
            },
        ],
    )


@pytest.fixture
def valid_orders(make_df) -> DataFrame:
    """Provide two valid orders at distinct stores and dates."""

    return make_df(
        ORDERS_SCHEMA,
        [
            {
                "order_id": "O000001",
                "customer_id": "C00001",
                "store_id": "S0001",
                "order_timestamp": datetime(2026, 1, 1, 10, 0, 0),
                "order_status": "Completed",
                "payment_method": "Credit Card",
                "channel": "Store",
            },
            {
                "order_id": "O000002",
                "customer_id": "C00002",
                "store_id": "S0002",
                "order_timestamp": datetime(2026, 1, 3, 11, 0, 0),
                "order_status": "Pending",
                "payment_method": "Debit Card",
                "channel": "Online",
            },
        ],
    )
