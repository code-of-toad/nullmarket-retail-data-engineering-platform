"""Focused automated tests for NullMarket Phase 8 data-quality rules."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from src.data_quality import (
    VALIDATION_REASONS_COLUMN,
    validate_inventory_snapshots,
    validate_order_items,
    validate_orders,
    validate_products,
)
from src.schemas import (
    INVENTORY_SNAPSHOTS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
)


def _reasons_for_single_reject(result) -> set[str]:
    """Return one rejected row's reasons without depending on row ordering."""

    rows = result.rejected.select(VALIDATION_REASONS_COLUMN).collect()
    assert len(rows) == 1
    return set(rows[0][VALIDATION_REASONS_COLUMN])


def test_duplicate_simple_product_key_quarantines_every_duplicate(make_df) -> None:
    """Both rows in a duplicate simple key must be rejected, not just one copy."""

    products = make_df(
        PRODUCTS_SCHEMA,
        [
            {
                "product_id": "P00001",
                "product_name": "First Copy",
                "category": "Grocery",
                "subcategory": "Pantry",
                "brand": "Northline",
                "unit_cost": Decimal("4.00"),
                "list_price": Decimal("10.00"),
                "active_flag": True,
            },
            {
                "product_id": "P00001",
                "product_name": "Second Copy",
                "category": "Grocery",
                "subcategory": "Pantry",
                "brand": "Northline",
                "unit_cost": Decimal("4.00"),
                "list_price": Decimal("10.00"),
                "active_flag": True,
            },
        ],
    )

    result = validate_products(products)

    assert result.accepted.count() == 0
    rejected = result.rejected.select("product_id", VALIDATION_REASONS_COLUMN).collect()
    assert len(rejected) == 2
    assert all(
        "duplicate_key: product_id" in row[VALIDATION_REASONS_COLUMN]
        for row in rejected
    )


def test_duplicate_composite_order_item_key_is_detected(
    make_df,
    valid_orders,
    valid_products,
) -> None:
    """The full (order_id, line_number) grain must be unique."""

    order_items = make_df(
        ORDER_ITEMS_SCHEMA,
        [
            {
                "order_id": "O000001",
                "line_number": 1,
                "product_id": "P00001",
                "quantity": 1,
                "unit_price": Decimal("10.00"),
                "discount_amount": Decimal("0.00"),
            },
            {
                "order_id": "O000001",
                "line_number": 1,
                "product_id": "P00002",
                "quantity": 1,
                "unit_price": Decimal("5.00"),
                "discount_amount": Decimal("0.00"),
            },
        ],
    )

    result = validate_order_items(order_items, valid_orders, valid_products)

    assert result.accepted.count() == 0
    rejected = result.rejected.select(VALIDATION_REASONS_COLUMN).collect()
    assert len(rejected) == 2
    assert all(
        "duplicate_key: order_id,line_number" in row[VALIDATION_REASONS_COLUMN]
        for row in rejected
    )


def test_required_null_is_rejected_while_valid_order_is_accepted(
    make_df,
    valid_stores,
) -> None:
    """A required-field failure must be quarantined without losing valid rows."""

    orders = make_df(
        ORDERS_SCHEMA,
        [
            {
                "order_id": "O000101",
                "customer_id": "C00101",
                "store_id": "S0001",
                "order_timestamp": datetime(2026, 1, 1, 9, 0, 0),
                "order_status": "Completed",
                "payment_method": "Credit Card",
                "channel": "Store",
            },
            {
                "order_id": "O000102",
                # This simulates a malformed/permissively parsed required field.
                "customer_id": None,
                "store_id": "S0001",
                "order_timestamp": datetime(2026, 1, 1, 9, 5, 0),
                "order_status": "Completed",
                "payment_method": "Credit Card",
                "channel": "Store",
            },
        ],
    )

    result = validate_orders(orders, valid_stores)

    accepted_ids = {row.order_id for row in result.accepted.select("order_id").collect()}
    rejected = result.rejected.select("order_id", VALIDATION_REASONS_COLUMN).collect()

    assert accepted_ids == {"O000101"}
    assert len(rejected) == 1
    assert rejected[0].order_id == "O000102"
    assert "customer_id: required_non_blank" in rejected[0][VALIDATION_REASONS_COLUMN]


def test_orphan_product_foreign_key_is_rejected(
    make_df,
    valid_orders,
    valid_products,
) -> None:
    """An order line cannot enter trusted data when its product parent is absent."""

    order_items = make_df(
        ORDER_ITEMS_SCHEMA,
        [
            {
                "order_id": "O000001",
                "line_number": 99,
                "product_id": "P99999",
                "quantity": 1,
                "unit_price": Decimal("10.00"),
                "discount_amount": Decimal("0.00"),
            }
        ],
    )

    result = validate_order_items(order_items, valid_orders, valid_products)

    assert result.accepted.count() == 0
    assert _reasons_for_single_reject(result) == {
        "orphan_foreign_key: product_id -> products.product_id"
    }


def test_invalid_order_item_quantity_price_and_discount_are_all_reported(
    make_df,
    valid_orders,
    valid_products,
) -> None:
    """A row can retain multiple useful rejection reasons at the same time."""

    order_items = make_df(
        ORDER_ITEMS_SCHEMA,
        [
            {
                "order_id": "O000001",
                "line_number": 50,
                "product_id": "P00001",
                "quantity": 0,
                "unit_price": Decimal("-1.00"),
                "discount_amount": Decimal("-0.50"),
            }
        ],
    )

    result = validate_order_items(order_items, valid_orders, valid_products)

    assert result.accepted.count() == 0
    assert _reasons_for_single_reject(result) == {
        "quantity: must_be_positive",
        "unit_price: must_be_non_negative",
        "discount_amount: must_be_non_negative",
    }


def test_invalid_product_cost_and_list_price_are_quarantined(make_df) -> None:
    """Negative product cost/price values must not reach accepted products."""

    products = make_df(
        PRODUCTS_SCHEMA,
        [
            {
                "product_id": "P00010",
                "product_name": "Invalid Cost Product",
                "category": "Home",
                "subcategory": "Kitchen",
                "brand": "MapleWorks",
                "unit_cost": Decimal("-2.00"),
                "list_price": Decimal("-1.00"),
                "active_flag": True,
            }
        ],
    )

    result = validate_products(products)

    assert result.accepted.count() == 0
    assert _reasons_for_single_reject(result) == {
        "unit_cost: must_be_non_negative",
        "list_price: must_be_non_negative",
    }


def test_invalid_inventory_values_are_rejected(
    make_df,
    valid_stores,
    valid_products,
) -> None:
    """Inventory quantities and reorder thresholds may be zero, but not negative."""

    inventory = make_df(
        INVENTORY_SNAPSHOTS_SCHEMA,
        [
            {
                "snapshot_date": date(2026, 1, 1),
                "store_id": "S0001",
                "product_id": "P00001",
                "quantity_on_hand": -1,
                "reorder_level": -2,
            }
        ],
    )

    result = validate_inventory_snapshots(inventory, valid_stores, valid_products)

    assert result.accepted.count() == 0
    assert _reasons_for_single_reject(result) == {
        "quantity_on_hand: must_be_non_negative",
        "reorder_level: must_be_non_negative",
    }
