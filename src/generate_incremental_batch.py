"""Generate the deterministic Phase 17 incremental NullMarket batch.

This file creates ONLY genuinely new transactional/snapshot source records.
The unchanged products/stores reference files are reused from the existing GCS
raw layer when the batch is uploaded, so the established transformation logic
and warehouse key mappings do not need to be redesigned.
"""

from __future__ import annotations

import csv
from pathlib import Path


# -----------------------------------------------------------------------------
# PHASE 17 BATCH IDENTITY
# -----------------------------------------------------------------------------
# The baseline Phase 6 generator created orders O000001 through O000500 and
# business dates through 2026-03-31. This batch therefore starts at O000501 and
# uses only April 2026 dates so the records are unambiguously new.
BATCH_ID = "phase17_batch_01"

# Keep generated bulk data outside source control. The project's .gitignore
# should continue to exclude generated data directories.
OUTPUT_ROOT = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "incremental"
    / BATCH_ID
)


# -----------------------------------------------------------------------------
# NEW ORDERS
# Grain: one row per order.
#
# The statuses intentionally include Completed, Pending, and Cancelled because
# NullMarket's authoritative requirements define NO order_status revenue filter.
# -----------------------------------------------------------------------------
ORDERS = [
    {
        "order_id": "O000501",
        "customer_id": "C00181",
        "store_id": "S0001",
        "order_timestamp": "2026-04-01 09:15:00",
        "order_status": "Completed",
        "payment_method": "Credit Card",
        "channel": "Store",
    },
    {
        "order_id": "O000502",
        "customer_id": "C00182",
        "store_id": "S0004",
        "order_timestamp": "2026-04-01 14:30:00",
        "order_status": "Pending",
        "payment_method": "Debit Card",
        "channel": "Online",
    },
    {
        "order_id": "O000503",
        "customer_id": "C00183",
        "store_id": "S0007",
        "order_timestamp": "2026-04-02 10:05:00",
        "order_status": "Cancelled",
        "payment_method": "Digital Wallet",
        "channel": "Online",
    },
    {
        "order_id": "O000504",
        "customer_id": "C00184",
        "store_id": "S0010",
        "order_timestamp": "2026-04-02 16:45:00",
        "order_status": "Completed",
        "payment_method": "Credit Card",
        "channel": "Store",
    },
    {
        "order_id": "O000505",
        "customer_id": "C00185",
        "store_id": "S0002",
        "order_timestamp": "2026-04-03 11:20:00",
        "order_status": "Completed",
        "payment_method": "Debit Card",
        "channel": "Store",
    },
    {
        "order_id": "O000506",
        "customer_id": "C00186",
        "store_id": "S0012",
        "order_timestamp": "2026-04-03 18:10:00",
        "order_status": "Pending",
        "payment_method": "Digital Wallet",
        "channel": "Online",
    },
]


# -----------------------------------------------------------------------------
# NEW ORDER ITEMS
# Grain: one product line within an order.
# Business key: (order_id, line_number).
#
# All product IDs deliberately avoid P00001, which is excluded from the current
# accepted product dimension because the baseline source contains a deterministic
# duplicate-key defect for that product.
# -----------------------------------------------------------------------------
ORDER_ITEMS = [
    {"order_id": "O000501", "line_number": 1, "product_id": "P00002", "quantity": 2, "unit_price": "24.50", "discount_amount": "0.00"},
    {"order_id": "O000501", "line_number": 2, "product_id": "P00003", "quantity": 1, "unit_price": "79.99", "discount_amount": "5.00"},
    {"order_id": "O000502", "line_number": 1, "product_id": "P00004", "quantity": 3, "unit_price": "18.25", "discount_amount": "2.50"},
    {"order_id": "O000502", "line_number": 2, "product_id": "P00005", "quantity": 1, "unit_price": "112.00", "discount_amount": "0.00"},
    {"order_id": "O000503", "line_number": 1, "product_id": "P00006", "quantity": 2, "unit_price": "45.75", "discount_amount": "4.00"},
    {"order_id": "O000503", "line_number": 2, "product_id": "P00007", "quantity": 4, "unit_price": "13.50", "discount_amount": "0.00"},
    {"order_id": "O000504", "line_number": 1, "product_id": "P00008", "quantity": 1, "unit_price": "149.95", "discount_amount": "15.00"},
    {"order_id": "O000504", "line_number": 2, "product_id": "P00009", "quantity": 2, "unit_price": "34.20", "discount_amount": "0.00"},
    {"order_id": "O000505", "line_number": 1, "product_id": "P00010", "quantity": 5, "unit_price": "9.99", "discount_amount": "3.00"},
    {"order_id": "O000505", "line_number": 2, "product_id": "P00011", "quantity": 1, "unit_price": "88.40", "discount_amount": "8.40"},
    {"order_id": "O000506", "line_number": 1, "product_id": "P00012", "quantity": 2, "unit_price": "57.60", "discount_amount": "5.00"},
    {"order_id": "O000506", "line_number": 2, "product_id": "P00013", "quantity": 3, "unit_price": "21.30", "discount_amount": "1.50"},
]


# -----------------------------------------------------------------------------
# NEW INVENTORY SNAPSHOTS
# Grain: one product x store x snapshot date.
# Business key: (snapshot_date, store_id, product_id).
#
# These are intentionally sparse new observations rather than a full 12 x 99
# matrix. NullMarket's requirements define the row grain but do not require every
# snapshot date to contain every possible product/store combination.
# -----------------------------------------------------------------------------
INVENTORY_SNAPSHOTS = [
    {"snapshot_date": "2026-04-04", "store_id": "S0001", "product_id": "P00014", "quantity_on_hand": 8,  "reorder_level": 15},
    {"snapshot_date": "2026-04-04", "store_id": "S0002", "product_id": "P00015", "quantity_on_hand": 22, "reorder_level": 18},
    {"snapshot_date": "2026-04-04", "store_id": "S0003", "product_id": "P00016", "quantity_on_hand": 4,  "reorder_level": 10},
    {"snapshot_date": "2026-04-04", "store_id": "S0004", "product_id": "P00017", "quantity_on_hand": 31, "reorder_level": 20},
    {"snapshot_date": "2026-04-04", "store_id": "S0005", "product_id": "P00018", "quantity_on_hand": 12, "reorder_level": 12},
    {"snapshot_date": "2026-04-04", "store_id": "S0006", "product_id": "P00019", "quantity_on_hand": 6,  "reorder_level": 14},
    {"snapshot_date": "2026-04-04", "store_id": "S0007", "product_id": "P00020", "quantity_on_hand": 27, "reorder_level": 16},
    {"snapshot_date": "2026-04-04", "store_id": "S0008", "product_id": "P00021", "quantity_on_hand": 9,  "reorder_level": 11},
    {"snapshot_date": "2026-04-04", "store_id": "S0009", "product_id": "P00022", "quantity_on_hand": 40, "reorder_level": 25},
    {"snapshot_date": "2026-04-04", "store_id": "S0010", "product_id": "P00023", "quantity_on_hand": 3,  "reorder_level": 8},
    {"snapshot_date": "2026-04-04", "store_id": "S0011", "product_id": "P00024", "quantity_on_hand": 19, "reorder_level": 19},
    {"snapshot_date": "2026-04-04", "store_id": "S0012", "product_id": "P00025", "quantity_on_hand": 14, "reorder_level": 9},
]


FIELDNAMES = {
    "orders": [
        "order_id",
        "customer_id",
        "store_id",
        "order_timestamp",
        "order_status",
        "payment_method",
        "channel",
    ],
    "order_items": [
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
    ],
    "inventory_snapshots": [
        "snapshot_date",
        "store_id",
        "product_id",
        "quantity_on_hand",
        "reorder_level",
    ],
}


def assert_unique(rows: list[dict], key_columns: tuple[str, ...], dataset_name: str) -> None:
    """Fail before writing if this deterministic batch accidentally repeats a key."""

    keys = [
        tuple(row[column] for column in key_columns)
        for row in rows
    ]

    if len(keys) != len(set(keys)):
        raise ValueError(
            f"{dataset_name} contains a duplicate business key for {key_columns}"
        )


def validate_batch_definition() -> None:
    """Run lightweight generator-time checks before Spark performs full validation."""

    # Verify the three declared source grains before files are written.
    assert_unique(ORDERS, ("order_id",), "orders")
    assert_unique(ORDER_ITEMS, ("order_id", "line_number"), "order_items")
    assert_unique(
        INVENTORY_SNAPSHOTS,
        ("snapshot_date", "store_id", "product_id"),
        "inventory_snapshots",
    )

    # Every order item in this batch must belong to one of the new orders.
    order_ids = {row["order_id"] for row in ORDERS}
    item_order_ids = {row["order_id"] for row in ORDER_ITEMS}

    if not item_order_ids.issubset(order_ids):
        raise ValueError("order_items contains an order_id absent from this batch")

    # These assertions make the intended incremental boundary explicit.
    # The baseline warehouse ends on 2026-03-31 for sales and 2026-03-26 for
    # inventory, so every new business date must be strictly later.
    if any(row["order_timestamp"][:10] <= "2026-03-31" for row in ORDERS):
        raise ValueError("incremental orders must be after the baseline sales history")

    if any(row["snapshot_date"] <= "2026-03-26" for row in INVENTORY_SNAPSHOTS):
        raise ValueError(
            "incremental inventory snapshots must be after the baseline inventory history"
        )


def write_csv(dataset_name: str, rows: list[dict]) -> Path:
    """Write one batch source using the same CSV conventions as Phase 6."""

    output_path = OUTPUT_ROOT / dataset_name / f"{dataset_name}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES[dataset_name])
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def main() -> None:
    """Generate only the new Phase 17 transactional/snapshot records."""

    validate_batch_definition()

    outputs = {
        "orders": write_csv("orders", ORDERS),
        "order_items": write_csv("order_items", ORDER_ITEMS),
        "inventory_snapshots": write_csv(
            "inventory_snapshots",
            INVENTORY_SNAPSHOTS,
        ),
    }

    print(f"Generated deterministic incremental batch: {BATCH_ID}")
    print(f"Output root: {OUTPUT_ROOT}")
    print(f"orders: {len(ORDERS)} rows")
    print(f"order_items: {len(ORDER_ITEMS)} rows")
    print(f"inventory_snapshots: {len(INVENTORY_SNAPSHOTS)} rows")
    print("sales dates: 2026-04-01 through 2026-04-03")
    print("inventory snapshot date: 2026-04-04")
    print("\nGenerated files:")
    for dataset_name, output_path in outputs.items():
        print(f"  {dataset_name}: {output_path}")


if __name__ == "__main__":
    main()
