"""Reusable data-quality validation for NullMarket Phase 8."""

from __future__ import annotations

# dataclass gives us a compact class for storing the two outputs of validation:
# accepted records and rejected records.
from dataclasses import dataclass

# Sequence is used only for type hints. It means a function can accept an ordered
# collection such as a tuple or list of column names.
from typing import Sequence

# DataFrame is Spark's table-like data structure.
# Window lets us calculate values across groups of related rows, which we use
# to detect duplicate keys without collapsing the original rows.
from pyspark.sql import DataFrame, Window

# PySpark SQL functions are conventionally imported as F.
# Examples below include F.col(), F.when(), F.trim(), and F.size().
from pyspark.sql import functions as F

# We use StringType to tell whether a key column contains strings.
# String keys need an extra blank-string check in addition to a null check.
from pyspark.sql.types import StringType

# Every validated DataFrame will receive this column.
# It stores zero or more human-readable reasons why a row failed validation.
VALIDATION_REASONS_COLUMN = "validation_reasons"


@dataclass(frozen=True)
class ValidationResult:
    """Accepted and rejected views of one validated source DataFrame."""

    # Rows with no validation failures.
    accepted: DataFrame

    # Rows with one or more validation failures.
    rejected: DataFrame


# -----------------------------------------------------------------------------
# SHARED VALIDATION HELPERS
# -----------------------------------------------------------------------------
# These functions are deliberately reusable. Dataset-specific validators later
# in the file combine them instead of rewriting the same validation logic over
# and over.


def initialize_validation(df: DataFrame) -> DataFrame:
    """Add the validation-reason array if it is not already present."""

    # A DataFrame might already have been through one or more validation steps.
    # If the reasons column already exists, do not replace it or lose earlier
    # failure reasons.
    if VALIDATION_REASONS_COLUMN in df.columns:
        return df

    # Add an empty ARRAY<STRING> column to every row.
    # Example at this point: validation_reasons = []
    return df.withColumn(
        VALIDATION_REASONS_COLUMN,
        F.expr("CAST(array() AS ARRAY<STRING>)"),
    )


def add_validation_reason(
    df: DataFrame,
    invalid_condition,
    reason: str,
) -> DataFrame:
    """Append one reason when a validation condition fails."""

    # Make sure the DataFrame has a place to store validation messages.
    df = initialize_validation(df)

    # withColumn() returns a new DataFrame; Spark DataFrames are immutable.
    # For each row:
    #   - if invalid_condition is true, append the supplied reason;
    #   - otherwise, keep the existing reasons unchanged.
    return df.withColumn(
        VALIDATION_REASONS_COLUMN,
        F.when(
            invalid_condition,
            # array_union() appends the reason while avoiding duplicate entries
            # if the exact same validation reason is applied more than once.
            F.array_union(
                F.col(VALIDATION_REASONS_COLUMN),
                F.array(F.lit(reason)),
            ),
        ).otherwise(F.col(VALIDATION_REASONS_COLUMN)),
    )


def validate_not_null(df: DataFrame, column: str) -> DataFrame:
    """Reject a typed required field when schema parsing leaves it null."""

    # This is especially useful after explicit schema parsing. For example,
    # a malformed timestamp can become null, and this rule makes that failure
    # visible in the rejected-record output.
    return add_validation_reason(
        df,
        F.col(column).isNull(),
        f"{column}: required_or_invalid",
    )


def validate_non_blank(df: DataFrame, column: str) -> DataFrame:
    """Reject a required string when it is null, empty, or whitespace only."""

    # A required string fails if it is:
    #   None / NULL
    #   ""
    #   "   "
    # trim() removes surrounding whitespace before comparing with "".
    return add_validation_reason(
        df,
        F.col(column).isNull() | (F.trim(F.col(column)) == ""),
        f"{column}: required_non_blank",
    )


def validate_unique_key(
    df: DataFrame,
    key_columns: Sequence[str],
) -> DataFrame:
    """Flag every row participating in a duplicate simple or composite key."""

    # A key cannot be defined with zero columns, so fail fast if the caller
    # accidentally supplies an empty collection.
    if not key_columns:
        raise ValueError("key_columns must contain at least one column")

    # partitionBy() creates a logical group for each distinct key value.
    # Examples:
    #   ("product_id",) groups rows by product_id.
    #   ("order_id", "line_number") groups rows by the full composite key.
    window = Window.partitionBy(*[F.col(column) for column in key_columns])

    # Count how many rows exist inside each key group.
    # If the count is greater than 1, every row in that key group is a
    # participant in a duplicate key and should be flagged.
    duplicate_condition = F.count(F.lit(1)).over(window) > 1

    # Build a readable key name for the stored validation reason.
    # Example: "order_id,line_number"
    key_name = ",".join(key_columns)

    return add_validation_reason(
        df,
        duplicate_condition,
        f"duplicate_key: {key_name}",
    )


def _is_present(df: DataFrame, column: str):
    """Return a Spark condition indicating that a key value is present."""

    # Find this column's schema definition so we can inspect its Spark data type.
    field = next(field for field in df.schema.fields if field.name == column)

    # All key types are considered present when they are not NULL.
    condition = F.col(column).isNotNull()

    # String keys also need to reject empty or whitespace-only values.
    if isinstance(field.dataType, StringType):
        condition = condition & (F.trim(F.col(column)) != "")

    # This returns a Spark Column expression (a condition), not a Python Boolean.
    return condition


def validate_foreign_key(
    df: DataFrame,
    child_column: str,
    parent_df: DataFrame,
    parent_column: str,
    parent_name: str,
) -> DataFrame:
    """Flag non-empty child keys that do not exist in the parent key set."""

    # Make sure validation_reasons exists before we perform the validation join.
    df = initialize_validation(df)

    # Read the parent key's Spark data type. String parent keys need an
    # additional blank-string check before they are considered valid parents.
    parent_field = next(
        field for field in parent_df.schema.fields if field.name == parent_column
    )

    # Select only the parent key because that is all we need for referential
    # integrity validation. Rename it to a temporary internal column so it does
    # not collide with columns already present in the child DataFrame.
    parent_keys = parent_df.select(
        F.col(parent_column).alias("__dq_parent_key")
    )

    # Start by excluding NULL parent keys.
    parent_present = F.col("__dq_parent_key").isNotNull()

    # For string keys, also exclude empty and whitespace-only values.
    if isinstance(parent_field.dataType, StringType):
        parent_present = parent_present & (F.trim(F.col("__dq_parent_key")) != "")

    # Keep only usable parent keys.
    # distinct() leaves one copy of each key, which prevents duplicate parent
    # rows from multiplying child rows during this validation join.
    # __dq_parent_exists is a temporary marker that tells us whether a match
    # was found after the left join.
    parent_keys = (
        parent_keys
        .where(parent_present)
        .distinct()
        .withColumn("__dq_parent_exists", F.lit(True))
    )

    # LEFT JOIN keeps every child row.
    # If its key matches a parent key, __dq_parent_exists will be True.
    # If no parent exists, the temporary parent columns will be NULL.
    joined = df.join(
        parent_keys,
        F.col(child_column) == F.col("__dq_parent_key"),
        "left",
    )

    # A row is an orphan only when:
    #   1. the child key itself is present, AND
    #   2. the join found no matching parent.
    # Missing child keys are handled by required/non-blank validation instead,
    # which keeps the failure reasons precise.
    result = add_validation_reason(
        joined,
        _is_present(joined, child_column)
        & F.col("__dq_parent_exists").isNull(),
        f"orphan_foreign_key: {child_column} -> {parent_name}.{parent_column}",
    )

    # Remove the temporary columns used only for the validation join so the
    # output returns to the original source shape plus validation_reasons.
    return result.drop("__dq_parent_key", "__dq_parent_exists")


def validate_positive(df: DataFrame, column: str) -> DataFrame:
    """Validate a documented strictly-positive numeric rule."""

    # "Positive" means greater than zero.
    # The isNotNull() guard lets the separate required-value validator own the
    # null failure instead of giving the same row an unrelated numeric reason.
    return add_validation_reason(
        df,
        F.col(column).isNotNull() & (F.col(column) <= 0),
        f"{column}: must_be_positive",
    )


def validate_non_negative(df: DataFrame, column: str) -> DataFrame:
    """Validate a documented non-negative numeric rule."""

    # "Non-negative" means zero is valid but any value below zero is invalid.
    return add_validation_reason(
        df,
        F.col(column).isNotNull() & (F.col(column) < 0),
        f"{column}: must_be_non_negative",
    )


def separate_records(df: DataFrame) -> ValidationResult:
    """Split a validated DataFrame without silently dropping failed records."""

    # Ensure the reasons array exists even if no other validator happened to
    # initialize it first.
    df = initialize_validation(df)

    # Accepted rows have an empty validation_reasons array.
    accepted = df.where(F.size(F.col(VALIDATION_REASONS_COLUMN)) == 0)

    # Rejected rows have at least one recorded validation failure.
    rejected = df.where(F.size(F.col(VALIDATION_REASONS_COLUMN)) > 0)

    # Return both populations together so callers cannot accidentally lose the
    # rejected records simply because they are interested in accepted ones.
    return ValidationResult(accepted=accepted, rejected=rejected)


# -----------------------------------------------------------------------------
# DATASET-SPECIFIC VALIDATORS
# -----------------------------------------------------------------------------
# Each function below applies only rules already documented for that source.
# They reuse the generic helpers above rather than embedding custom validation
# logic directly into the future pipeline.


def validate_orders(orders: DataFrame, stores: DataFrame) -> ValidationResult:
    """Apply only the documented orders source rules."""

    # Start with an empty validation_reasons array on every order row.
    df = initialize_validation(orders)

    # These fields are required strings, so NULL, "", and whitespace-only
    # values are all invalid.
    for column in (
        "order_id",
        "customer_id",
        "store_id",
        "order_status",
        "payment_method",
        "channel",
    ):
        df = validate_non_blank(df, column)

    # order_timestamp is already parsed as a TimestampType by Phase 7.
    # A malformed source value can appear as NULL after parsing, so reject it.
    df = validate_not_null(df, "order_timestamp")

    # orders has one row per order, so order_id must be unique.
    df = validate_unique_key(df, ("order_id",))

    # Every non-empty orders.store_id must identify an accepted store.
    df = validate_foreign_key(df, "store_id", stores, "store_id", "stores")

    # Finally split orders into accepted and rejected DataFrames.
    return separate_records(df)


def validate_order_items(
    order_items: DataFrame,
    orders: DataFrame,
    products: DataFrame,
) -> ValidationResult:
    """Apply only the documented order_items source rules."""

    df = initialize_validation(order_items)

    # These required identifiers are strings and therefore cannot be NULL or
    # blank.
    for column in ("order_id", "product_id"):
        df = validate_non_blank(df, column)

    # These are required typed/numeric fields. If schema parsing leaves one
    # NULL, the row is invalid.
    for column in (
        "line_number",
        "quantity",
        "unit_price",
        "discount_amount",
    ):
        df = validate_not_null(df, column)

    # The source grain is one product line within an order, so the full
    # (order_id, line_number) combination must be unique.
    df = validate_unique_key(df, ("order_id", "line_number"))

    # Each order item must reference an accepted order and an accepted product.
    df = validate_foreign_key(df, "order_id", orders, "order_id", "orders")
    df = validate_foreign_key(df, "product_id", products, "product_id", "products")

    # Apply the documented numeric business rules.
    df = validate_positive(df, "line_number")
    df = validate_positive(df, "quantity")
    df = validate_non_negative(df, "unit_price")
    df = validate_non_negative(df, "discount_amount")

    return separate_records(df)


def validate_products(products: DataFrame) -> ValidationResult:
    """Apply only the documented products source rules."""

    df = initialize_validation(products)

    # Required descriptive product fields cannot be NULL or blank.
    for column in (
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
    ):
        df = validate_non_blank(df, column)

    # Cost, price, and the Boolean active flag are required typed values.
    for column in ("unit_cost", "list_price", "active_flag"):
        df = validate_not_null(df, column)

    # products has one row per product, so product_id must be unique.
    df = validate_unique_key(df, ("product_id",))

    # Costs and list prices may be zero but may not be negative.
    df = validate_non_negative(df, "unit_cost")
    df = validate_non_negative(df, "list_price")

    return separate_records(df)


def validate_stores(stores: DataFrame) -> ValidationResult:
    """Apply only the documented stores source rules."""

    df = initialize_validation(stores)

    # All documented store text fields are required and non-blank.
    for column in (
        "store_id",
        "store_name",
        "city",
        "province",
        "region",
        "store_type",
    ):
        df = validate_non_blank(df, column)

    # open_date is required. Because Phase 7 already parses it as DateType,
    # malformed dates can be identified here when they appear as NULL.
    df = validate_not_null(df, "open_date")

    # stores has one row per store, so store_id must be unique.
    df = validate_unique_key(df, ("store_id",))

    return separate_records(df)


def validate_inventory_snapshots(
    inventory_snapshots: DataFrame,
    stores: DataFrame,
    products: DataFrame,
) -> ValidationResult:
    """Apply only the documented inventory_snapshots source rules."""

    df = initialize_validation(inventory_snapshots)

    # snapshot_date is part of the composite key and is required.
    df = validate_not_null(df, "snapshot_date")

    # Store and product identifiers are required strings.
    for column in ("store_id", "product_id"):
        df = validate_non_blank(df, column)

    # Inventory quantity and reorder level are required integer fields.
    for column in ("quantity_on_hand", "reorder_level"):
        df = validate_not_null(df, column)

    # The declared grain is one product at one store on one snapshot date.
    # Therefore this complete three-column key must be unique.
    df = validate_unique_key(df, ("snapshot_date", "store_id", "product_id"))

    # Each inventory snapshot must reference accepted store and product parents.
    df = validate_foreign_key(df, "store_id", stores, "store_id", "stores")
    df = validate_foreign_key(df, "product_id", products, "product_id", "products")

    # Inventory quantities and reorder thresholds can be zero but not negative.
    df = validate_non_negative(df, "quantity_on_hand")
    df = validate_non_negative(df, "reorder_level")

    return separate_records(df)


def validate_all_sources(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
    stores: DataFrame,
    inventory_snapshots: DataFrame,
) -> dict[str, ValidationResult]:
    """Validate parents first so accepted children reference accepted parents."""

    # stores and products are parent/reference datasets, so validate them first.
    stores_result = validate_stores(stores)
    products_result = validate_products(products)

    # orders depends on stores. Importantly, it is checked against ACCEPTED
    # stores, not against stores that already failed their own validation.
    orders_result = validate_orders(orders, stores_result.accepted)

    # order_items depends on both orders and products, so use only accepted
    # parents from those datasets for referential-integrity validation.
    order_items_result = validate_order_items(
        order_items,
        orders_result.accepted,
        products_result.accepted,
    )

    # inventory_snapshots depends on stores and products and likewise checks
    # only against accepted parent records.
    inventory_result = validate_inventory_snapshots(
        inventory_snapshots,
        stores_result.accepted,
        products_result.accepted,
    )

    # Return one ValidationResult per source dataset.
    return {
        "orders": orders_result,
        "order_items": order_items_result,
        "products": products_result,
        "stores": stores_result,
        "inventory_snapshots": inventory_result,
    }
