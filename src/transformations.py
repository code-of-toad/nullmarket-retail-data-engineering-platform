"""PySpark transformations for NullMarket Phase 9."""

from __future__ import annotations

# Decimal is used to create an exact zero-money literal for revenue calculations.
# Using Decimal avoids introducing binary floating-point behavior into currency logic.
from decimal import Decimal

# DataFrame is Spark's table-like distributed data structure.
# Window defines groups/orderings used by ranking, rolling calculations, and
# deterministic warehouse-key generation.
from pyspark.sql import DataFrame, Window

# Spark SQL functions are imported under the conventional alias F.
from pyspark.sql import functions as F

# DecimalType gives calculated money columns an explicit fixed-precision type.
from pyspark.sql.types import DecimalType


# Phase 7 source money columns use DECIMAL(12,2). Derived measures can grow when
# values are multiplied or aggregated, so Phase 9 uses a wider DECIMAL(18,2).
MONEY_TYPE = DecimalType(18, 2)


# -----------------------------------------------------------------------------
# DIMENSION BUILDERS
# -----------------------------------------------------------------------------
# These functions receive only Phase 8 accepted records. They do not read files
# or write outputs; they only transform DataFrames. Keeping this logic here
# separates reusable transformations from pipeline orchestration.


def build_dim_product(products: DataFrame) -> DataFrame:
    """Build the current product dimension from accepted product records."""

    # Product source rows are already validated as one row per product. Ordering
    # by the stable business key makes the generated surrogate-key assignment
    # deterministic for the same accepted source data.
    key_window = Window.orderBy(F.col("product_id"))

    return (
        products
        # Keep only attributes documented for dim_product. unit_cost belongs in
        # fact_sales because it is an input to line-level gross margin.
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "list_price",
            "active_flag",
        )
        # row_number() generates a simple warehouse surrogate key in the
        # deterministic product_id ordering.
        .withColumn("product_key", F.row_number().over(key_window))
        # Reorder columns so the warehouse key appears first.
        .select(
            "product_key",
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "list_price",
            "active_flag",
        )
        # Stable ordering makes local inspection easier; row ordering itself is
        # not the dimension's business identity.
        .orderBy("product_key")
    )


def build_dim_store(stores: DataFrame) -> DataFrame:
    """Build the current store dimension from accepted store records."""

    # Store source rows are validated as one row per store. Ordering by store_id
    # gives reproducible surrogate-key assignment for this current-state model.
    key_window = Window.orderBy(F.col("store_id"))

    return (
        stores
        # Keep only the descriptive attributes defined for dim_store.
        .select(
            "store_id",
            "store_name",
            "city",
            "province",
            "region",
            "store_type",
            "open_date",
        )
        # Generate the warehouse surrogate key.
        .withColumn("store_key", F.row_number().over(key_window))
        # Present the surrogate key first in the dimension schema.
        .select(
            "store_key",
            "store_id",
            "store_name",
            "city",
            "province",
            "region",
            "store_type",
            "open_date",
        )
        .orderBy("store_key")
    )


def build_dim_date(
    orders: DataFrame,
    inventory_snapshots: DataFrame,
) -> DataFrame:
    """Build a continuous date dimension covering accepted sales and inventory dates."""

    # Sales timestamps are converted to calendar dates because dim_date has one
    # row per date rather than one row per timestamp.
    sales_dates = orders.select(
        F.to_date("order_timestamp").alias("full_date")
    )

    # Inventory already provides a DateType snapshot_date, so only rename it to
    # the common full_date column used by the date-dimension logic.
    inventory_dates = inventory_snapshots.select(
        F.col("snapshot_date").alias("full_date")
    )

    # The business object here is a set of calendar dates, so removing repeated
    # date values is semantic set construction, not arbitrary record resolution.
    observed_dates = (
        sales_dates
        # Combine dates from both business processes into one conformed calendar.
        .unionByName(inventory_dates)
        # Defensive guard: accepted inputs should already contain valid dates,
        # but null dates cannot define calendar boundaries.
        .filter(F.col("full_date").isNotNull())
        # This deduplicates date values only; it does not resolve duplicate
        # business records from any source dataset.
        .dropDuplicates(["full_date"])
    )

    # Find the earliest and latest accepted business dates. The complete date
    # dimension will span every day between these boundaries.
    bounds = observed_dates.agg(
        F.min("full_date").alias("min_date"),
        F.max("full_date").alias("max_date"),
    )

    return (
        bounds
        # sequence() builds one array containing every calendar day between the
        # boundaries; explode() converts that array into one row per date.
        .select(
            F.explode(
                F.sequence(
                    F.col("min_date"),
                    F.col("max_date"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("full_date")
        )
        # date_key is the deterministic YYYYMMDD warehouse key documented in
        # the Phase 4 model.
        .withColumn(
            "date_key",
            F.date_format("full_date", "yyyyMMdd").cast("int"),
        )
        # Derive reusable calendar attributes for analytical grouping/filtering.
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month_number", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter_number", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .select(
            "date_key",
            "full_date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter_number",
            "year",
        )
        .orderBy("full_date")
    )


# -----------------------------------------------------------------------------
# SALES TRANSFORMATIONS
# -----------------------------------------------------------------------------


def build_sales_dataset(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
    stores: DataFrame,
) -> DataFrame:
    """Build the accepted, enriched sales dataset at one-row-per-order-line grain."""

    # Narrow each accepted source to the columns required by this transformation.
    # Besides readability, this reduces the chance of ambiguous duplicate column
    # names appearing after the joins.
    order_columns = orders.select(
        "order_id",
        "customer_id",
        "store_id",
        "order_timestamp",
        "order_status",
        "payment_method",
        "channel",
    )
    item_columns = order_items.select(
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
    )
    product_columns = products.select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_cost",
    )
    store_columns = stores.select(
        "store_id",
        "store_name",
        "city",
        "province",
        "region",
        "store_type",
    )

    # order_items is the driving DataFrame because the target sales grain is one
    # row per order line. Phase 8 already guarantees accepted foreign-key
    # relationships, so these are many-to-one INNER JOINs that should preserve
    # the order-line row count rather than multiply it.
    sales = (
        item_columns
        .join(order_columns, on="order_id", how="inner")
        .join(product_columns, on="product_id", how="inner")
        .join(store_columns, on="store_id", how="inner")
    )

    # No order_status filter is applied because no such inclusion/exclusion rule
    # exists in the authoritative business requirements.
    return (
        sales
        # gross_sales = quantity * unit_price
        .withColumn(
            "gross_sales",
            (F.col("quantity") * F.col("unit_price")).cast(MONEY_TYPE),
        )
        # net_sales = gross_sales - discount_amount
        .withColumn(
            "net_sales",
            (F.col("gross_sales") - F.col("discount_amount")).cast(MONEY_TYPE),
        )
        # gross_margin = net_sales - (quantity * unit_cost)
        .withColumn(
            "gross_margin",
            (
                F.col("net_sales")
                - (F.col("quantity") * F.col("unit_cost"))
            ).cast(MONEY_TYPE),
        )
        # Keep a business-readable enriched dataset. This is useful for analysis
        # and is also the source used to construct fact_sales.
        .select(
            "order_id",
            "line_number",
            "customer_id",
            "order_timestamp",
            "order_status",
            "payment_method",
            "channel",
            "store_id",
            "store_name",
            "city",
            "province",
            "region",
            "store_type",
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "quantity",
            "unit_price",
            "discount_amount",
            "unit_cost",
            "gross_sales",
            "net_sales",
            "gross_margin",
        )
    )


def build_fact_sales(
    sales: DataFrame,
    dim_date: DataFrame,
    dim_product: DataFrame,
    dim_store: DataFrame,
) -> DataFrame:
    """Create fact_sales while preserving the (order_id, line_number) grain."""

    # Build minimal lookup DataFrames containing only each dimension's business
    # key and warehouse key. Dimension uniqueness makes each lookup many-to-one.
    date_lookup = dim_date.select("date_key", "full_date")
    product_lookup = dim_product.select("product_key", "product_id")
    store_lookup = dim_store.select("store_key", "store_id")

    return (
        sales
        # fact_sales links to dim_date at calendar-day grain, so convert the
        # order timestamp to a date before performing the lookup.
        .withColumn("order_date", F.to_date("order_timestamp"))
        .join(
            date_lookup,
            F.col("order_date") == F.col("full_date"),
            "inner",
        )
        # full_date is only needed for the lookup; date_key remains in the fact.
        .drop("full_date")
        # Replace operational relationship identifiers with warehouse keys while
        # retaining order_id/line_number as degenerate business identifiers.
        .join(product_lookup, on="product_id", how="inner")
        .join(store_lookup, on="store_id", how="inner")
        .select(
            "date_key",
            "store_key",
            "product_key",
            "order_id",
            "line_number",
            "quantity",
            "unit_price",
            "discount_amount",
            "unit_cost",
            "gross_sales",
            "net_sales",
            "gross_margin",
        )
        # Deterministic ordering makes the local output easier to inspect.
        .orderBy("order_id", "line_number")
    )


# -----------------------------------------------------------------------------
# INVENTORY TRANSFORMATIONS
# -----------------------------------------------------------------------------


def build_inventory_dataset(
    inventory_snapshots: DataFrame,
    products: DataFrame,
    stores: DataFrame,
) -> DataFrame:
    """Build enriched inventory while preserving date/store/product snapshot grain."""

    # Product and store dimensions are descriptive many-to-one enrichments of an
    # inventory snapshot. Accepted parent keys are unique because of Phase 8.
    product_columns = products.select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
    )
    store_columns = stores.select(
        "store_id",
        "store_name",
        "city",
        "province",
        "region",
        "store_type",
    )

    return (
        inventory_snapshots
        # Begin strictly at the documented source grain:
        # snapshot_date x store_id x product_id.
        .select(
            "snapshot_date",
            "store_id",
            "product_id",
            "quantity_on_hand",
            "reorder_level",
        )
        .join(product_columns, on="product_id", how="inner")
        .join(store_columns, on="store_id", how="inner")
        # The documented low-stock rule is quantity_on_hand < reorder_level.
        # when()/otherwise() makes that rule explicit as a Boolean attribute.
        .withColumn(
            "is_low_stock",
            F.when(
                F.col("quantity_on_hand") < F.col("reorder_level"),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .select(
            "snapshot_date",
            "store_id",
            "store_name",
            "city",
            "province",
            "region",
            "store_type",
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "quantity_on_hand",
            "reorder_level",
            "is_low_stock",
        )
    )


def build_fact_inventory_snapshot(
    inventory: DataFrame,
    dim_date: DataFrame,
    dim_product: DataFrame,
    dim_store: DataFrame,
) -> DataFrame:
    """Create fact_inventory_snapshot at date/store/product grain."""

    # As with fact_sales, use narrow many-to-one lookups to map source business
    # identifiers onto conformed warehouse keys.
    date_lookup = dim_date.select("date_key", "full_date")
    product_lookup = dim_product.select("product_key", "product_id")
    store_lookup = dim_store.select("store_key", "store_id")

    return (
        inventory
        # snapshot_date already has date grain, so it maps directly to full_date.
        .join(
            date_lookup,
            F.col("snapshot_date") == F.col("full_date"),
            "inner",
        )
        .drop("full_date")
        .join(product_lookup, on="product_id", how="inner")
        .join(store_lookup, on="store_id", how="inner")
        # Do not mix sales measures into this fact. Inventory is point-in-time
        # state and must remain at its separate snapshot grain.
        .select(
            "date_key",
            "store_key",
            "product_key",
            "quantity_on_hand",
            "reorder_level",
            "is_low_stock",
        )
        .orderBy("date_key", "store_key", "product_key")
    )


# -----------------------------------------------------------------------------
# AGGREGATION AND WINDOW-FUNCTION DEMONSTRATIONS
# -----------------------------------------------------------------------------
# These functions demonstrate Phase 9 Spark operations using actual NullMarket
# analytical requirements rather than artificial examples.


def build_product_rankings(
    fact_sales: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Rank products by total net sales using row_number, rank, and dense_rank."""

    # Aggregate order-line net sales to one row per product before ranking.
    product_revenue = (
        fact_sales
        .groupBy("product_key")
        .agg(F.sum("net_sales").cast(MONEY_TYPE).alias("net_sales"))
        # Add business-facing product identifiers/names for readable output.
        .join(
            dim_product.select("product_key", "product_id", "product_name"),
            on="product_key",
            how="inner",
        )
    )

    # rank() and dense_rank() use revenue only so equal revenue values truly tie.
    revenue_rank_window = Window.orderBy(F.col("net_sales").desc())

    # row_number() must always pick a unique ordering, so product_id is a stable
    # tie-breaker after descending revenue.
    deterministic_row_window = Window.orderBy(
        F.col("net_sales").desc(),
        F.col("product_id").asc(),
    )

    return (
        product_revenue
        # row_number: unique 1, 2, 3... positions even when revenue ties.
        .withColumn(
            "row_number",
            F.row_number().over(deterministic_row_window),
        )
        # rank: ties share a position and later positions contain gaps.
        .withColumn("rank", F.rank().over(revenue_rank_window))
        # dense_rank: ties share a position but later positions have no gaps.
        .withColumn("dense_rank", F.dense_rank().over(revenue_rank_window))
        .select(
            "product_key",
            "product_id",
            "product_name",
            "net_sales",
            "row_number",
            "rank",
            "dense_rank",
        )
        .orderBy("row_number")
    )


def build_daily_revenue_trend(
    fact_sales: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Calculate calendar-day revenue, seven-day rolling revenue, and prior-day revenue."""

    # First aggregate transactional order-line revenue to one row per sales date.
    daily_revenue = (
        fact_sales
        .groupBy("date_key")
        .agg(F.sum("net_sales").cast(MONEY_TYPE).alias("daily_net_sales"))
    )

    # A Decimal zero preserves fixed-precision money semantics when a calendar
    # date has no matching sales rows.
    zero_money = F.lit(Decimal("0.00")).cast(MONEY_TYPE)

    # LEFT JOIN from dim_date keeps every calendar date, including zero-sales
    # days. This is important because a seven-row window should represent seven
    # consecutive calendar days rather than seven days on which sales occurred.
    calendar_revenue = (
        dim_date
        .select("date_key", "full_date")
        .join(daily_revenue, on="date_key", how="left")
        .withColumn(
            "daily_net_sales",
            F.coalesce(F.col("daily_net_sales"), zero_money),
        )
    )

    # Order the analytical window chronologically.
    calendar_window = Window.orderBy("full_date")

    # Current row plus the previous six rows = a seven-calendar-day rolling
    # frame because the continuous date dimension supplies one row per day.
    seven_day_window = calendar_window.rowsBetween(-6, 0)

    return (
        calendar_revenue
        # Rolling seven-day net sales includes the current date.
        .withColumn(
            "rolling_7_day_net_sales",
            F.sum("daily_net_sales").over(seven_day_window).cast(MONEY_TYPE),
        )
        # lag(..., 1) exposes the prior calendar day's revenue on the current row.
        .withColumn(
            "previous_day_net_sales",
            F.lag("daily_net_sales", 1).over(calendar_window),
        )
        # The first day has no previous row, so its change naturally remains null.
        .withColumn(
            "day_over_day_change",
            (
                F.col("daily_net_sales") - F.col("previous_day_net_sales")
            ).cast(MONEY_TYPE),
        )
        .orderBy("full_date")
    )


def build_latest_inventory(inventory: DataFrame) -> DataFrame:
    """Select the most recent valid snapshot for each store/product pair."""

    # This is not arbitrary duplicate resolution. The documented business
    # requirement explicitly asks for the most recent inventory level for each
    # product/store pair. Phase 8 already rejects duplicate snapshot business keys.
    latest_window = Window.partitionBy(
        "store_id",
        "product_id",
    ).orderBy(F.col("snapshot_date").desc())

    return (
        inventory
        # row_number = 1 identifies the newest accepted snapshot inside each
        # store/product group.
        .withColumn("snapshot_recency", F.row_number().over(latest_window))
        .filter(F.col("snapshot_recency") == 1)
        # The helper ranking column is not part of the business dataset.
        .drop("snapshot_recency")
        .orderBy("store_id", "product_id")
    )


def build_low_stock_inventory(inventory: DataFrame) -> DataFrame:
    """Return valid inventory records below the documented reorder threshold."""

    return (
        inventory
        # is_low_stock was derived from the authoritative business rule earlier.
        .filter(F.col("is_low_stock"))
        # Return only the columns needed to inspect which products/stores are
        # below their reorder thresholds.
        .select(
            "snapshot_date",
            "store_id",
            "store_name",
            "product_id",
            "product_name",
            "quantity_on_hand",
            "reorder_level",
        )
        .orderBy("snapshot_date", "store_id", "product_id")
    )
