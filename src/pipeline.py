"""Local orchestration entry point for NullMarket Phase 9."""

from __future__ import annotations

# Path keeps project-relative file-system handling independent of the user's
# current Windows path syntax.
from pathlib import Path

# PyYAML loads the externalized pipeline configuration created in Phase 5.
import yaml

# SparkSession starts the local Spark application. DataFrame is used in type
# hints for orchestration helpers.
from pyspark.sql import DataFrame, SparkSession

# Spark SQL functions support rejected-record formatting and validation checks.
from pyspark.sql import functions as F

# Phase 8 owns source validation. The pipeline consumes its accepted/rejected
# contract instead of reimplementing data-quality rules here.
from src.data_quality import VALIDATION_REASONS_COLUMN, validate_all_sources

# Phase 7 owns the explicit schema contract for every operational source.
from src.schemas import SOURCE_SCHEMAS

# Phase 9 transformation functions contain business/data-shaping logic. Keeping
# these imports separate from orchestration makes the responsibility boundary
# visible: pipeline.py coordinates; transformations.py transforms.
from src.transformations import (
    build_daily_revenue_trend,
    build_dim_date,
    build_dim_product,
    build_dim_store,
    build_fact_inventory_snapshot,
    build_fact_sales,
    build_inventory_dataset,
    build_latest_inventory,
    build_low_stock_inventory,
    build_product_rankings,
    build_sales_dataset,
)


# -----------------------------------------------------------------------------
# CONFIGURATION AND SPARK SETUP
# -----------------------------------------------------------------------------


def load_config(project_root: Path) -> dict:
    """Load external pipeline configuration."""

    # Configuration owns environment/path values so they are not scattered as
    # hard-coded strings throughout the processing code.
    config_path = project_root / "config" / "config.yaml"
    return yaml.safe_load(config_path.read_text(encoding="utf-8"))


def create_spark_session(application_name: str) -> SparkSession:
    """Create the local Spark session used by the Phase 9 pipeline."""

    return (
        SparkSession.builder
        # Add a phase suffix so the application is recognizable in Spark logs.
        .appName(f"{application_name}-Phase9")
        # local[*] uses the available local CPU cores for this development run.
        .master("local[*]")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# SOURCE INGESTION AND LOCAL OUTPUT HELPERS
# -----------------------------------------------------------------------------


def read_source(
    spark: SparkSession,
    raw_path: Path,
    source_name: str,
) -> DataFrame:
    """Read one CSV source with its Phase 7 explicit schema."""

    return (
        spark.read
        # Phase 6 CSVs contain a header row with the documented source names.
        .option("header", "true")
        # Do not reorder source values just because a header exists; schema
        # assignment follows the explicit Phase 7 StructType definition.
        .option("enforceSchema", "false")
        # PERMISSIVE allows malformed typed values to become null so Phase 8 can
        # identify and quarantine them rather than crashing the entire read.
        .option("mode", "PERMISSIVE")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        # Explicit schemas prevent production logic from relying on inference.
        .schema(SOURCE_SCHEMAS[source_name])
        .csv(str(raw_path / f"{source_name}.csv"))
    )


def write_csv(df: DataFrame, path: Path) -> None:
    """Write a Phase 9 local CSV output; Parquet begins in Phase 10."""

    (
        df.write
        # Overwrite makes repeated local development runs replace the previous
        # Phase 9 demonstration output instead of appending duplicate files.
        .mode("overwrite")
        .option("header", "true")
        .csv(str(path))
    )


def prepare_rejected_for_csv(df: DataFrame) -> DataFrame:
    """Flatten validation reasons so rejected records remain inspectable in CSV."""

    # Phase 8 stores validation reasons as ARRAY<STRING>. CSV has no native array
    # type, so join the reasons into one readable text field for this local output.
    return df.withColumn(
        VALIDATION_REASONS_COLUMN,
        F.concat_ws(" | ", F.col(VALIDATION_REASONS_COLUMN)),
    )


# -----------------------------------------------------------------------------
# PIPELINE CORRECTNESS CHECKS
# -----------------------------------------------------------------------------
# These assertions do not replace the future automated tests in Phase 12. They
# are runtime guards for Phase 9 so a successful Spark job cannot silently emit
# facts at the wrong grain or with unreconciled measures.


def assert_unique_grain(
    df: DataFrame,
    key_columns: list[str],
    dataset_name: str,
) -> None:
    """Fail fast if a transformation violates its declared output grain."""

    duplicate_count = (
        df.groupBy(*key_columns)
        # If the declared key represents the grain, every key group must contain
        # exactly one output row.
        .count()
        .filter(F.col("count") > 1)
        # We only need to know whether at least one violation exists, so limit
        # the check before triggering the final count action.
        .limit(1)
        .count()
    )

    if duplicate_count:
        raise ValueError(
            f"{dataset_name} violates declared grain for keys {key_columns}"
        )


def assert_row_count_preserved(
    source_df: DataFrame,
    target_df: DataFrame,
    dataset_name: str,
) -> None:
    """Catch accidental row loss or multiplication across many-to-one joins."""

    # fact_sales should have one row per accepted order line, and the inventory
    # fact should have one row per accepted snapshot. A mismatch signals either
    # row loss or accidental multiplication during enrichment/key lookup joins.
    source_count = source_df.count()
    target_count = target_df.count()

    if source_count != target_count:
        raise ValueError(
            f"{dataset_name} row-count mismatch: "
            f"source={source_count}, target={target_count}"
        )


def assert_sales_measure_reconciliation(
    accepted_order_items: DataFrame,
    accepted_products: DataFrame,
    fact_sales: DataFrame,
) -> None:
    """Reconcile documented sales formulas from accepted source rows to fact_sales."""

    # Independently recompute the three authoritative measures from accepted
    # source data. This guards against an incorrect transformation while avoiding
    # any undocumented order-status filtering.
    expected = (
        accepted_order_items
        .select(
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
        )
        # unit_cost lives on the accepted products source and is required for
        # gross_margin.
        .join(
            accepted_products.select("product_id", "unit_cost"),
            on="product_id",
            how="inner",
        )
        # gross_sales = quantity * unit_price
        .withColumn(
            "gross_sales",
            F.col("quantity") * F.col("unit_price"),
        )
        # net_sales = gross_sales - discount_amount
        .withColumn(
            "net_sales",
            F.col("gross_sales") - F.col("discount_amount"),
        )
        # gross_margin = net_sales - (quantity * unit_cost)
        .withColumn(
            "gross_margin",
            F.col("net_sales")
            - (F.col("quantity") * F.col("unit_cost")),
        )
        # Reconcile aggregate totals rather than relying on a few sample rows.
        .agg(
            F.sum("gross_sales").alias("gross_sales"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        # first() is a Spark action; agg() returns exactly one summary row.
        .first()
    )

    # Calculate the same totals from the generated fact table.
    actual = (
        fact_sales
        .agg(
            F.sum("gross_sales").alias("gross_sales"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        .first()
    )

    # All three documented measures must match accepted-source calculations.
    for measure in ("gross_sales", "net_sales", "gross_margin"):
        if expected[measure] != actual[measure]:
            raise ValueError(
                f"fact_sales {measure} does not reconcile to accepted source data: "
                f"expected={expected[measure]}, actual={actual[measure]}"
            )


# -----------------------------------------------------------------------------
# PHASE 9 ORCHESTRATION
# -----------------------------------------------------------------------------


def main() -> None:
    # __file__ is src/pipeline.py, so parents[1] resolves to the repository root.
    project_root = Path(__file__).resolve().parents[1]
    config = load_config(project_root)

    # Resolve configurable local paths relative to the repository root.
    raw_path = project_root / config["paths"]["raw"]
    curated_path = project_root / config["paths"]["curated"]
    rejected_path = project_root / config["paths"]["rejected"]

    # Orchestration owns Spark lifecycle management; transformation functions do
    # not create their own sessions.
    spark = create_spark_session(config["application"]["name"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read all five raw operational datasets using their explicit schemas.
        sources = {
            name: read_source(spark, raw_path, name)
            for name in SOURCE_SCHEMAS
        }

        # Run the reusable Phase 8 framework once. It validates parent datasets
        # first and returns accepted/rejected views for every source.
        validation = validate_all_sources(
            orders=sources["orders"],
            order_items=sources["order_items"],
            products=sources["products"],
            stores=sources["stores"],
            inventory_snapshots=sources["inventory_snapshots"],
        )

        # Build a simple name -> accepted DataFrame dictionary. Every trusted
        # transformation below receives these accepted records only.
        accepted = {
            name: result.accepted
            for name, result in validation.items()
        }

        # Rejected source rows stay excluded from transformations but remain
        # separately written and inspectable with their validation reasons.
        for name, result in validation.items():
            write_csv(
                prepare_rejected_for_csv(result.rejected),
                rejected_path / name,
            )

        # Build conformed dimensions from accepted parent/business-date data.
        dim_product = build_dim_product(accepted["products"])
        dim_store = build_dim_store(accepted["stores"])
        dim_date = build_dim_date(
            accepted["orders"],
            accepted["inventory_snapshots"],
        )

        # Enrich accepted order lines, calculate documented sales measures, then
        # map date/product/store business identifiers to warehouse keys.
        sales = build_sales_dataset(
            accepted["orders"],
            accepted["order_items"],
            accepted["products"],
            accepted["stores"],
        )
        fact_sales = build_fact_sales(
            sales,
            dim_date,
            dim_product,
            dim_store,
        )

        # Build the inventory business dataset and its independent periodic-
        # snapshot fact. It is intentionally not joined directly to fact_sales.
        inventory = build_inventory_dataset(
            accepted["inventory_snapshots"],
            accepted["products"],
            accepted["stores"],
        )
        fact_inventory_snapshot = build_fact_inventory_snapshot(
            inventory,
            dim_date,
            dim_product,
            dim_store,
        )

        # Verify fact grain before treating the transformation as successful.
        assert_unique_grain(
            fact_sales,
            ["order_id", "line_number"],
            "fact_sales",
        )
        assert_unique_grain(
            fact_inventory_snapshot,
            ["date_key", "store_key", "product_key"],
            "fact_inventory_snapshot",
        )

        # Verify the many-to-one joins preserved one output row per accepted
        # source business record at each fact's declared grain.
        assert_row_count_preserved(
            accepted["order_items"],
            fact_sales,
            "fact_sales",
        )
        assert_row_count_preserved(
            accepted["inventory_snapshots"],
            fact_inventory_snapshot,
            "fact_inventory_snapshot",
        )

        # Independently reconcile the three authoritative sales measures.
        assert_sales_measure_reconciliation(
            accepted["order_items"],
            accepted["products"],
            fact_sales,
        )

        # Phase 9 window/aggregation demonstrations. These remain derived
        # analytical DataFrames rather than additional warehouse fact tables.
        product_rankings = build_product_rankings(fact_sales, dim_product)
        daily_revenue_trend = build_daily_revenue_trend(fact_sales, dim_date)
        latest_inventory = build_latest_inventory(inventory)
        low_stock_inventory = build_low_stock_inventory(inventory)

        # Phase 9 uses local CSV only. Phase 10 replaces this curated write
        # format with Parquet as required by the roadmap.
        outputs = {
            "dim_product": dim_product,
            "dim_store": dim_store,
            "dim_date": dim_date,
            "fact_sales": fact_sales,
            "fact_inventory_snapshot": fact_inventory_snapshot,
        }

        # Persist only the five required warehouse-shaped Phase 9 datasets.
        for name, df in outputs.items():
            write_csv(df, curated_path / name)

        # ---------------------------------------------------------------------
        # EXECUTION REPORTING
        # ---------------------------------------------------------------------
        # These actions make success/reject counts and generated dataset sizes
        # visible so a local run is inspectable rather than silently completing.
        print("\nNullMarket Phase 9 execution summary")
        print("=" * 40)
        for name in SOURCE_SCHEMAS:
            accepted_count = validation[name].accepted.count()
            rejected_count = validation[name].rejected.count()
            print(
                f"{name}: accepted={accepted_count}, rejected={rejected_count}"
            )

        print("\nCurated Phase 9 outputs")
        for name, df in outputs.items():
            print(f"{name}: rows={df.count()}")

        print("\nDerived business demonstrations")
        print(f"product_rankings: rows={product_rankings.count()}")
        print(f"daily_revenue_trend: rows={daily_revenue_trend.count()}")
        print(f"latest_inventory: rows={latest_inventory.count()}")
        print(f"low_stock_inventory: rows={low_stock_inventory.count()}")

        # Show a small sample of the ranking and rolling-revenue outputs so the
        # Phase 9 window-function demonstrations are directly inspectable.
        print("\nTop products by net sales")
        product_rankings.show(10, truncate=False)

        print("\nFirst 10 calendar days of revenue trend")
        daily_revenue_trend.show(10, truncate=False)

        print("\nPhase 9 pipeline completed successfully.")

    finally:
        # Always release the local Spark session, including when an exception is
        # raised by a validation or reconciliation guard above.
        spark.stop()


# Standard Python entry point: importing this module does not execute the
# pipeline, but `python -m src.pipeline` does.
if __name__ == "__main__":
    main()
