"""Phase 17 incremental Spark orchestration for NullMarket.

This entry point processes only the new Phase 17 transactional/snapshot batch
while reusing the existing product/store reference data and the established
schema, validation, and transformation modules.

It deliberately writes to batch-scoped curated/rejected paths so the historical
Phase 15/16 curated layer is not overwritten.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.data_quality import VALIDATION_REASONS_COLUMN, validate_all_sources
from src.schemas import SOURCE_SCHEMAS

# Reuse the existing Phase 15 persistence/correctness helpers rather than
# creating competing implementations of Parquet writing and validation.
from src.pipeline import (
    assert_inventory_semantics,
    assert_round_trip_values,
    assert_row_count_preserved,
    assert_sales_measure_reconciliation,
    assert_schema_types_preserved,
    assert_unique_grain,
    prepare_rejected_for_csv,
    read_parquet,
    write_csv,
    write_parquet,
)

# Business transformation logic remains exactly the same as the established
# full-refresh pipeline. Phase 17 changes only which source rows are processed
# and where the resulting incremental batch is written.
from src.transformations import (
    build_dim_date,
    build_dim_product,
    build_dim_store,
    build_fact_inventory_snapshot,
    build_fact_sales,
    build_inventory_dataset,
    build_sales_dataset,
)


# -----------------------------------------------------------------------------
# STORAGE HELPERS
# -----------------------------------------------------------------------------


def join_storage_path(base: str, *parts: str) -> str:
    """Join a local filesystem path or a gs:// URI safely."""

    # pathlib handles Windows local paths correctly.
    if not base.startswith("gs://"):
        return str(Path(base).joinpath(*parts))

    # Object-storage URIs are not local filesystem paths, so join them manually.
    return "/".join(
        [base.rstrip("/")]
        + [part.strip("/") for part in parts if part]
    )


def incremental_source_file_path(
    incremental_raw_path: str,
    source_name: str,
) -> str:
    """Return one Phase 17 batch source file.

    The deterministic Phase 17 generator always creates:
        <batch-root>/<dataset>/<dataset>.csv
    """

    return join_storage_path(
        incremental_raw_path,
        source_name,
        f"{source_name}.csv",
    )


def reference_source_file_path(
    reference_raw_path: str,
    source_name: str,
) -> str:
    """Return an unchanged product/store reference source file.

    The existing local Phase 6 raw layout is flat:
        data/raw/products.csv

    The established GCS Phase 14 layout is nested:
        raw/products/products.csv
    """

    if reference_raw_path.startswith("gs://"):
        return join_storage_path(
            reference_raw_path,
            source_name,
            f"{source_name}.csv",
        )

    return join_storage_path(
        reference_raw_path,
        f"{source_name}.csv",
    )


# -----------------------------------------------------------------------------
# SPARK SETUP AND SOURCE READING
# -----------------------------------------------------------------------------


def create_spark_session(
    application_name: str,
    environment: str,
) -> SparkSession:
    """Create Spark locally or attach to the managed Spark master."""

    builder = SparkSession.builder.appName(
        f"{application_name}-Phase17-Incremental"
    )

    # Local development explicitly uses local CPU cores. Managed Spark supplies
    # its own master configuration, so we must not force local[*] in GCP.
    if environment == "local":
        builder = builder.master("local[*]")

    return builder.getOrCreate()


def read_csv_with_schema(
    spark: SparkSession,
    file_path: str,
    source_name: str,
) -> DataFrame:
    """Read one source CSV with the existing explicit Phase 7 schema."""

    return (
        spark.read
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(SOURCE_SCHEMAS[source_name])
        .csv(file_path)
    )


# -----------------------------------------------------------------------------
# ARGUMENTS
# -----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse the explicit Phase 17 batch paths."""

    parser = argparse.ArgumentParser(
        description="Run the NullMarket Phase 17 incremental Spark batch"
    )

    parser.add_argument(
        "--environment",
        choices=("local", "gcp"),
        required=True,
    )
    parser.add_argument(
        "--application-name",
        default="NullMarket",
    )

    # New orders/order_items/inventory live under this batch-specific root.
    parser.add_argument(
        "--incremental-raw-path",
        required=True,
    )

    # Existing products/stores are reference data required for FK validation,
    # enrichment, unit_cost lookup, and deterministic warehouse-key mapping.
    parser.add_argument(
        "--reference-raw-path",
        required=True,
    )

    # Existing baseline curated dimensions are read only to prove that the
    # product/store surrogate-key mappings are unchanged.
    parser.add_argument(
        "--baseline-curated-path",
        required=True,
    )

    # These MUST be batch-specific locations. The script never writes to the
    # historical base curated/rejected paths.
    parser.add_argument(
        "--curated-path",
        required=True,
    )
    parser.add_argument(
        "--rejected-path",
        required=True,
    )

    return parser.parse_args()


# -----------------------------------------------------------------------------
# PHASE 17-SPECIFIC ASSERTIONS
# -----------------------------------------------------------------------------


def assert_expected_incremental_counts(
    sources: dict[str, DataFrame],
) -> None:
    """Verify we are processing exactly the deterministic Phase 17 batch."""

    expected_counts = {
        "orders": 6,
        "order_items": 12,
        "inventory_snapshots": 12,
    }

    for name, expected_count in expected_counts.items():
        actual_count = sources[name].count()

        if actual_count != expected_count:
            raise ValueError(
                f"{name} incremental row-count mismatch: "
                f"expected={expected_count}, actual={actual_count}"
            )


def assert_no_incremental_rejections(
    validation,
) -> None:
    """Require every genuinely new transactional/snapshot row to be valid."""

    # Product reference data contains the known deterministic Phase 6 duplicate
    # defect, so it is intentionally NOT included in this zero-rejection check.
    # The new Phase 17 business records themselves must all pass validation.
    for name in ("orders", "order_items", "inventory_snapshots"):
        rejected_count = validation[name].rejected.count()

        if rejected_count != 0:
            raise ValueError(
                f"{name} incremental batch unexpectedly rejected "
                f"{rejected_count} row(s)"
            )


def assert_incremental_date_range(
    dim_date: DataFrame,
) -> None:
    """Verify the batch produces only the intended new April 2026 dates."""

    bounds = (
        dim_date
        .agg(
            F.min("full_date").alias("min_date"),
            F.max("full_date").alias("max_date"),
            F.count("*").alias("date_rows"),
        )
        .first()
    )

    # The deterministic batch contains sales on April 1-3 and an inventory
    # snapshot on April 4. build_dim_date therefore correctly produces the
    # continuous four-day range April 1 through April 4.
    if (
        str(bounds["min_date"]) != "2026-04-01"
        or str(bounds["max_date"]) != "2026-04-04"
        or bounds["date_rows"] != 4
    ):
        raise ValueError(
            "Incremental dim_date did not produce the expected "
            "2026-04-01 through 2026-04-04 range"
        )


def assert_reference_dimension_stability(
    spark: SparkSession,
    baseline_curated_path: str,
    dim_product: DataFrame,
    dim_store: DataFrame,
) -> None:
    """Prove product/store surrogate-key mappings match the existing baseline."""

    baseline_product = read_parquet(
        spark,
        join_storage_path(baseline_curated_path, "dim_product"),
    )
    baseline_store = read_parquet(
        spark,
        join_storage_path(baseline_curated_path, "dim_store"),
    )

    # Schema and complete-row equality prove the reference dimensions generated
    # for this incremental batch assign the same surrogate keys as the existing
    # curated warehouse dimensions.
    assert_schema_types_preserved(
        dim_product,
        baseline_product,
        "dim_product reference mapping",
    )
    assert_round_trip_values(
        dim_product,
        baseline_product,
        "dim_product reference mapping",
    )

    assert_schema_types_preserved(
        dim_store,
        baseline_store,
        "dim_store reference mapping",
    )
    assert_round_trip_values(
        dim_store,
        baseline_store,
        "dim_store reference mapping",
    )


# -----------------------------------------------------------------------------
# PHASE 17 INCREMENTAL ORCHESTRATION
# -----------------------------------------------------------------------------


def main() -> None:
    """Validate, transform, and persist only the Phase 17 incremental batch."""

    args = parse_args()

    spark = create_spark_session(
        args.application_name,
        args.environment,
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---------------------------------------------------------------------
        # 1. READ ONLY NEW TRANSACTIONAL/SNAPSHOT DATA
        # ---------------------------------------------------------------------
        # These three datasets contain only the Phase 17 batch. Historical
        # orders/order_items/inventory are deliberately not read or transformed.
        sources = {
            "orders": read_csv_with_schema(
                spark,
                incremental_source_file_path(
                    args.incremental_raw_path,
                    "orders",
                ),
                "orders",
            ),
            "order_items": read_csv_with_schema(
                spark,
                incremental_source_file_path(
                    args.incremental_raw_path,
                    "order_items",
                ),
                "order_items",
            ),
            "inventory_snapshots": read_csv_with_schema(
                spark,
                incremental_source_file_path(
                    args.incremental_raw_path,
                    "inventory_snapshots",
                ),
                "inventory_snapshots",
            ),
        }

        # ---------------------------------------------------------------------
        # 2. READ ONLY THE REFERENCE DATA REQUIRED BY THE TRANSFORMATIONS
        # ---------------------------------------------------------------------
        # products and stores are small parent/reference datasets. They are
        # required to validate foreign keys, obtain product unit_cost, enrich the
        # new records, and recreate the existing deterministic surrogate-key
        # mappings. Reading them does NOT rebuild historical facts.
        sources["products"] = read_csv_with_schema(
            spark,
            reference_source_file_path(
                args.reference_raw_path,
                "products",
            ),
            "products",
        )
        sources["stores"] = read_csv_with_schema(
            spark,
            reference_source_file_path(
                args.reference_raw_path,
                "stores",
            ),
            "stores",
        )

        # Make the batch boundary observable and fail if the wrong source path
        # was supplied accidentally.
        assert_expected_incremental_counts(sources)

        # ---------------------------------------------------------------------
        # 3. REUSE THE EXISTING DATA-QUALITY FRAMEWORK
        # ---------------------------------------------------------------------
        validation = validate_all_sources(
            orders=sources["orders"],
            order_items=sources["order_items"],
            products=sources["products"],
            stores=sources["stores"],
            inventory_snapshots=sources["inventory_snapshots"],
        )

        accepted = {
            name: result.accepted
            for name, result in validation.items()
        }

        # The deterministic new business records must all be accepted.
        assert_no_incremental_rejections(validation)

        # Keep rejected data inspectable in a batch-scoped location. The known
        # historical product duplicate defect may still appear here because the
        # product catalogue is reused as reference data.
        for name, result in validation.items():
            write_csv(
                prepare_rejected_for_csv(result.rejected),
                join_storage_path(args.rejected_path, name),
            )

        # ---------------------------------------------------------------------
        # 4. RECREATE ONLY THE REFERENCE DIMENSIONS NEEDED FOR KEY LOOKUPS
        # ---------------------------------------------------------------------
        # These are in-memory lookup DataFrames. Product/store dimensions are NOT
        # written as incremental warehouse outputs because this batch introduces
        # no product or store changes.
        dim_product = build_dim_product(accepted["products"])
        dim_store = build_dim_store(accepted["stores"])

        # Prove the deterministic surrogate-key mapping still matches the
        # existing curated baseline before those keys are placed into new facts.
        assert_reference_dimension_stability(
            spark,
            args.baseline_curated_path,
            dim_product,
            dim_store,
        )

        # dim_date is different: the April dates are genuinely new dimension
        # members and must be persisted for the later BigQuery MERGE.
        dim_date = build_dim_date(
            accepted["orders"],
            accepted["inventory_snapshots"],
        )
        assert_incremental_date_range(dim_date)

        # ---------------------------------------------------------------------
        # 5. REUSE THE EXISTING SALES TRANSFORMATION
        # ---------------------------------------------------------------------
        # No order_status filter is introduced. The authoritative requirements
        # define no status-based revenue inclusion/exclusion rule.
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

        # ---------------------------------------------------------------------
        # 6. REUSE THE EXISTING INVENTORY TRANSFORMATION
        # ---------------------------------------------------------------------
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

        # ---------------------------------------------------------------------
        # 7. VERIFY FACT GRAIN, ROW PRESERVATION, AND BUSINESS CALCULATIONS
        # ---------------------------------------------------------------------
        assert_unique_grain(
            fact_sales,
            ["order_id", "line_number"],
            "incremental fact_sales",
        )
        assert_unique_grain(
            fact_inventory_snapshot,
            ["date_key", "store_key", "product_key"],
            "incremental fact_inventory_snapshot",
        )
        assert_unique_grain(
            dim_date,
            ["date_key"],
            "incremental dim_date",
        )

        assert_row_count_preserved(
            accepted["order_items"],
            fact_sales,
            "incremental fact_sales",
        )
        assert_row_count_preserved(
            accepted["inventory_snapshots"],
            fact_inventory_snapshot,
            "incremental fact_inventory_snapshot",
        )

        assert_sales_measure_reconciliation(
            accepted["order_items"],
            accepted["products"],
            fact_sales,
        )
        assert_inventory_semantics(fact_inventory_snapshot)

        # ---------------------------------------------------------------------
        # 8. WRITE ONLY THE INCREMENTAL WAREHOUSE-SHAPED OUTPUTS
        # ---------------------------------------------------------------------
        # Crucially, args.curated_path is the batch-scoped path:
        #
        #   curated/incremental/phase17_batch_01/
        #
        # Therefore overwrite semantics are safe and retryable: rerunning this
        # same batch replaces only this batch's staged Parquet, not historical
        # curated data.
        outputs = {
            "dim_date": dim_date,
            "fact_sales": fact_sales,
            "fact_inventory_snapshot": fact_inventory_snapshot,
        }

        for name, df in outputs.items():
            write_parquet(
                df,
                join_storage_path(args.curated_path, name),
                # Preserve the established inventory physical layout.
                ["date_key"] if name == "fact_inventory_snapshot" else None,
            )

        # ---------------------------------------------------------------------
        # 9. READ THE BATCH-SCOPED PARQUET BACK AND VERIFY EXACT PERSISTENCE
        # ---------------------------------------------------------------------
        persisted = {
            name: read_parquet(
                spark,
                join_storage_path(args.curated_path, name),
            )
            for name in outputs
        }

        for name in outputs:
            assert_schema_types_preserved(
                outputs[name],
                persisted[name],
                name,
            )
            assert_round_trip_values(
                outputs[name],
                persisted[name],
                name,
            )

        assert_unique_grain(
            persisted["dim_date"],
            ["date_key"],
            "persisted incremental dim_date",
        )
        assert_unique_grain(
            persisted["fact_sales"],
            ["order_id", "line_number"],
            "persisted incremental fact_sales",
        )
        assert_unique_grain(
            persisted["fact_inventory_snapshot"],
            ["date_key", "store_key", "product_key"],
            "persisted incremental fact_inventory_snapshot",
        )

        assert_sales_measure_reconciliation(
            accepted["order_items"],
            accepted["products"],
            persisted["fact_sales"],
        )
        assert_inventory_semantics(
            persisted["fact_inventory_snapshot"]
        )

        # ---------------------------------------------------------------------
        # 10. EXECUTION SUMMARY
        # ---------------------------------------------------------------------
        print("\nNullMarket Phase 17 incremental execution summary")
        print("=" * 52)
        print(f"environment: {args.environment}")
        print(f"incremental raw input: {args.incremental_raw_path}")
        print(f"reference raw input: {args.reference_raw_path}")
        print(f"baseline curated reference: {args.baseline_curated_path}")
        print(f"incremental curated output: {args.curated_path}")
        print(f"incremental rejected output: {args.rejected_path}")

        print("\nSource validation")
        for name in (
            "orders",
            "order_items",
            "inventory_snapshots",
            "products",
            "stores",
        ):
            print(
                f"{name}: "
                f"accepted={validation[name].accepted.count()}, "
                f"rejected={validation[name].rejected.count()}"
            )

        print("\nIncremental curated Parquet")
        print(
            f"dim_date: rows={persisted['dim_date'].count()}, "
            "schema=PASS, grain=PASS, values=PASS"
        )
        print(
            f"fact_sales: rows={persisted['fact_sales'].count()}, "
            "schema=PASS, grain=PASS, values=PASS"
        )
        print(
            "fact_inventory_snapshot: "
            f"rows={persisted['fact_inventory_snapshot'].count()}, "
            "schema=PASS, grain=PASS, values=PASS"
        )

        print("\nReference-key stability")
        print("dim_product surrogate-key mapping: PASS")
        print("dim_store surrogate-key mapping: PASS")

        print("\nHistorical curated fact data was not read or overwritten.")
        print("Phase 17 incremental Spark batch completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
