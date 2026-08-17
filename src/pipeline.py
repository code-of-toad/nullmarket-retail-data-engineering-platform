"""Local orchestration entry point for NullMarket Phase 10."""

from __future__ import annotations

# pathlib gives us operating-system-independent project path handling.
from pathlib import Path

# PyYAML loads the external configuration established in Phase 5.
import yaml

# SparkSession creates the local Spark application.
# DataFrame is used for type hints on helper functions.
from pyspark.sql import DataFrame, SparkSession

# Spark SQL functions are imported under the conventional alias F.
from pyspark.sql import functions as F

# Phase 8 owns source validation. The pipeline consumes its reusable validation
# interface rather than reimplementing data-quality rules here.
from src.data_quality import VALIDATION_REASONS_COLUMN, validate_all_sources

# Phase 7 owns the explicit source schema contract.
from src.schemas import SOURCE_SCHEMAS

# Phase 9 owns all transformation/business logic.
# Phase 10 deliberately reuses these functions instead of duplicating them.
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

    # Keep environment-specific paths outside processing logic.
    # This lets the same pipeline later move from local paths to cloud paths
    # through configuration rather than duplicated transformation code.
    config_path = project_root / "config" / "config.yaml"

    return yaml.safe_load(
        config_path.read_text(encoding="utf-8")
    )


def create_spark_session(application_name: str) -> SparkSession:
    """Create the local Spark session used by the Phase 10 pipeline."""

    return (
        SparkSession.builder
        # Give the run a recognizable application name in Spark logs.
        .appName(f"{application_name}-Phase10")
        # local[*] uses the available local CPU cores for development.
        .master("local[*]")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# SOURCE INGESTION AND OUTPUT HELPERS
# -----------------------------------------------------------------------------


def read_source(
    spark: SparkSession,
    raw_path: Path,
    source_name: str,
) -> DataFrame:
    """Read one CSV source with its Phase 7 explicit schema."""

    return (
        spark.read
        # Phase 6-generated source CSVs include headers.
        .option("header", "true")
        # Keep Spark from rearranging fields based on the CSV header.
        # The explicit StructType remains the source schema contract.
        .option("enforceSchema", "false")
        # PERMISSIVE allows malformed typed fields to become null so the
        # Phase 8 validation framework can quarantine them.
        .option("mode", "PERMISSIVE")
        # Explicit parsing formats make date/timestamp behavior reproducible.
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        # Never rely on schema inference for the pipeline.
        .schema(SOURCE_SCHEMAS[source_name])
        .csv(str(raw_path / f"{source_name}.csv"))
    )


def write_csv(df: DataFrame, path: Path) -> None:
    """Keep rejected records in the existing inspectable CSV format."""

    (
        df.write
        # Repeated local runs replace the prior rejected output instead of
        # appending duplicate files.
        .mode("overwrite")
        .option("header", "true")
        .csv(str(path))
    )


def prepare_rejected_for_csv(df: DataFrame) -> DataFrame:
    """Flatten validation reasons so rejected records remain inspectable in CSV."""

    # Phase 8 stores validation reasons as ARRAY<STRING>.
    # CSV cannot natively preserve array types, so flatten the messages into
    # one readable text field before writing rejected records.
    return df.withColumn(
        VALIDATION_REASONS_COLUMN,
        F.concat_ws(" | ", F.col(VALIDATION_REASONS_COLUMN)),
    )


def write_parquet(
    df: DataFrame,
    path: Path,
    partition_columns: list[str] | None = None,
) -> None:
    """Write one curated dataset as Parquet, optionally partitioned."""

    # Phase 10 replaces Phase 9's curated CSV output with Parquet.
    # overwrite keeps local reruns deterministic by replacing the old dataset.
    writer = df.write.mode("overwrite")

    # partitionBy() changes the physical directory/file layout only.
    # It does NOT change the DataFrame's logical grain or business meaning.
    #
    # Example:
    #   fact_inventory_snapshot/date_key=20260101/...
    #
    # The records inside that directory still represent the documented
    # date/store/product snapshot grain.
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    # Parquet preserves typed columns and stores data column-wise.
    writer.parquet(str(path))


def read_parquet(
    spark: SparkSession,
    path: Path,
) -> DataFrame:
    """Read one persisted curated Parquet dataset back from storage."""

    # Reading the data back is important: successful write completion alone
    # does not prove that the persisted output still has the expected schema,
    # row count, grain, or calculated values.
    return spark.read.parquet(str(path))


# -----------------------------------------------------------------------------
# PIPELINE CORRECTNESS CHECKS
# -----------------------------------------------------------------------------
# These are runtime correctness guards, not replacements for Phase 12 pytest
# coverage. Their purpose is to prevent a locally successful pipeline from
# silently producing structurally incorrect curated datasets.


def assert_unique_grain(
    df: DataFrame,
    key_columns: list[str],
    dataset_name: str,
) -> None:
    """Fail fast if a dataset violates its declared unique key/grain."""

    duplicate_count = (
        df.groupBy(*key_columns)
        # A declared grain key must identify exactly one row.
        .count()
        .filter(F.col("count") > 1)
        # We only need to know whether at least one violation exists.
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
    """Catch accidental row loss or multiplication."""

    # fact_sales should have one row per accepted order line.
    # fact_inventory_snapshot should have one row per accepted snapshot.
    #
    # If enrichment joins accidentally become one-to-many, the target count
    # would increase. If valid records disappear, the target count would fall.
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
    """Reconcile the documented sales formulas from accepted source rows."""

    # Independently reconstruct the three authoritative sales measures from
    # accepted source data. This gives us a source-to-fact reconciliation
    # instead of merely trusting the transformation that created fact_sales.
    expected = (
        accepted_order_items
        .select(
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
        )
        # unit_cost is required for gross-margin calculation.
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
        # Compare full totals rather than only inspecting sample rows.
        .agg(
            F.sum("gross_sales").alias("gross_sales"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        .first()
    )

    # Calculate the corresponding totals from fact_sales.
    actual = (
        fact_sales
        .agg(
            F.sum("gross_sales").alias("gross_sales"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        .first()
    )

    # Every authoritative measure must match exactly.
    for measure in ("gross_sales", "net_sales", "gross_margin"):
        if expected[measure] != actual[measure]:
            raise ValueError(
                f"fact_sales {measure} does not reconcile to accepted source data: "
                f"expected={expected[measure]}, actual={actual[measure]}"
            )


def schema_type_map(df: DataFrame) -> dict[str, str]:
    """Return column -> Spark data-type mappings."""

    # We intentionally compare names and actual Spark data types while ignoring
    # nullable metadata. Parquet readers may report fields as nullable even when
    # the upstream logical contract was stricter, so nullable flags are not a
    # reliable round-trip equality test here.
    return {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }


def assert_schema_types_preserved(
    expected_df: DataFrame,
    persisted_df: DataFrame,
    dataset_name: str,
) -> None:
    """Verify Parquet preserved all column names and Spark data types."""

    expected = schema_type_map(expected_df)
    actual = schema_type_map(persisted_df)

    if expected != actual:
        raise ValueError(
            f"{dataset_name} schema mismatch after Parquet round trip: "
            f"expected={expected}, actual={actual}"
        )


def assert_round_trip_values(
    expected_df: DataFrame,
    persisted_df: DataFrame,
    dataset_name: str,
) -> None:
    """Verify persisted rows exactly match the in-memory curated output."""

    # Row-count equality is the first persistence check.
    if expected_df.count() != persisted_df.count():
        raise ValueError(
            f"{dataset_name} row count changed after Parquet round trip"
        )

    # A partition column may be reconstructed from the directory structure and
    # returned in a different column position. Re-select the persisted DataFrame
    # in the original column order before comparing complete rows.
    aligned_persisted = persisted_df.select(*expected_df.columns)

    # exceptAll() is deliberately used rather than except().
    # exceptAll() preserves duplicate multiplicity, which makes it appropriate
    # for exact dataset comparison rather than set-only comparison.
    missing_from_persisted = (
        expected_df
        .exceptAll(aligned_persisted)
        .limit(1)
        .count()
    )

    unexpected_in_persisted = (
        aligned_persisted
        .exceptAll(expected_df)
        .limit(1)
        .count()
    )

    # Both directions are required:
    #   expected - persisted catches missing/changed rows;
    #   persisted - expected catches unexpected/changed rows.
    if missing_from_persisted or unexpected_in_persisted:
        raise ValueError(
            f"{dataset_name} values changed after Parquet round trip"
        )


def assert_inventory_semantics(
    fact_inventory_snapshot: DataFrame,
) -> None:
    """Verify persisted inventory values and the low-stock relationship."""

    invalid = (
        fact_inventory_snapshot
        .filter(
            # Inventory measures must remain present after persistence.
            F.col("quantity_on_hand").isNull()
            | F.col("reorder_level").isNull()
            # The documented low-stock rule is:
            # quantity_on_hand < reorder_level
            | (
                F.col("is_low_stock")
                != (F.col("quantity_on_hand") < F.col("reorder_level"))
            )
        )
        .limit(1)
        .count()
    )

    if invalid:
        raise ValueError(
            "fact_inventory_snapshot inventory values/low-stock logic are invalid"
        )


def assert_partition_layout(
    curated_path: Path,
    fact_inventory_snapshot: DataFrame,
) -> list[str]:
    """Verify the intended Phase 10 physical partition layout."""

    inventory_path = curated_path / "fact_inventory_snapshot"

    # Spark's partitionBy("date_key") creates directories such as:
    #   date_key=20260101
    #   date_key=20260115
    #
    # Inspect the actual filesystem so this validates physical layout rather
    # than simply trusting the write configuration.
    inventory_partition_dirs = sorted(
        path.name
        for path in inventory_path.glob("date_key=*")
        if path.is_dir()
    )

    # Every distinct inventory snapshot date should correspond to exactly one
    # top-level date_key partition directory.
    expected_partition_count = (
        fact_inventory_snapshot
        .select("date_key")
        .distinct()
        .count()
    )

    if len(inventory_partition_dirs) != expected_partition_count:
        raise ValueError(
            "fact_inventory_snapshot partition-directory count does not match "
            "its distinct date_key count"
        )

    # fact_sales is intentionally NOT physically partitioned in this small demo.
    # With roughly 1,500 rows spread over roughly 90 dates, date partitioning
    # would mainly create many tiny directories/files rather than demonstrate a
    # sensible layout.
    sales_partition_dirs = [
        path
        for path in (curated_path / "fact_sales").glob("date_key=*")
        if path.is_dir()
    ]

    if sales_partition_dirs:
        raise ValueError(
            "fact_sales should remain unpartitioned in the current demonstration dataset"
        )

    return inventory_partition_dirs


# -----------------------------------------------------------------------------
# PHASE 10 ORCHESTRATION
# -----------------------------------------------------------------------------


def main() -> None:
    # src/pipeline.py -> parents[1] resolves to the repository root.
    project_root = Path(__file__).resolve().parents[1]

    # Load paths/application settings from config rather than hard-coding them.
    config = load_config(project_root)

    raw_path = project_root / config["paths"]["raw"]
    curated_path = project_root / config["paths"]["curated"]
    rejected_path = project_root / config["paths"]["rejected"]

    # The pipeline entry point owns the Spark lifecycle.
    spark = create_spark_session(config["application"]["name"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---------------------------------------------------------------------
        # EXISTING PHASE 7-9 LOGIC: INGEST -> VALIDATE -> TRANSFORM
        # ---------------------------------------------------------------------
        # Phase 10 is a storage-layer phase. We deliberately reuse the already
        # validated Phase 9 transformation path instead of creating a second
        # transformation implementation.

        # Read all five raw operational datasets with explicit schemas.
        sources = {
            name: read_source(spark, raw_path, name)
            for name in SOURCE_SCHEMAS
        }

        # Apply the reusable Phase 8 validation framework.
        validation = validate_all_sources(
            orders=sources["orders"],
            order_items=sources["order_items"],
            products=sources["products"],
            stores=sources["stores"],
            inventory_snapshots=sources["inventory_snapshots"],
        )

        # Only accepted source records can flow into trusted transformations.
        accepted = {
            name: result.accepted
            for name, result in validation.items()
        }

        # Rejected records remain separately persisted with validation reasons.
        # Phase 10 changes only the CURATED layer to Parquet; it does not require
        # us to redesign rejected-record storage.
        for name, result in validation.items():
            write_csv(
                prepare_rejected_for_csv(result.rejected),
                rejected_path / name,
            )

        # ---------------------------------------------------------------------
        # REUSE PHASE 9 DIMENSION TRANSFORMATIONS
        # ---------------------------------------------------------------------

        # dim_product grain: one row per accepted product.
        dim_product = build_dim_product(accepted["products"])

        # dim_store grain: one row per accepted store.
        dim_store = build_dim_store(accepted["stores"])

        # dim_date grain: one row per calendar date across the required range.
        dim_date = build_dim_date(
            accepted["orders"],
            accepted["inventory_snapshots"],
        )

        # ---------------------------------------------------------------------
        # REUSE PHASE 9 SALES TRANSFORMATIONS
        # ---------------------------------------------------------------------

        # Build the enriched sales DataFrame from accepted sources.
        # No order_status filter is introduced here or in Phase 10.
        sales = build_sales_dataset(
            accepted["orders"],
            accepted["order_items"],
            accepted["products"],
            accepted["stores"],
        )

        # fact_sales grain remains:
        # one row per validated order line,
        # uniquely identified by (order_id, line_number).
        fact_sales = build_fact_sales(
            sales,
            dim_date,
            dim_product,
            dim_store,
        )

        # ---------------------------------------------------------------------
        # REUSE PHASE 9 INVENTORY TRANSFORMATIONS
        # ---------------------------------------------------------------------

        # Build the enriched inventory dataset at snapshot grain.
        inventory = build_inventory_dataset(
            accepted["inventory_snapshots"],
            accepted["products"],
            accepted["stores"],
        )

        # fact_inventory_snapshot grain remains:
        # one row per date/store/product snapshot.
        fact_inventory_snapshot = build_fact_inventory_snapshot(
            inventory,
            dim_date,
            dim_product,
            dim_store,
        )

        # ---------------------------------------------------------------------
        # KEEP PHASE 9 CORRECTNESS GUARDS
        # ---------------------------------------------------------------------

        # Preserve the exact fact grains established before Parquet is involved.
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

        # Verify dimension enrichment did not multiply or discard fact rows.
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

        # Reconcile sales measures before persistence.
        assert_sales_measure_reconciliation(
            accepted["order_items"],
            accepted["products"],
            fact_sales,
        )

        # ---------------------------------------------------------------------
        # KEEP PHASE 9 ANALYTICAL DEMONSTRATIONS
        # ---------------------------------------------------------------------
        # These DataFrames demonstrate aggregation/window functionality.
        # They are not additional warehouse tables and are not persisted as
        # Phase 10 curated outputs.

        product_rankings = build_product_rankings(
            fact_sales,
            dim_product,
        )

        daily_revenue_trend = build_daily_revenue_trend(
            fact_sales,
            dim_date,
        )

        latest_inventory = build_latest_inventory(inventory)
        low_stock_inventory = build_low_stock_inventory(inventory)

        # These are the five authoritative warehouse-shaped curated datasets.
        outputs = {
            "dim_product": dim_product,
            "dim_store": dim_store,
            "dim_date": dim_date,
            "fact_sales": fact_sales,
            "fact_inventory_snapshot": fact_inventory_snapshot,
        }

        # ---------------------------------------------------------------------
        # STEP 10.1 — WRITE CURATED DATA AS PARQUET
        # STEP 10.2 — APPLY LOGICAL PARTITIONING WHERE JUSTIFIED
        # ---------------------------------------------------------------------
        #
        # Why Parquet?
        # - Columnar storage: analytical workloads can read only needed columns.
        # - Compression: values of similar types are stored together and often
        #   compress efficiently.
        # - Schema preservation: typed columns are stored with the dataset.
        # - Predicate pushdown support: readers can use Parquet metadata/statistics
        #   to avoid processing some irrelevant row groups.
        #
        # Why not partition every dataset?
        # Physical partitioning creates directories/files. Too many partition
        # values can create excessive small files and metadata overhead.
        # Partitioning should follow likely access patterns AND sensible
        # cardinality, not be added automatically.

        partitioning = {
            # Both facts are date-oriented candidates conceptually.
            #
            # However, the current inventory data has only seven snapshot dates,
            # so date_key provides a low-cardinality demonstration of physical
            # date partitioning:
            #
            # fact_inventory_snapshot/
            #   date_key=20260101/
            #   date_key=20260115/
            #   ...
            #
            # This is a data-layout demonstration, NOT a measured performance
            # improvement claim.
            "fact_inventory_snapshot": ["date_key"],
        }

        # fact_sales is deliberately absent from the mapping.
        #
        # Although sales is frequently filtered by date in real analytical
        # systems, the current demonstration has roughly 1,500 fact rows across
        # roughly 90 dates. Partitioning such a small dataset by date_key would
        # mainly create many tiny partitions/files and demonstrate poor physical
        # design rather than useful optimization.
        for name, df in outputs.items():
            write_parquet(
                df,
                curated_path / name,
                partitioning.get(name),
            )

        # ---------------------------------------------------------------------
        # STEP 10.3 — READ EVERY CURATED DATASET BACK FROM PARQUET
        # ---------------------------------------------------------------------
        #
        # This creates NEW DataFrames from the persisted files. Downstream
        # validation therefore checks what was actually written to storage,
        # not merely the in-memory DataFrames produced before the write.

        persisted = {
            name: read_parquet(
                spark,
                curated_path / name,
            )
            for name in outputs
        }

        # ---------------------------------------------------------------------
        # VERIFY PERSISTED SCHEMAS, ROW COUNTS, AND VALUES
        # ---------------------------------------------------------------------

        for name in outputs:
            # Confirm Parquet preserved the expected column names and data types.
            assert_schema_types_preserved(
                outputs[name],
                persisted[name],
                name,
            )

            # Confirm the complete persisted rows match the in-memory curated
            # output exactly. This also verifies row counts.
            assert_round_trip_values(
                outputs[name],
                persisted[name],
                name,
            )

        # ---------------------------------------------------------------------
        # VERIFY DECLARED DIMENSION AND FACT GRAINS AFTER READ-BACK
        # ---------------------------------------------------------------------
        #
        # Partitioning fact_inventory_snapshot by date_key must NOT change its
        # logical grain. Spark reconstructs date_key from the partition directory
        # when the dataset is read from its base path.

        readback_unique_keys = {
            # One row per product; both surrogate and business keys are unique.
            "dim_product": [
                ["product_key"],
                ["product_id"],
            ],

            # One row per store; both surrogate and business keys are unique.
            "dim_store": [
                ["store_key"],
                ["store_id"],
            ],

            # One row per calendar date.
            "dim_date": [
                ["date_key"],
                ["full_date"],
            ],

            # One row per validated order line.
            "fact_sales": [
                ["order_id", "line_number"],
            ],

            # One row per validated date/store/product inventory snapshot.
            "fact_inventory_snapshot": [
                ["date_key", "store_key", "product_key"],
            ],
        }

        for name, key_sets in readback_unique_keys.items():
            for key_columns in key_sets:
                assert_unique_grain(
                    persisted[name],
                    key_columns,
                    name,
                )

        # ---------------------------------------------------------------------
        # VERIFY PERSISTED BUSINESS VALUES
        # ---------------------------------------------------------------------

        # Re-run the source-to-fact reconciliation against the Parquet data.
        # This explicitly verifies that persistence did not alter:
        #
        # gross_sales = quantity * unit_price
        # net_sales   = gross_sales - discount_amount
        # gross_margin = net_sales - (quantity * unit_cost)
        assert_sales_measure_reconciliation(
            accepted["order_items"],
            accepted["products"],
            persisted["fact_sales"],
        )

        # Verify inventory measures and the low-stock calculation after read-back.
        assert_inventory_semantics(
            persisted["fact_inventory_snapshot"]
        )

        # Inspect the actual filesystem to confirm our intended physical layout:
        # inventory partitioned by date_key, sales left unpartitioned.
        inventory_partition_dirs = assert_partition_layout(
            curated_path,
            persisted["fact_inventory_snapshot"],
        )

        # ---------------------------------------------------------------------
        # EXECUTION REPORTING
        # ---------------------------------------------------------------------
        # Keep local execution inspectable. These counts make it obvious how many
        # records passed validation and what was written/read back.

        print("\nNullMarket Phase 10 execution summary")
        print("=" * 40)

        for name in SOURCE_SCHEMAS:
            accepted_count = validation[name].accepted.count()
            rejected_count = validation[name].rejected.count()

            print(
                f"{name}: accepted={accepted_count}, rejected={rejected_count}"
            )

        print("\nCurated Parquet read-back verification")

        for name, df in persisted.items():
            print(
                f"{name}: rows={df.count()}, "
                f"schema=PASS, grain=PASS, values=PASS"
            )

        print("\nPhysical partition layout")

        # fact_sales intentionally demonstrates the decision NOT to partition a
        # small date-oriented dataset when the partition cardinality is too high
        # relative to its row count.
        print("fact_sales: unpartitioned")

        # fact_inventory_snapshot demonstrates low-cardinality date partitioning.
        print("fact_inventory_snapshot: partitioned by date_key")

        for directory in inventory_partition_dirs:
            print(f"  {directory}")

        print("\nDerived business demonstrations")
        print(f"product_rankings: rows={product_rankings.count()}")
        print(f"daily_revenue_trend: rows={daily_revenue_trend.count()}")
        print(f"latest_inventory: rows={latest_inventory.count()}")
        print(f"low_stock_inventory: rows={low_stock_inventory.count()}")

        print("\nPhase 10 pipeline completed successfully.")

    finally:
        # Always stop Spark, including when one of the validation guards raises
        # an exception.
        spark.stop()


# Standard Python module entry point:
# importing src.pipeline will not execute the pipeline,
# while `python -m src.pipeline` will.
if __name__ == "__main__":
    main()
