"""Reproducible Spark performance demonstrations for NullMarket Phase 11.

This module reads the Phase 10 curated Parquet outputs and inspects real
NullMarket operations. It does not modify or rewrite curated data.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Reuse Phase 9 business transformations so the window demonstrations inspect
# real NullMarket logic instead of unrelated toy examples.
from src.transformations import (
    build_daily_revenue_trend,
    build_product_rankings,
)


# -----------------------------------------------------------------------------
# SETUP HELPERS
# -----------------------------------------------------------------------------


def load_config(project_root: Path) -> dict:
    """Load the same project configuration used by the main pipeline."""

    # Keeping path configuration externalized preserves the Phase 5 design:
    # demonstrations should not hard-code machine-specific repository paths.
    config_path = project_root / "config" / "config.yaml"
    return yaml.safe_load(config_path.read_text(encoding="utf-8"))


def create_spark_session(application_name: str) -> SparkSession:
    """Create the local Spark application used only for Phase 11 inspection."""

    return (
        SparkSession.builder
        # Use a distinct application name so Phase 11 activity is recognizable
        # in Spark logs and the Spark UI when it is inspected locally.
        .appName(f"{application_name}-Phase11-Performance")
        # local[*] lets local Spark use the machine's available CPU cores.
        .master("local[*]")
        .getOrCreate()
    )


def section(title: str) -> None:
    """Print a readable separator between Phase 11 experiments."""

    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_partition_count(label: str, df: DataFrame) -> None:
    """Print the number of Spark DataFrame partitions backing a DataFrame."""

    # A DataFrame partition is a unit of distributed work. Spark generally runs
    # one task per partition within a stage, so partition count directly affects
    # available parallelism and task overhead.
    print(f"{label}: {df.rdd.getNumPartitions()} partition(s)")


# -----------------------------------------------------------------------------
# PHASE 11 DEMONSTRATIONS
# -----------------------------------------------------------------------------


def inspect_narrow_transformations(fact_sales: DataFrame) -> None:
    """Inspect select/filter/withColumn operations that do not require shuffling."""

    section("11.1 / 11.3 — Narrow transformations: filter + select + withColumn")

    narrow_df = (
        fact_sales
        # filter() can evaluate each input partition independently.
        .filter(F.col("net_sales") > 0)
        # withColumn() derives a value from columns already present in each row;
        # rows do not need to move between partitions.
        .withColumn(
            "margin_rate",
            F.when(
                F.col("net_sales") != 0,
                F.col("gross_margin") / F.col("net_sales"),
            ),
        )
        # select() projects columns and is also normally a narrow operation.
        .select(
            "order_id",
            "line_number",
            "product_key",
            "net_sales",
            "gross_margin",
            "margin_rate",
        )
    )

    # "extended" prints parsed, analyzed, and optimized logical plans plus the
    # physical execution plan. This is the best mode here for distinguishing
    # what Spark understands logically from how it chooses to execute it.
    narrow_df.explain(mode="extended")

    print(
        "\nStudy cue: this chain should not need an Exchange solely because of "
        "filter/select/withColumn. Look for the Parquet scan, filters, and projections."
    )


def inspect_filter_pushdown_and_partition_pruning(
    fact_inventory_snapshot: DataFrame,
) -> None:
    """Connect Phase 10 Parquet concepts to an actual filtered read plan."""

    section("11.1 — Parquet filter / partition-pruning inspection")

    # Choose one real partition value without hard-coding a business date.
    # first() is an action and therefore triggers Spark execution.
    sample_date_key = (
        fact_inventory_snapshot
        .select("date_key")
        .distinct()
        .orderBy("date_key")
        .first()["date_key"]
    )

    filtered_inventory = fact_inventory_snapshot.filter(
        F.col("date_key") == sample_date_key
    )

    filtered_inventory.explain(mode="formatted")

    print(f"\nInspected date_key: {sample_date_key}")
    print(
        "Study cue: in the Parquet scan, look for PartitionFilters. "
        "That is evidence Spark can use the Phase 10 date_key directory layout."
    )



def inspect_join_plan(
    fact_sales: DataFrame,
    dim_product: DataFrame,
) -> None:
    """Inspect a real fact-to-dimension join using Spark's normal optimizer."""

    section("11.1 — Join execution plan: fact_sales -> dim_product")

    product_lookup = dim_product.select(
        "product_key",
        "product_id",
        "product_name",
        "category",
    )

    joined = fact_sales.join(
        product_lookup,
        on="product_key",
        how="inner",
    )

    # Do not force a join strategy in Step 11.1. The point here is to inspect
    # what Spark's optimizer naturally chooses for the current data/configuration.
    joined.explain(mode="formatted")

    print(
        "\nStudy cue: identify the chosen join operator. Because dim_product is "
        "genuinely small, Spark may automatically choose a broadcast join. "
        "Step 11.4 will disable auto-broadcast to create a controlled comparison."
    )


def inspect_aggregation(fact_sales: DataFrame) -> DataFrame:
    """Inspect a real product-level sales aggregation that requires a shuffle."""

    section("11.1 / 11.3 — Wide transformation: product aggregation")

    product_revenue = (
        fact_sales
        # Rows for the same product may begin in different input partitions.
        # groupBy therefore requires Spark to redistribute rows by product_key.
        .groupBy("product_key")
        .agg(F.sum("net_sales").alias("net_sales"))
    )

    product_revenue.explain(mode="formatted")

    print(
        "\nStudy cue: look for Exchange with hash partitioning plus partial/final "
        "HashAggregate operators. The Exchange marks the shuffle boundary."
    )

    return product_revenue


def inspect_project_windows(
    fact_sales: DataFrame,
    dim_product: DataFrame,
    dim_date: DataFrame,
) -> None:
    """Inspect the real NullMarket ranking and rolling-revenue windows."""

    section("11.1 / 11.3 — Window operations from NullMarket transformations")

    # build_product_rankings() first aggregates by product and then globally
    # ranks all products by revenue. Its Window.orderBy(...) intentionally has
    # no partitionBy(...) because the business question is a global ranking.
    product_rankings = build_product_rankings(
        fact_sales,
        dim_product,
    )

    print("\nProduct-ranking physical plan")
    product_rankings.explain(mode="formatted")

    # build_daily_revenue_trend() creates daily revenue, then calculates a
    # seven-day rolling sum and lag over one global chronological sequence.
    daily_revenue_trend = build_daily_revenue_trend(
        fact_sales,
        dim_date,
    )

    print("\nDaily-revenue trend physical plan")
    daily_revenue_trend.explain(mode="formatted")

    print(
        "\nStudy cue: a global Window.orderBy(...) has no partitionBy(...). "
        "Spark may show Exchange SinglePartition and emit the "
        "'No Partition Defined for Window operation' warning because all rows "
        "must participate in one ordered window."
    )


def inspect_partitioning(fact_sales: DataFrame) -> None:
    """Compare current partitions, repartition(), and coalesce()."""

    section("11.2 — DataFrame partition-count experiment")

    show_partition_count("Phase 10 fact_sales read", fact_sales)

    # repartition() can increase or decrease partition count and performs a
    # shuffle. Partitioning by product_key demonstrates intentional redistribution
    # so rows with the same key are routed consistently.
    repartitioned = fact_sales.repartition(8, "product_key")
    show_partition_count("repartition(8, 'product_key')", repartitioned)
    print("\nrepartition() plan")
    repartitioned.explain(mode="formatted")

    # coalesce() is primarily useful for reducing partition count without a full
    # shuffle. This can be cheaper, but the resulting partitions may be uneven.
    coalesced = repartitioned.coalesce(2)
    show_partition_count("repartitioned.coalesce(2)", coalesced)
    print("\ncoalesce() plan")
    coalesced.explain(mode="formatted")

    print(
        "\nStudy cue: repartition() should show an Exchange because data moves "
        "between partitions. coalesce() should show Coalesce and avoids a full "
        "redistribution when reducing partitions."
    )


def inspect_shuffle_vs_broadcast_join(
    spark: SparkSession,
    fact_sales: DataFrame,
    dim_product: DataFrame,
) -> None:
    """Compare a forced shuffle join with an explicit broadcast join."""

    section("11.4 — Shuffle join versus broadcast join")

    # Select only the dimension attributes actually needed by this example.
    # Keeping the broadcast side narrow is good engineering because broadcast
    # memory cost depends on the data sent to every executor.
    product_lookup = dim_product.select(
        "product_key",
        "product_id",
        "product_name",
        "category",
    )

    # Disable automatic broadcast selection ONLY for this demonstration.
    # This gives us a defensible baseline plan where Spark must choose a
    # non-broadcast strategy. We are comparing plans, not execution speed.
    original_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    try:
        shuffle_join = fact_sales.join(
            product_lookup,
            on="product_key",
            how="inner",
        )

        print("\nA. Broadcast disabled — baseline join plan")
        shuffle_join.explain(mode="formatted")

        # The explicit broadcast() hint tells Spark that product_lookup is small
        # enough to copy to executors. A BroadcastHashJoin can then avoid
        # repartitioning the larger fact side by the join key.
        broadcast_join = fact_sales.join(
            F.broadcast(product_lookup),
            on="product_key",
            how="inner",
        )

        print("\nB. Explicit broadcast — broadcast join plan")
        broadcast_join.explain(mode="formatted")

    finally:
        # Restore the session setting so this educational comparison does not
        # silently alter later Spark behavior in the same application.
        spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold",
            original_threshold,
        )

    print(
        "\nStudy cue: the baseline should contain shuffle Exchange operators. "
        "The broadcast plan should contain BroadcastExchange and a "
        "BroadcastHashJoin, avoiding a shuffle of the larger fact side."
    )


def inspect_key_distribution(fact_sales: DataFrame) -> None:
    """Inspect actual NullMarket key frequencies as a first skew diagnostic."""

    section("11.4 / completion gate — Data-skew inspection")

    product_distribution = (
        fact_sales
        .groupBy("product_key")
        .count()
        .orderBy(F.col("count").desc(), F.col("product_key"))
    )

    print(
        "Top product_key frequencies in the current demonstration dataset "
        "(inspection only; this does not prove a material skew problem):"
    )
    product_distribution.show(10, truncate=False)

    print(
        "Study cue: skew means some keys own disproportionately more rows than "
        "others. During a key-based shuffle, those heavy keys can create a few "
        "much larger/longer tasks that delay the entire stage."
    )


def demonstrate_lazy_evaluation(fact_sales: DataFrame) -> None:
    """Show that transformations define work while an action triggers execution."""

    section("Completion gate — Lazy evaluation, DAGs, jobs, stages, and tasks")

    # No Spark job runs merely because this transformation chain is declared.
    lazy_df = (
        fact_sales
        .filter(F.col("net_sales") > 0)
        .groupBy("store_key")
        .agg(F.sum("net_sales").alias("store_net_sales"))
        .orderBy(F.col("store_net_sales").desc())
    )

    print(
        "The DataFrame above has been defined, but Spark has not needed to "
        "materialize all result rows yet. explain() inspects the planned DAG."
    )
    lazy_df.explain(mode="formatted")

    # count() is an ACTION. It forces Spark to execute the required lineage.
    result_count = lazy_df.count()
    print(f"\nAction completed: lazy_df.count() = {result_count}")

    print(
        "Study cue: an action creates a Spark job. Shuffle boundaries divide "
        "the job's DAG into stages. Within each stage, Spark launches tasks "
        "against partitions."
    )


def parse_args() -> argparse.Namespace:
    """Parse the roadmap step to run."""

    parser = argparse.ArgumentParser(
        description="Run NullMarket Phase 11 Spark performance demonstrations."
    )
    parser.add_argument(
        "step",
        nargs="?",
        default="all",
        choices=("11.1", "11.2", "11.3", "11.4", "all"),
        help="Roadmap step to run. Default: all.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the requested Phase 11 Spark performance demonstration."""

    args = parse_args()

    # src/performance_demo.py -> parents[1] is the repository root.
    project_root = Path(__file__).resolve().parents[1]
    config = load_config(project_root)
    curated_path = project_root / config["paths"]["curated"]

    spark = create_spark_session(config["application"]["name"])
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Phase 11 reads the Phase 10 persisted layer. This makes the experiments
        # reproducible and avoids changing trusted transformation or validation
        # logic merely to teach execution concepts.
        dim_product = spark.read.parquet(str(curated_path / "dim_product"))
        dim_date = spark.read.parquet(str(curated_path / "dim_date"))
        fact_sales = spark.read.parquet(str(curated_path / "fact_sales"))
        fact_inventory_snapshot = spark.read.parquet(
            str(curated_path / "fact_inventory_snapshot")
        )

        section("Phase 11 — Starting partition counts")
        show_partition_count("dim_product", dim_product)
        show_partition_count("dim_date", dim_date)
        show_partition_count("fact_sales", fact_sales)
        show_partition_count(
            "fact_inventory_snapshot",
            fact_inventory_snapshot,
        )

        if args.step in ("11.1", "all"):
            # Step 11.1 requires plans for filters, joins, aggregations, and
            # windows using actual NullMarket datasets.
            inspect_narrow_transformations(fact_sales)
            inspect_filter_pushdown_and_partition_pruning(
                fact_inventory_snapshot
            )
            inspect_join_plan(fact_sales, dim_product)
            inspect_aggregation(fact_sales)
            inspect_project_windows(
                fact_sales,
                dim_product,
                dim_date,
            )
            demonstrate_lazy_evaluation(fact_sales)

        if args.step in ("11.2", "all"):
            inspect_partitioning(fact_sales)

        if args.step in ("11.3", "all"):
            # Re-run representative narrow and wide plans side by side so the
            # transformation boundary is easy to study in isolation.
            inspect_narrow_transformations(fact_sales)
            inspect_aggregation(fact_sales)
            inspect_project_windows(
                fact_sales,
                dim_product,
                dim_date,
            )

        if args.step in ("11.4", "all"):
            inspect_shuffle_vs_broadcast_join(
                spark,
                fact_sales,
                dim_product,
            )
            inspect_key_distribution(fact_sales)

        section(f"Phase 11 {args.step} demonstration complete")
        print(
            "No curated datasets were modified. Review the printed plans before "
            "marking the corresponding roadmap step complete."
        )

    finally:
        # Always release the Spark session even if one experiment raises.
        spark.stop()


if __name__ == "__main__":
    main()
