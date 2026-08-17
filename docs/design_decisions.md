# NullMarket Design Decisions and Troubleshooting Notes

This document records implementation decisions and troubleshooting evidence that materially affected the NullMarket data engineering platform. It is intentionally limited to decisions that have actually been implemented or observed.

---

## Phase 15 — Managed Spark Cloud Execution

### Decision: Reuse the same transformation logic for local and cloud execution

NullMarket does not maintain a separate set of PySpark transformations for GCP. The Phase 15 orchestration layer accepts environment-specific storage paths while continuing to reuse the established schema, validation, and transformation modules.

**Rationale**

- Business logic should not change merely because storage moves from local disk to Google Cloud Storage.
- Keeping transformation functions storage-agnostic reduces duplication and the risk of local/cloud logic drifting apart.
- Local execution explicitly uses `local[*]`, while managed cloud execution leaves Spark master configuration to Managed Service for Apache Spark.
- Cloud-specific differences are limited to orchestration concerns such as `gs://` paths, dependency packaging, workload identity, and batch submission.

**Implemented outcome**

The Phase 15 pipeline successfully read the five raw operational datasets from GCS, executed in a Managed Service for Apache Spark batch, and wrote the curated datasets back to GCS.

---

## Phase 15 Troubleshooting Example — Console Could Not Select a Valid Regional Subnet

### Problem

The Google Cloud Console's **Create batch** workflow for Managed Service for Apache Spark would not expose an eligible local network/subnetwork for the Phase 15 batch, even after a dedicated VPC and Toronto subnet had been created.

### Symptoms

- The batch was configured for region `northamerica-northeast2`.
- A custom VPC named `nullmarket-spark-vpc` existed.
- A subnet named `nullmarket-spark-toronto` existed in `northamerica-northeast2` with primary IPv4 range `10.10.0.0/24`.
- The Managed Spark Create batch page continued to display **"No local networks are available."**
- Reloading and recreating the batch form did not make the network selectable.
- Temporarily switching the execution identity from the workload service account to the user account did not change the Console behavior.

### Root cause

The failure was isolated to the **Console network-selection path**, not to the underlying VPC/subnet or the Spark workload configuration.

The exact internal cause of the Console resource-discovery failure was not observable from the project, so it would be inaccurate to claim a specific Google Cloud UI defect or IAM permission as proven root cause. What was established empirically is that the same regional subnet that the Console would not expose was accepted by the Managed Spark service when supplied explicitly through the Google Cloud CLI.

### Diagnostic process

1. Verified that `nullmarket-spark-vpc` existed in the NullMarket GCP project.
2. Verified that `nullmarket-spark-toronto` belonged to that VPC.
3. Verified that the subnet region matched the batch region: `northamerica-northeast2`.
4. Verified the subnet's primary IPv4 range as `10.10.0.0/24`.
5. Reopened the Managed Spark Create batch page to eliminate a stale form as the likely explanation.
6. Tested the Console with both the workload service account and user-account execution options; the network selector still showed no local networks.
7. Bypassed the Console selector and submitted the batch with `gcloud dataproc batches submit pyspark`, supplying the full subnet resource explicitly with `--subnet`.
8. Queried the submitted batch and confirmed terminal state `SUCCEEDED`.
9. Inspected Cloud Logging, the Managed Spark batch page, the Spark UI, and GCS outputs to confirm the workload executed successfully.

### Resolution

Submitted the workload through Cloud Shell with an explicit subnet instead of relying on the Console network dropdown.

The successful batch used:

- project: `nullmarket-retail-de`
- region: `northamerica-northeast2`
- batch: `nullmarket-phase15-01`
- runtime: `3.0`
- workload service account: `nullmarket-spark-workload@nullmarket-retail-de.iam.gserviceaccount.com`
- subnet: `projects/nullmarket-retail-de/regions/northamerica-northeast2/subnetworks/nullmarket-spark-toronto`
- raw input: `gs://nullmarket-retail-de-data/raw`
- curated output: `gs://nullmarket-retail-de-data/curated`
- rejected output: `gs://nullmarket-retail-de-data/rejected`

The batch completed with state `SUCCEEDED` in approximately 3 minutes 38 seconds.

### Lesson learned

A cloud-console UI is only one client of the underlying service. When a Console selector behaves inconsistently, first verify the actual infrastructure independently, then use the CLI or API to supply explicit resource identifiers rather than repeatedly changing valid infrastructure.

Troubleshooting should distinguish between:

- **resource validity** — whether the VPC, subnet, region, IAM identity, and storage paths are valid;
- **submission-client behavior** — whether a particular Console form can discover/select those resources; and
- **workload behavior** — whether Spark can actually start, read inputs, process data, and write outputs.

The successful CLI submission proved that the VPC/subnet and workload configuration were usable even though the Console network selector was not.

---

## Phase 15 Observability Evidence

The successful cloud run produced the following observable evidence:

- Managed Spark batch state: `SUCCEEDED`.
- Batch type: PySpark.
- Runtime: 3.0.
- Region: `northamerica-northeast2`.
- Elapsed batch time: approximately 3 minutes 38 seconds.
- Driver logs were available in Cloud Logging and ended with `Phase 15 pipeline completed successfully.`
- Spark UI reported 574 completed jobs, no failed jobs, 574 completed stages, and no failed stages.
- The executor summary reported 603 completed tasks and 0 failed tasks for the captured application view.
- Curated GCS outputs existed for `dim_date`, `dim_product`, `dim_store`, `fact_sales`, and `fact_inventory_snapshot`.
- `fact_inventory_snapshot` preserved the intended physical `date_key=...` partition layout with seven date partitions.

### Important interpretation of Spark UI evidence

The Spark UI confirms jobs, stages, and tasks executed in the managed cloud batch. The captured executor view only showed the driver entry, so NullMarket should **not** claim that this small demonstration run proved a particular number of concurrently active worker executors. The defensible claim is that the PySpark application executed remotely on Google-managed Spark infrastructure and produced inspectable Spark execution metadata.

### Window-operation warnings

Cloud logs contained repeated warnings that some window operations had no partition definition and therefore moved data to a single partition. These warnings are consistent with NullMarket's intentionally global ranking and chronological window calculations. They do not indicate a failed workload, but they identify a scalability consideration: global ordering can become a bottleneck as data volume grows.

## BigQuery Partitioning and Clustering

### Decision

The final BigQuery fact tables use business-date partitioning:

- `fact_sales` is partitioned by `order_date`.
- `fact_inventory_snapshot` is partitioned by `snapshot_date`.

The business dates are derived deterministically from the existing `date_key`
relationship to `dim_date.full_date`.

The tables use the following clustering:

- `fact_sales`: `product_key`, then `store_key`
- `fact_inventory_snapshot`: `store_key`, then `product_key`

### Rationale

Sales requirements repeatedly analyze revenue and margin by product/category
and store, so `product_key` and `store_key` are defensible clustering fields.

Inventory requirements focus on inventory availability and low-stock analysis
for store/product combinations, so `store_key` and `product_key` are used.

`category` remains an attribute of `dim_product` rather than being duplicated
into a fact table solely for clustering.

Partitioning uses business dates rather than ingestion time because analytical
queries are expected to filter sales by order date and inventory by snapshot
date.

### Scale Limitation

The NullMarket demonstration dataset is small, so this project does not claim
measured query-performance or cost improvements from partitioning or
clustering. The implementation demonstrates a defensible BigQuery physical
design pattern that becomes more relevant as warehouse volume increases.

---

## Phase 17 — Idempotency and Incremental Processing

### Decision: Keep the validated full-refresh path and add a batch-scoped incremental path

NullMarket retains its existing full-refresh processing while adding a separate deterministic incremental batch for new transactional and inventory data.

**Rationale**

- Idempotency and incremental processing solve different problems: a full refresh proves the complete dataset can be rebuilt safely, while an incremental load avoids unnecessary reprocessing of unchanged history.
- Keeping a known-good full-refresh path provides a reconciliation baseline for incremental testing.
- Batch-specific raw and curated prefixes make the new input/output boundary explicit and keep incremental evidence separate from historical curated data.

**Implemented outcome**

- Repeating the full-refresh process with identical input did not create unintended duplicate business records.
- Phase 17 generated a deterministic subsequent batch containing new orders, order items, and inventory snapshots.
- The new batch was processed locally and with Managed Spark using batch-scoped GCS paths.
- Historical Phase 16 curated Parquet remained available as an untouched reconciliation baseline.

### Decision: Reuse existing product/store reference data and preserve surrogate-key mappings

The incremental batch contains new transactional/snapshot records but does not regenerate product or store reference entities.

**Rationale**

- `fact_sales` and `fact_inventory_snapshot` depend on stable dimension-key mappings.
- Rebuilding surrogate keys independently for each incremental batch could cause the same source business key to point to a different warehouse key.
- Existing product and store reference data already represents the accepted current dimension population needed by the new facts.

**Implemented outcome**

Phase 17 explicitly validated that existing product and store business keys retained the same surrogate-key mappings while new fact rows were produced.

### Decision: Use deterministic business keys with insert-only BigQuery MERGE for incremental ingestion

Incremental warehouse loads use deterministic business uniqueness rather than unconditional append behavior.

Relevant business keys are:

```text
fact_sales:
(order_id, line_number)

fact_inventory_snapshot:
(snapshot_date, store_key, product_key)
```

**Rationale**

- A plain append would duplicate rows if the same batch were retried.
- The current incremental demonstration contains new records and does not implement change capture for updates to existing historical facts.
- Insert-only `MERGE` therefore matches the implemented requirement: insert a business record only when its key is not already present.

**Implemented outcome**

The same incremental BigQuery load was retried and did not produce duplicate warehouse rows.

### Decision: Treat historical and incremental reconciliation as separate correctness checks

Phase 17 validates both the preserved history and the newly added records.

**Rationale**

A final warehouse row count alone cannot prove an incremental load is correct. A faulty implementation could preserve the expected total while mutating old rows, duplicating some keys, or changing measures.

**Implemented outcome**

Validation separately established that:

- historical warehouse data reconciled exactly with the untouched Phase 16 curated Parquet;
- new warehouse records reconciled exactly with the Phase 17 incremental Parquet;
- duplicate business keys were zero;
- orphan relationships were zero;
- business-date mismatches were zero;
- sales-measure mismatches were zero;
- low-stock mismatches were zero;
- existing BigQuery partitioning and clustering were preserved.

---

## Phase 18 — Consolidated Implemented Architecture Decisions

Phase 18 does not introduce a new runtime component. It documents the engineering decisions already implemented in Phases 1–17.

### Why Apache Spark / PySpark?

**Implemented decision**

NullMarket uses PySpark DataFrames as the main transformation engine locally and in Google-managed Spark.

**Rationale**

- The roadmap requires practical distributed data-processing patterns rather than a pipeline tied only to in-memory single-process transformations.
- Spark provides the DataFrame operations, joins, aggregations, window functions, partition concepts, broadcast joins, and execution-plan inspection required by the project.
- The same transformation functions can operate on local files or GCS-backed DataFrames without duplicating business logic.

**Boundary**

The demonstration dataset is small. NullMarket demonstrates Spark engineering patterns and managed execution; it does not claim that Spark was required by the measured data volume or that enterprise-scale throughput was benchmarked.

### Why explicit schemas?

**Implemented decision**

Every operational source is read with an explicit PySpark `StructType` schema.

**Rationale**

- Source types are part of the data contract and should not depend on inference from a particular file sample.
- Explicit types preserve fixed-precision decimal handling for currency and consistent date/timestamp parsing.
- Malformed typed values can be exposed to the validation layer rather than silently changing the inferred schema.
- Stable schemas make transformations and automated tests more reproducible.

### Why separate raw, rejected, and curated states?

**Implemented decision**

NullMarket keeps raw source data, rejected records, and curated analytical output logically separate in storage.

**Rationale**

- Raw data preserves the ingested source representation for reprocessing.
- Rejected storage makes data-quality failures inspectable instead of silently discarding them.
- Curated storage contains only validated and transformed warehouse-shaped datasets intended for downstream analytical loading.
- Separating these states makes trust boundaries and troubleshooting clearer.

### Why Parquet for the curated layer?

**Implemented decision**

The five curated analytical datasets are persisted as Parquet.

**Rationale**

- Parquet is columnar and suited to analytical reads that select subsets of columns.
- It preserves typed schemas better than CSV.
- It supports compression and Spark optimizations such as predicate pushdown.
- Persisted Parquet can be read back and independently validated before warehouse loading.

### Why dimensional modeling?

**Implemented decision**

The warehouse uses conformed `dim_date`, `dim_product`, and `dim_store` dimensions shared by separate sales and inventory fact tables.

**Rationale**

- Product, store, and date attributes provide reusable analytical context for grouping, filtering, and ranking measures.
- Shared dimensions provide consistent business definitions across facts.
- Separate facts prevent sales transactions and inventory snapshot state from being mixed at incompatible grains.

### Why `fact_sales` uses order-line grain

**Implemented decision**

`fact_sales` contains one row per validated `(order_id, line_number)`.

**Rationale**

Quantity, unit price, discount, product identity, and gross margin are defined at the order-line level. Aggregating to one row per order would lose product-level analytical detail and make the documented line-level measures unavailable.

### Why `fact_inventory_snapshot` uses date/store/product grain

**Implemented decision**

`fact_inventory_snapshot` contains one row per product, per store, per snapshot date.

**Rationale**

Inventory is point-in-time state rather than a transaction. The snapshot grain preserves historical observations while preventing repeated inventory states from being treated as additive transactions across dates.

### Why the curated Parquet partition strategy is selective

**Implemented decision**

`fact_inventory_snapshot` is physically partitioned by `date_key` in the curated Parquet layer, while `fact_sales` is not physically date-partitioned for the current demonstration dataset.

**Rationale**

- The inventory dataset has a small number of snapshot dates, so the layout provides a defensible low-cardinality date-partitioning demonstration.
- The small sales fact is spread across many dates; partitioning it by date would create many tiny directories/files and illustrate the small-file problem rather than good physical design.

**Boundary**

This is a layout decision, not a measured performance-improvement claim.

### Why BigQuery partitions by business date

**Implemented decision**

- `fact_sales` is partitioned by `order_date`.
- `fact_inventory_snapshot` is partitioned by `snapshot_date`.

**Rationale**

The documented analytical questions are time-oriented, so business dates are more useful analytical partition boundaries than ingestion time for this warehouse.

### Why BigQuery clustering uses product/store keys

**Implemented decision**

- `fact_sales`: `product_key`, then `store_key`.
- `fact_inventory_snapshot`: `store_key`, then `product_key`.

**Rationale**

Sales requirements repeatedly group/filter by product/category and store, while inventory requirements focus on store/product availability and low-stock combinations. The cluster columns support those access patterns without denormalizing dimension attributes such as category into facts solely for physical optimization.

**Boundary**

The demonstration dataset is too small to support defensible claims of measured BigQuery cost or performance savings.

### How failures are handled

**Implemented decision**

NullMarket uses explicit failure paths at multiple layers rather than treating a completed process as sufficient proof of correctness.

**Implemented mechanisms**

- invalid source records are placed in rejected output with validation reasons;
- grain and row-count guards detect accidental multiplication or row loss;
- sales measures are independently reconciled from accepted source data;
- Parquet outputs are read back and compared with in-memory curated results;
- Managed Spark job state, logs, and Spark UI execution metadata are inspectable;
- BigQuery validation SQL checks uniqueness, relationships, dates, and measures;
- incremental BigQuery `MERGE` prevents duplicate inserts when a batch is retried.

An external orchestrator, centralized alerting, and automated production retry policy are not implemented.

### How the current design could scale without claiming production scale

The implemented architecture already uses technologies and patterns that support larger workloads conceptually: distributed Spark execution, Parquet, partition-aware data layouts, broadcast-join awareness, execution-plan inspection, incremental processing, and BigQuery partitioning/clustering.

At substantially higher volume, the same design would require additional engineering decisions such as partition/file sizing, shuffle reduction, skew mitigation, more deliberate broadcast thresholds, monitoring, automated retries, orchestration, and operational controls.

These are scalability and productionization considerations only. NullMarket has not benchmarked enterprise-scale throughput, cost savings, or production reliability, and Phase 18 does not add those capabilities.
