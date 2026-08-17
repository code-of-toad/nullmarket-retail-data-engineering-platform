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
