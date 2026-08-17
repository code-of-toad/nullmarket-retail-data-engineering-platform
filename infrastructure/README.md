# NullMarket Cloud Infrastructure

## Phase 14 — Google Cloud Storage

Phase 14 establishes the cloud storage layer only. Cloud Spark execution,
BigQuery, orchestration, CI/CD, and infrastructure-as-code remain out of scope
until their roadmap phases.

## Google Cloud Project

- Project name: `NullMarket Retail DE`
- Project ID: `nullmarket-retail-de`
- Region used for storage: `northamerica-northeast2` (Toronto)

A project provides the administrative boundary for resources, billing, IAM,
service enablement, and quotas.

## Billing Safeguard

A monthly project budget is configured for NullMarket with email alerts.

The budget is an alerting control rather than a hard spending cap.

## Authentication and IAM

Phase 14 uses the signed-in human Google account for interactive Console work.

No service-account JSON key is created or committed. Future managed workloads
should use workload/service-account authentication instead of long-lived key
files.

Important IAM concepts:

- Principal: identity that receives access.
- Permission: one allowed action.
- Role: collection of permissions.
- Service account: non-human identity used by workloads.
- Least privilege: grant only the access required for the task.

## Cloud Storage

Bucket:

`gs://nullmarket-retail-de-data`

Configuration:

- Regional storage in `northamerica-northeast2`
- Standard storage class
- Uniform bucket-level access
- Public access prevention enforced
- Hierarchical namespace disabled
- Soft delete disabled
- Object versioning disabled
- No retention policy
- Google-managed encryption

## Logical Storage Layout

```text
raw/
├── orders/
├── order_items/
├── products/
├── stores/
└── inventory_snapshots/

curated/
├── dim_product/
├── dim_store/
├── dim_date/
├── fact_sales/
└── fact_inventory_snapshot/

rejected/
```

With the bucket's flat namespace, these folder-like paths are object-name
prefixes rather than traditional filesystem directories.

## Raw Objects Uploaded

```text
raw/orders/orders.csv
raw/order_items/order_items.csv
raw/products/products.csv
raw/stores/stores.csv
raw/inventory_snapshots/inventory_snapshots.csv
```

The raw layer preserves source-format data so validation and transformation can
be rerun independently of downstream curated outputs.

## Phase Boundary

Phase 14 does not modify PySpark transformation logic. The GCS URIs are
externalized in `config/config.yaml` so Phase 15 can adapt orchestration to
cloud paths without duplicating transformation functions.
