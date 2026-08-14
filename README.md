# NullMarket: Retail Data Engineering Platform

NullMarket is a portfolio-grade **retail data engineering platform** designed to demonstrate end-to-end batch data engineering using **PySpark, Apache Spark, SQL, Google Cloud Platform, BigQuery, and Parquet**.

The project simulates a Canadian retail environment in which operational sales, product, store, and inventory data are ingested, validated, transformed, modeled, and loaded into an analytical warehouse.

## Architecture

```text
Retail Source Systems
        |
        v
Google Cloud Storage
     Raw Layer
        |
        v
Apache Spark / PySpark
        |
        +--> Schema Enforcement
        +--> Data Quality Validation
        +--> Deduplication
        +--> Cleansing
        +--> Joins & Transformations
        |
        v
Google Cloud Storage
   Curated Parquet
        |
        v
      BigQuery
        |
        +--> Dimension Tables
        +--> Fact Tables
        +--> Partitioning
        +--> Clustering
        |
        v
   Analytical SQL
```

## Technology Stack

- **Python**
- **PySpark**
- **Apache Spark**
- **SQL**
- **Google Cloud Platform**
- **Google Cloud Storage**
- **BigQuery**
- **Parquet**
- **pytest**
- **Git / GitHub**

## Source Systems

NullMarket models five operational datasets:

| Dataset | Grain |
|---|---|
| `orders` | One row per order |
| `order_items` | One row per product line within an order |
| `products` | One row per product |
| `stores` | One row per store |
| `inventory_snapshots` | One row per product, per store, per snapshot date |

## Target Data Model

The analytical warehouse is designed around a dimensional model containing:

```text
dim_date
dim_product
dim_store
fact_sales
fact_inventory_snapshot
```

This supports analytical use cases such as:

- daily and rolling sales revenue;
- store and product performance;
- category-level sales;
- gross-margin analysis;
- average order value;
- product rankings;
- inventory monitoring;
- low-stock detection.

## Engineering Objectives

The project is designed to demonstrate:

- explicit schema enforcement;
- reproducible synthetic data generation;
- reusable data-quality validation;
- primary-key and referential-integrity checks;
- rejected-record handling;
- PySpark DataFrame transformations;
- multi-table joins;
- aggregations and window functions;
- columnar Parquet storage;
- dimensional data modeling;
- cloud-based Spark execution;
- BigQuery partitioning and clustering;
- analytical SQL;
- automated testing;
- idempotent and incremental-processing concepts.

## Repository Structure

```text
nullmarket-retail-data-engineering-platform/
|
├── README.md
├── ROADMAP.md
├── requirements.txt
├── .gitignore
|
├── config/
├── data/
├── docs/
├── infrastructure/
├── sql/
├── src/
└── tests/
```

Directories are introduced as their corresponding components are implemented.

## Documentation

Project documentation includes:

- `ROADMAP.md` — canonical implementation sequence
- `docs/business_requirements.md` — business problem and analytical requirements
- `docs/data_dictionary.md` — source-system grains, keys, schemas, and validation rules
- `docs/data_model.md` — dimensional warehouse design
- `docs/architecture.md` — system architecture and data flow
- `docs/design_decisions.md` — engineering decisions, tradeoffs, and troubleshooting notes

## Project Principles

NullMarket prioritizes:

1. **Correctness** — transformations and business metrics must be verifiable.
2. **Data quality** — invalid records must be detected rather than silently ignored.
3. **Reproducibility** — the project must be executable from a documented clean environment.
4. **Scalability** — design choices should reflect distributed data-engineering principles.
5. **Maintainability** — schemas, validation, transformations, and configuration remain logically separated.
6. **Defensibility** — architectural and implementation decisions must have clear technical reasoning.

## Project Status

Implementation follows the step-by-step specification defined in [`ROADMAP.md`](ROADMAP.md).

---

**NullMarket is a fictional retailer created solely for this data engineering project.**