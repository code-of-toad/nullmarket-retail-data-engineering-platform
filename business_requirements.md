# NullMarket: Retail Data Engineering Platform

## Business Requirements

## 1. Purpose

NullMarket is a fictional Canadian retailer used to simulate a realistic retail data engineering environment.

The purpose of the platform is to consolidate operational data from multiple retail source systems into a reliable analytical data platform that supports sales, product, store, and inventory analysis.

The system must ingest raw operational data, validate its quality, transform it into business-ready datasets, and load curated analytical tables into a cloud data warehouse.

---

## 2. Business Problem

NullMarket generates operational data across multiple systems representing:

- customer orders;
- individual order line items;
- products;
- retail stores;
- inventory snapshots.

These datasets have different grains, schemas, relationships, and data-quality risks.

Without an integrated data platform, analysts and business stakeholders cannot reliably answer questions that require combining information across multiple operational systems.

The platform must therefore provide a centralized and trustworthy analytical representation of NullMarket's retail operations.

---

## 3. Business Objectives

The platform must enable NullMarket to:

1. Consolidate retail data from multiple operational sources.
2. Produce consistent and validated analytical datasets.
3. Detect and isolate invalid source records.
4. Calculate standardized sales and profitability metrics.
5. Support store, product, category, and inventory analysis.
6. Preserve appropriate historical information for analytical use.
7. Provide warehouse tables optimized for analytical SQL.
8. Support repeatable and scalable batch data processing.

---

## 4. Primary Data Consumers

The analytical datasets are intended to support the following hypothetical consumers.

### Business Analysts

Require trusted datasets for:

- sales analysis;
- store comparisons;
- product performance;
- category performance;
- inventory analysis.

### Operations Teams

Require information about:

- inventory availability;
- low-stock products;
- store-level inventory positions;
- sales trends.

### Merchandising Teams

Require information about:

- product demand;
- category performance;
- product rankings;
- gross margin.

### Data and Analytics Teams

Require:

- documented schemas;
- consistent business definitions;
- validated datasets;
- reproducible transformations;
- reliable analytical tables.

---

## 5. Source Systems

The platform will integrate five operational datasets.

### 5.1 `orders`

Represents customer orders.

**Grain:** one row per order.

Provides information about:

- the customer;
- the store;
- the order timestamp;
- order status;
- payment method;
- sales channel.

---

### 5.2 `order_items`

Represents the individual products included in each order.

**Grain:** one row per product line within an order.

Provides information about:

- product purchased;
- quantity;
- selling price;
- discount.

---

### 5.3 `products`

Represents the product catalogue.

**Grain:** one row per product.

Provides information about:

- product identity;
- category;
- subcategory;
- brand;
- product cost;
- list price;
- active status.

---

### 5.4 `stores`

Represents NullMarket retail locations.

**Grain:** one row per store.

Provides information about:

- store identity;
- location;
- province;
- region;
- store type;
- opening date.

---

### 5.5 `inventory_snapshots`

Represents inventory levels observed at specific points in time.

**Grain:** one row per product, per store, per snapshot date.

Provides information about:

- quantity on hand;
- reorder level;
- product;
- store;
- snapshot date.

---

## 6. Required Business Questions

The analytical platform must support answers to at least the following questions.

### Sales

1. What is total revenue by day?
2. What is total revenue by store?
3. What is total revenue by product?
4. What is total revenue by product category?
5. Which products generate the most revenue?
6. Which stores generate the most revenue?
7. What is the seven-day rolling sales revenue?
8. What is the average order value?

### Profitability

9. What is gross margin by product?
10. What is gross margin by product category?
11. Which products or categories contribute the greatest share of gross margin?

### Store Performance

12. How do stores rank by revenue?
13. How do stores rank within their province or region?
14. How does sales performance differ across store types?

### Inventory

15. What quantity of each product is available at each store?
16. Which products are below their reorder level?
17. Which stores currently have low inventory for specific products?
18. What are the most recent inventory levels for each product and store?

---

## 7. Core Business Metrics

The platform must provide consistent definitions for the following metrics.

### Gross Sales

```text
gross_sales = quantity × unit_price
```

### Net Sales

```text
net_sales = gross_sales - discount_amount
```

### Gross Margin

```text
gross_margin =
net_sales - (quantity × unit_cost)
```

### Average Order Value

```text
average_order_value =
total net sales / number of distinct orders
```

### Revenue Contribution

The percentage of total net sales attributable to a store, product, category, or other analytical grouping.

### Low-Stock Indicator

A product/store inventory record is considered low stock when:

```text
quantity_on_hand < reorder_level
```

---

## 8. Functional Requirements

### FR-01 — Source Ingestion

The platform must ingest data from all five defined source datasets.

### FR-02 — Schema Enforcement

Each source dataset must be processed using an explicitly defined schema.

### FR-03 — Data Validation

The platform must validate incoming data for:

- required values;
- key uniqueness;
- referential integrity;
- valid numeric ranges;
- valid dates and timestamps;
- defined business rules.

### FR-04 — Rejected Record Handling

Records that fail required validation rules must be identifiable and separable from accepted records.

Invalid data must not be silently incorporated into trusted analytical datasets.

### FR-05 — Deduplication

The platform must detect duplicate business keys and prevent unintended duplication in curated datasets.

### FR-06 — Data Transformation

The platform must transform operational data into business-ready analytical datasets.

Required transformation categories include:

- cleansing;
- joins;
- derived columns;
- aggregations;
- ranking;
- rolling calculations;
- latest-record selection.

### FR-07 — Curated Storage

Processed datasets must be stored in a columnar format suitable for analytical processing.

### FR-08 — Dimensional Modeling

The analytical layer must contain fact and dimension tables supporting retail analysis.

Required tables:

```text
dim_date
dim_product
dim_store
fact_sales
fact_inventory_snapshot
```

### FR-09 — Analytical Warehouse

Curated data must be loaded into BigQuery for analytical querying.

### FR-10 — Analytical SQL

The warehouse must support SQL queries using techniques including:

- joins;
- aggregations;
- CTEs;
- conditional logic;
- window functions;
- ranking;
- rolling calculations.

### FR-11 — Repeatable Execution

The pipeline must support repeated execution without creating unintended duplicate business records.

### FR-12 — Incremental Processing

The design must support processing new batches of operational data without requiring unnecessary full reprocessing of historical data.

---

## 9. Data-Quality Requirements

The system must validate at least the following categories.

### Primary-Key Integrity

Primary keys must:

- not be null;
- uniquely identify records at the declared grain.

### Composite-Key Integrity

Composite business keys must uniquely identify records where applicable.

Examples:

```text
(order_id, line_number)

(snapshot_date, store_id, product_id)
```

### Referential Integrity

Foreign keys must correspond to valid parent records where required.

Examples:

```text
order_items.order_id
    -> orders.order_id

order_items.product_id
    -> products.product_id

orders.store_id
    -> stores.store_id

inventory_snapshots.product_id
    -> products.product_id

inventory_snapshots.store_id
    -> stores.store_id
```

### Business-Rule Validation

Examples include:

```text
quantity > 0

unit_price >= 0

discount_amount >= 0

unit_cost >= 0

quantity_on_hand >= 0

reorder_level >= 0
```

---

## 10. Non-Functional Requirements

### NFR-01 — Correctness

Analytical results must reconcile with valid source data and defined business rules.

### NFR-02 — Reproducibility

The project must be executable from a documented environment using deterministic source-data generation where applicable.

### NFR-03 — Scalability

The architecture and transformations should use patterns appropriate for distributed data processing rather than relying solely on single-machine assumptions.

### NFR-04 — Maintainability

Schemas, configuration, validation logic, transformation logic, and pipeline execution should remain logically separated.

### NFR-05 — Testability

Core transformation and validation logic must support automated testing.

### NFR-06 — Idempotency

Repeated processing of the same input must not corrupt analytical datasets or create unintended duplicate records.

### NFR-07 — Observability

Pipeline execution must provide enough information to determine:

- whether processing succeeded;
- how many records were processed;
- how many records were rejected;
- where failures occurred.

### NFR-08 — Security

Credentials, secrets, tokens, and private keys must never be committed to source control.

Cloud access should follow the principle of least privilege.

### NFR-09 — Documentation

Major schemas, grains, relationships, calculations, architecture decisions, and assumptions must be documented.

---

## 11. Data Storage Layers

The platform will use three logical data states.

### Raw

Contains source data in its original ingested form.

Purpose:

- preserve source records;
- allow reprocessing;
- maintain separation between source and transformed data.

### Rejected

Contains records that fail defined validation rules.

Rejected records should include enough context to identify why validation failed.

### Curated

Contains validated and transformed datasets suitable for downstream warehouse loading and analytical use.

Curated datasets will use Parquet.

---

## 12. Processing Model

NullMarket will primarily implement **batch data processing**.

A pipeline execution will conceptually perform:

```text
Ingest
  ↓
Enforce Schema
  ↓
Validate
  ↓
Separate Accepted / Rejected Records
  ↓
Clean and Deduplicate
  ↓
Transform and Join
  ↓
Calculate Business Metrics
  ↓
Write Curated Data
  ↓
Load Analytical Warehouse
  ↓
Validate Warehouse Results
```

---

## 13. Cloud Requirements

The cloud implementation will use Google Cloud Platform.

Primary services:

### Google Cloud Storage

Used for:

- raw source data;
- curated Parquet data;
- rejected records where appropriate.

### Managed Apache Spark Environment

Used to execute PySpark transformations using cloud compute.

### BigQuery

Used as the analytical data warehouse.

BigQuery tables should use partitioning and clustering where justified by data volume and query patterns.

---

## 14. Assumptions

The following assumptions define the project environment.

1. NullMarket is fictional.
2. All operational source datasets are simulated.
3. Customer information is represented only to the extent necessary for analytical relationships.
4. The project does not contain real customer or personally identifiable information.
5. Currency values represent Canadian dollars unless otherwise specified.
6. Source data is primarily processed in batch.
7. Source records may contain intentionally injected quality defects.
8. The demonstration dataset is smaller than an enterprise production workload.
9. Scalability will be demonstrated through architecture and distributed-processing techniques rather than claims of production-scale transaction volume.
10. Business definitions in this document are authoritative for this project.

---

## 15. Out of Scope

The core implementation does not require:

- real-time streaming;
- Kafka;
- machine learning;
- recommendation systems;
- customer-facing applications;
- dashboards;
- payment processing;
- production ERP integration;
- production point-of-sale integration;
- Kubernetes;
- enterprise orchestration;
- full infrastructure-as-code automation.

These may be discussed as future extensions but are not required to satisfy the platform's core business requirements.

---

## 16. Success Criteria

NullMarket satisfies its business requirements when:

- all five source datasets can be ingested;
- schemas are explicitly enforced;
- invalid records can be detected and isolated;
- source relationships are validated;
- sales and inventory datasets can be transformed successfully;
- standardized business metrics are calculated correctly;
- curated Parquet datasets are produced;
- analytical fact and dimension tables are created;
- BigQuery can answer the required business questions;
- repeated processing does not introduce unintended duplicates;
- critical transformations and validations are covered by automated tests;
- the complete architecture and data definitions are documented;
- analytical results can be reconciled with valid source data.