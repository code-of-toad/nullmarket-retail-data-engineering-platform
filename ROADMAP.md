# NullMarket: Retail Data Engineering Platform

## Canonical Project Roadmap

NullMarket is a portfolio-grade retail data engineering platform designed to demonstrate practical competence in:

- Python
- SQL
- PySpark
- Apache Spark
- data modeling
- data quality
- dimensional warehousing
- Parquet
- Google Cloud Platform
- Cloud Storage
- BigQuery
- testing
- pipeline design
- scalable data-processing concepts

This document defines the **complete implementation sequence** for the project.

It is intentionally independent of dates, deadlines, and current progress.

Follow the phases and steps **in order**. Do not advance past a completion gate until its requirements are satisfied.

---

# 0. Target Architecture

The completed system will implement this flow:

```text
Synthetic Retail Source Systems
            |
            v
     Local Source Files
            |
            v
 Google Cloud Storage
        Raw Layer
            |
            v
   Apache Spark / PySpark
            |
            +--> Schema Enforcement
            +--> Data Validation
            +--> Deduplication
            +--> Cleansing
            +--> Joins
            +--> Transformations
            +--> Aggregations
            |
            v
 Google Cloud Storage
     Curated Parquet
            |
            v
         BigQuery
            |
            +--> Dimensions
            +--> Fact Tables
            +--> Partitioning
            +--> Clustering
            |
            v
      Analytical SQL
```

---

# 1. Final Project Requirements

The completed project must demonstrate all of the following.

## Data engineering

- Multi-source ingestion
- Explicit schemas
- Batch processing
- Data-quality validation
- Referential-integrity validation
- Deduplication
- Data cleansing
- Multi-table transformations
- Incremental-processing concepts
- Idempotent processing
- Rejected-record handling
- Columnar storage
- Analytical data modeling

## PySpark

- DataFrames
- Explicit `StructType` schemas
- `select`
- `filter`
- `withColumn`
- `when`
- joins
- aggregations
- `groupBy`
- window functions
- deduplication
- date/time functions
- repartitioning concepts
- broadcast joins
- execution-plan inspection

## SQL

- joins
- aggregations
- CTEs
- subqueries
- `CASE`
- window functions
- ranking
- rolling calculations
- deduplication
- analytical queries
- warehouse validation queries

## Cloud

- Google Cloud Storage
- cloud authentication
- IAM concepts
- cloud-based Spark execution
- BigQuery
- partitioned warehouse tables
- clustered warehouse tables

## Engineering quality

- modular project structure
- configuration management
- unit tests
- data-quality tests
- documentation
- reproducible setup
- meaningful Git history
- no committed credentials
- no unnecessary generated data

---

# 2. Scope

## Required

The following components are mandatory:

1. Five simulated operational source systems
2. Synthetic source-data generator
3. Explicit source schemas
4. Local PySpark pipeline
5. Data-quality framework
6. Rejected-record handling
7. Curated Parquet layer
8. Dimensional warehouse model
9. Analytical SQL
10. Google Cloud Storage integration
11. Cloud Spark execution
12. BigQuery warehouse
13. BigQuery partitioning and clustering
14. Automated tests
15. Architecture documentation
16. Data dictionary
17. Design-decision documentation
18. Professional README

## Optional extensions

Implement only after all required components are complete:

- GitHub Actions
- orchestration
- Terraform
- additional observability
- pipeline audit tables
- CDC simulation
- performance benchmarking
- dashboarding
- streaming
- Kafka

---

# 3. Source Systems

NullMarket simulates five operational retail datasets.

---

## 3.1 `orders`

**Grain:** one row per order.

Primary key:

```text
order_id
```

Core fields:

```text
order_id
customer_id
store_id
order_timestamp
order_status
payment_method
channel
```

---

## 3.2 `order_items`

**Grain:** one row per product line within an order.

Composite primary key:

```text
(order_id, line_number)
```

Core fields:

```text
order_id
line_number
product_id
quantity
unit_price
discount_amount
```

---

## 3.3 `products`

**Grain:** one row per product.

Primary key:

```text
product_id
```

Core fields:

```text
product_id
product_name
category
subcategory
brand
unit_cost
list_price
active_flag
```

---

## 3.4 `stores`

**Grain:** one row per store.

Primary key:

```text
store_id
```

Core fields:

```text
store_id
store_name
city
province
region
store_type
open_date
```

---

## 3.5 `inventory_snapshots`

**Grain:** one row per product, per store, per snapshot date.

Composite primary key:

```text
(snapshot_date, store_id, product_id)
```

Core fields:

```text
snapshot_date
store_id
product_id
quantity_on_hand
reorder_level
```

---

# 4. Target Warehouse Model

The analytical warehouse will contain:

```text
dim_date
dim_product
dim_store
fact_sales
fact_inventory_snapshot
```

---

## `fact_sales`

**Grain:** one row per order line.

Measures:

```text
quantity
unit_price
discount_amount
gross_sales
net_sales
unit_cost
gross_margin
```

Derived calculations:

```text
gross_sales =
quantity * unit_price

net_sales =
gross_sales - discount_amount

gross_margin =
net_sales - (quantity * unit_cost)
```

---

## `fact_inventory_snapshot`

**Grain:** one row per product, per store, per snapshot date.

Measures:

```text
quantity_on_hand
reorder_level
```

---

# 5. Repository Structure

The final repository should approximately follow:

```text
nullmarket-retail-data-engineering-platform/
|
├── README.md
├── ROADMAP.md
├── requirements.txt
├── .gitignore
|
├── config/
│   └── config.yaml
|
├── data/
│   └── sample/
|
├── docs/
│   ├── business_requirements.md
│   ├── data_dictionary.md
│   ├── architecture.md
│   ├── data_model.md
│   └── design_decisions.md
|
├── src/
│   ├── generate_data.py
│   ├── schemas.py
│   ├── data_quality.py
│   ├── transformations.py
│   └── pipeline.py
|
├── sql/
│   ├── ddl/
│   ├── validation/
│   └── analytics/
|
├── tests/
│   ├── conftest.py
│   ├── test_data_quality.py
│   └── test_transformations.py
|
└── infrastructure/
    └── README.md
```

Do not create empty directories merely for appearance. Introduce them when the roadmap reaches the corresponding implementation step.

---

# PHASE 1 — Repository Foundation

## Step 1.1 — Initialize Git

Create the project:

```bash
mkdir nullmarket-retail-data-engineering-platform
cd nullmarket-retail-data-engineering-platform
git init -b main
```

Create:

```text
README.md
ROADMAP.md
requirements.txt
.gitignore
docs/
```

---

## Step 1.2 — Configure `.gitignore`

Exclude:

- Python caches
- virtual environments
- IDE configuration
- local credentials
- environment files
- Spark temporary files
- generated raw data
- generated processed data
- test caches

Never commit GCP credentials or secrets.

---

## Step 1.3 — Create Minimal README

The initial README should identify:

- project name
- project purpose
- core technology stack

Do not write the final README yet.

---

## Step 1.4 — Create Remote GitHub Repository

Repository name:

```text
nullmarket-retail-data-engineering-platform
```

Repository visibility:

```text
Public
```

Suggested description:

```text
Retail data engineering platform built with PySpark, SQL, GCP, and BigQuery.
```

Connect the local repository and push `main`.

---

## Completion Gate — Phase 1

Verify:

- [x] Local Git repository exists.
- [x] GitHub repository exists.
- [x] `main` is the default branch.
- [x] `.gitignore` is configured.
- [x] No secrets are tracked.
- [x] Initial commit has been pushed.

---

# PHASE 2 — Business and Data Requirements

## Step 2.1 — Define Business Scenario

Create:

```text
docs/business_requirements.md
```

Define NullMarket as a fictional Canadian retailer requiring an analytical data platform that consolidates sales, product, store, and inventory information.

---

## Step 2.2 — Define Business Questions

The platform must support at least:

1. Daily sales revenue
2. Revenue by store
3. Revenue by product
4. Revenue by category
5. Top-selling products
6. Store performance rankings
7. Seven-day rolling revenue
8. Average order value
9. Gross margin
10. Inventory levels
11. Low-stock products
12. Product/store inventory availability

---

## Step 2.3 — Define Non-Functional Requirements

Document requirements for:

- correctness
- reproducibility
- scalability
- maintainability
- testability
- idempotency
- observability
- security

---

## Completion Gate — Phase 2

Be able to explain:

- [ ] What business problem the pipeline solves.
- [ ] Who consumes the resulting data.
- [ ] Why the five source datasets are required.
- [ ] Which analytical questions the warehouse supports.
- [ ] Which engineering qualities the system prioritizes.

---

# PHASE 3 — Source-System Design

## Step 3.1 — Create Data Dictionary

Create:

```text
docs/data_dictionary.md
```

For every source column document:

```text
Column
Data type
Nullable?
Key type
Business definition
Validation rule
```

---

## Step 3.2 — Define Grain

Explicitly document the grain of every source table.

Do this **before implementing transformations**.

---

## Step 3.3 — Define Relationships

Document relationships such as:

```text
orders.store_id
    -> stores.store_id

order_items.order_id
    -> orders.order_id

order_items.product_id
    -> products.product_id

inventory_snapshots.store_id
    -> stores.store_id

inventory_snapshots.product_id
    -> products.product_id
```

---

## Completion Gate — Phase 3

Be able to explain:

- [ ] What grain means.
- [ ] Why grain must be defined before joining tables.
- [ ] Why `orders` and `order_items` have different grains.
- [ ] How incorrect joins can duplicate measures.
- [ ] Which keys uniquely identify every dataset.

---

# PHASE 4 — Warehouse Design

## Step 4.1 — Design Dimensions

Define:

```text
dim_date
dim_product
dim_store
```

Determine:

- key strategy
- attributes
- source mappings

---

## Step 4.2 — Design Facts

Define:

```text
fact_sales
fact_inventory_snapshot
```

Document:

- grain
- foreign keys
- measures
- derived measures

---

## Step 4.3 — Document Model

Create:

```text
docs/data_model.md
```

Include:

- fact/dimension definitions
- grains
- keys
- relationships
- measures
- design rationale

---

## Completion Gate — Phase 4

Be able to explain:

- [ ] Fact vs dimension.
- [ ] Measure vs attribute.
- [ ] Natural vs surrogate key.
- [ ] Why `fact_sales` uses order-line grain.
- [ ] Why inventory requires snapshot grain.
- [ ] Why facts must not mix incompatible grains.

---

# PHASE 5 — Local Development Environment

## Step 5.1 — Create Python Virtual Environment

Create and activate an isolated environment.

---

## Step 5.2 — Install Required Dependencies

Initial dependencies should include only packages actually required by the implementation, including:

```text
pyspark
pytest
pyyaml
```

Use versions compatible with the selected Spark runtime.

Record exact installed versions in:

```text
requirements.txt
```

---

## Step 5.3 — Verify Spark

Create a local Spark session and confirm:

- Spark starts successfully.
- Python can create a DataFrame.
- A simple Spark action completes.

---

## Step 5.4 — Configure Project Paths

Create:

```text
config/config.yaml
```

Centralize configurable values such as:

```text
raw paths
curated paths
GCS locations
BigQuery dataset
application name
```

Avoid scattering environment-specific strings through source code.

---

## Completion Gate — Phase 5

Verify:

- [ ] Virtual environment works.
- [ ] Spark starts.
- [ ] Test DataFrame executes.
- [ ] Dependencies are reproducible.
- [ ] Configuration is separated from processing logic.

---

# PHASE 6 — Synthetic Source Data

## Step 6.1 — Create Generator

Create:

```text
src/generate_data.py
```

Use a fixed random seed so generated data is reproducible.

---

## Step 6.2 — Generate Valid Business Data

Generate realistic values for:

- Canadian stores
- product categories
- products
- orders
- order lines
- inventory snapshots

---

## Step 6.3 — Introduce Controlled Data Errors

Inject known examples of:

- duplicate keys
- null required values
- invalid foreign keys
- negative quantities
- invalid prices
- invalid inventory values
- malformed or invalid timestamps

The errors must be deterministic so tests can verify them.

---

## Step 6.4 — Keep Only Small Samples in Git

Generated bulk datasets must remain outside Git.

Commit only small representative samples under:

```text
data/sample/
```

---

## Completion Gate — Phase 6

Verify:

- [ ] All five datasets can be generated.
- [ ] Generation is deterministic.
- [ ] Valid relationships exist.
- [ ] Known bad records exist.
- [ ] Large generated files are ignored by Git.

---

# PHASE 7 — PySpark Schema Layer

## Step 7.1 — Create Schemas

Create:

```text
src/schemas.py
```

Define explicit `StructType` schemas for all five sources.

Use appropriate Spark types such as:

```text
StringType
IntegerType
LongType
DecimalType
BooleanType
DateType
TimestampType
```

---

## Step 7.2 — Read Source Data

Read each dataset using its explicit schema.

Do not depend on schema inference for production pipeline logic.

---

## Step 7.3 — Validate Schema Behaviour

Test deliberately malformed values and observe how Spark handles them.

---

## Completion Gate — Phase 7

Be able to explain:

- [ ] Why explicit schemas are preferable.
- [ ] What `StructType` represents.
- [ ] What `StructField` represents.
- [ ] Why money should not normally use binary floating-point types.
- [ ] How schema enforcement protects downstream processing.

---

# PHASE 8 — Data-Quality Framework

## Step 8.1 — Create Quality Module

Create:

```text
src/data_quality.py
```

Build reusable validation functions rather than embedding every check directly inside the pipeline.

---

## Step 8.2 — Validate Primary Keys

Check uniqueness and required values for:

```text
orders.order_id
products.product_id
stores.store_id
```

---

## Step 8.3 — Validate Composite Keys

Check:

```text
order_items(order_id, line_number)

inventory_snapshots(
    snapshot_date,
    store_id,
    product_id
)
```

---

## Step 8.4 — Validate Referential Integrity

Detect orphan records.

Examples:

```text
order_items.order_id -> orders.order_id
order_items.product_id -> products.product_id
orders.store_id -> stores.store_id
```

---

## Step 8.5 — Validate Business Rules

Examples:

```text
quantity > 0
unit_price >= 0
discount_amount >= 0
unit_cost >= 0
quantity_on_hand >= 0
reorder_level >= 0
```

---

## Step 8.6 — Separate Valid and Invalid Records

Do not silently discard failures.

Produce:

```text
accepted records
rejected records
validation reason
```

---

## Completion Gate — Phase 8

Verify:

- [ ] Duplicate keys are detected.
- [ ] Null violations are detected.
- [ ] Invalid foreign keys are detected.
- [ ] Invalid business values are detected.
- [ ] Bad records can be quarantined.
- [ ] Validation results are inspectable.

---

# PHASE 9 — Transformation Pipeline

## Step 9.1 — Create Transformation Module

Create:

```text
src/transformations.py
```

Keep transformations separate from orchestration.

---

## Step 9.2 — Build Sales Dataset

Join:

```text
orders
    |
order_items
    |
products
    |
stores
```

Use appropriate join types based on business requirements.

---

## Step 9.3 — Calculate Measures

Create:

```text
gross_sales
net_sales
gross_margin
```

---

## Step 9.4 — Build Inventory Dataset

Combine inventory snapshots with product/store attributes as required.

---

## Step 9.5 — Demonstrate Core Spark Operations

The project must meaningfully use:

```text
select
filter
withColumn
dropDuplicates
join
groupBy
agg
when
orderBy
```

---

## Step 9.6 — Implement Window Functions

Demonstrate:

```text
row_number
rank
dense_rank
rolling sum
lag
```

Use them for real business transformations such as:

- product rankings
- store rankings
- rolling revenue
- latest-record selection
- duplicate resolution

---

## Step 9.7 — Create Pipeline Entry Point

Create:

```text
src/pipeline.py
```

Pipeline responsibilities:

```text
read configuration
create Spark session
read inputs
validate inputs
separate rejects
transform accepted data
write outputs
report execution summary
```

---

## Completion Gate — Phase 9

Verify:

- [ ] Pipeline executes end to end locally.
- [ ] Sales facts are generated correctly.
- [ ] Inventory facts are generated correctly.
- [ ] Dimension datasets are generated.
- [ ] Invalid records are isolated.
- [ ] Business measures reconcile to source data.

---

# PHASE 10 — Parquet Curated Layer

## Step 10.1 — Write Curated Data as Parquet

Produce curated datasets for:

```text
dim_product
dim_store
dim_date
fact_sales
fact_inventory_snapshot
```

---

## Step 10.2 — Apply Logical Partitioning

Partition large date-oriented datasets only where justified.

Avoid excessive partition cardinality.

---

## Step 10.3 — Read Curated Data Back

Verify:

- row counts
- schema
- calculated values
- partition behaviour

---

## Completion Gate — Phase 10

Be able to explain:

- [ ] Why Parquet is preferable to CSV for analytics.
- [ ] Columnar storage.
- [ ] Compression.
- [ ] Schema preservation.
- [ ] Predicate pushdown.
- [ ] Partition pruning.
- [ ] The small-file problem.

---

# PHASE 11 — Spark Performance Concepts

## Step 11.1 — Inspect Execution Plans

Use:

```python
df.explain()
```

Inspect plans for:

- joins
- aggregations
- filters
- window operations

---

## Step 11.2 — Study Spark Partitioning

Inspect partition counts.

Experiment with:

```text
repartition()
coalesce()
```

---

## Step 11.3 — Compare Transformation Types

Identify project examples of:

### Narrow transformations

```text
select
filter
withColumn
```

### Wide transformations

```text
groupBy
distinct
orderBy
shuffle joins
```

---

## Step 11.4 — Demonstrate Broadcast Join

Use a genuinely small dimension table and compare the physical execution plan.

---

## Completion Gate — Phase 11

Be able to explain:

- [ ] Driver.
- [ ] Executor.
- [ ] Job.
- [ ] Stage.
- [ ] Task.
- [ ] Partition.
- [ ] Lazy evaluation.
- [ ] Shuffle.
- [ ] Narrow vs wide transformation.
- [ ] Broadcast join.
- [ ] Data skew.
- [ ] `repartition` vs `coalesce`.

---

# PHASE 12 — Automated Testing

## Step 12.1 — Configure Pytest

Create:

```text
tests/conftest.py
```

Provide reusable Spark test fixtures.

---

## Step 12.2 — Test Data Quality

Create:

```text
tests/test_data_quality.py
```

Test:

- duplicates
- null values
- invalid foreign keys
- invalid quantities
- invalid prices
- quarantine behaviour

---

## Step 12.3 — Test Transformations

Create:

```text
tests/test_transformations.py
```

Test:

- revenue calculations
- margin calculations
- joins
- window calculations
- deduplication
- expected row counts

---

## Step 12.4 — Run Full Test Suite

Execute:

```bash
pytest
```

---

## Completion Gate — Phase 12

Verify:

- [ ] Tests execute automatically.
- [ ] Valid cases pass.
- [ ] Known-invalid cases are caught.
- [ ] Transformation arithmetic is verified.
- [ ] Join logic is verified.

---

# PHASE 13 — SQL Layer

## Step 13.1 — Create SQL Structure

Create:

```text
sql/
├── ddl/
├── validation/
└── analytics/
```

---

## Step 13.2 — Write Validation Queries

Create SQL capable of independently verifying:

- row counts
- uniqueness
- null values
- invalid relationships
- measure totals

---

## Step 13.3 — Write Analytical Queries

Implement queries for:

1. Daily revenue
2. Revenue by store
3. Revenue by product
4. Revenue by category
5. Top 10 products
6. Store ranking within province
7. Seven-day rolling revenue
8. Average order value
9. Gross margin by category
10. Low-stock products
11. Revenue contribution percentage
12. Sales trend comparisons

---

## Step 13.4 — Required SQL Techniques

Across the query set, demonstrate:

```text
INNER JOIN
LEFT JOIN
GROUP BY
HAVING
CASE
CTEs
subqueries
ROW_NUMBER
RANK
DENSE_RANK
SUM OVER
AVG OVER
LAG
LEAD
```

---

## Completion Gate — Phase 13

Be able to reproduce core SQL without relying on generated answers.

---

# PHASE 14 — Google Cloud Storage

## Step 14.1 — Create Dedicated GCP Project

Use a project dedicated to NullMarket.

Enable only required APIs/services.

Configure billing safeguards.

---

## Step 14.2 — Configure Authentication

Use supported local/cloud authentication mechanisms.

Never place credential files in Git.

Understand:

- IAM
- roles
- permissions
- service accounts
- least privilege

---

## Step 14.3 — Create Storage Layout

Implement:

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

---

## Step 14.4 — Upload Source Data

Upload generated source datasets into the raw layer.

Verify objects before continuing.

---

## Completion Gate — Phase 14

Verify:

- [ ] Cloud project exists.
- [ ] Authentication works.
- [ ] Storage exists.
- [ ] Raw data exists in GCS.
- [ ] Credentials are absent from Git.

---

# PHASE 15 — Cloud Spark Execution

## Step 15.1 — Adapt Pipeline for Cloud Paths

The same transformation logic should accept cloud paths through configuration rather than separate hard-coded cloud code.

---

## Step 15.2 — Execute Spark Workload in GCP

Run:

```text
GCS raw
    |
    v
PySpark
    |
    v
GCS curated
```

using an appropriate managed Spark execution environment.

---

## Step 15.3 — Inspect Cloud Execution

Review:

- logs
- job status
- execution time
- failures
- input/output paths

---

## Step 15.4 — Document a Real Failure

For at least one genuine implementation problem, record:

```text
Problem
Symptoms
Root cause
Diagnostic process
Resolution
Lesson learned
```

in:

```text
docs/design_decisions.md
```

---

## Completion Gate — Phase 15

Verify:

- [ ] Spark reads from GCS.
- [ ] Spark executes remotely.
- [ ] Curated Parquet is written to GCS.
- [ ] Logs are inspectable.
- [ ] At least one troubleshooting example is understood.

---

# PHASE 16 — BigQuery Warehouse

## Step 16.1 — Create Dataset

Create a dedicated BigQuery dataset for the NullMarket warehouse.

---

## Step 16.2 — Create Tables

Create:

```text
dim_date
dim_product
dim_store
fact_sales
fact_inventory_snapshot
```

---

## Step 16.3 — Load Curated Data

Load warehouse-ready data into BigQuery.

---

## Step 16.4 — Partition Fact Tables

Use appropriate date columns.

Examples:

```text
fact_sales
-> order_date

fact_inventory_snapshot
-> snapshot_date
```

---

## Step 16.5 — Cluster Where Justified

Evaluate frequently filtered/joined columns such as:

```text
store_id
product_id
category
```

Choose clustering fields based on query patterns rather than adding them arbitrarily.

---

## Step 16.6 — Validate Warehouse

Compare BigQuery results with curated Spark outputs.

Validate:

- row counts
- totals
- uniqueness
- null constraints
- derived measures

---

## Completion Gate — Phase 16

Verify:

- [ ] All warehouse tables exist.
- [ ] Data loads successfully.
- [ ] Fact tables use sensible partitioning.
- [ ] Clustering decisions are documented.
- [ ] Warehouse totals reconcile to Spark output.

---

# PHASE 17 — Idempotency and Incremental Processing

## Step 17.1 — Test Repeated Execution

Run the same pipeline twice using the same source input.

The second execution must not create unintended duplicate business records.

---

## Step 17.2 — Establish Idempotent Strategy

Use appropriate techniques such as:

- deterministic business keys
- deduplication
- partition replacement
- overwrite semantics
- merge/upsert concepts

---

## Step 17.3 — Demonstrate Incremental Processing

Add a subsequent batch containing new business dates/records.

Process only the required data rather than rebuilding everything unnecessarily.

---

## Step 17.4 — Verify Historical Data

Ensure existing warehouse history remains correct.

---

## Completion Gate — Phase 17

Be able to explain:

- [ ] Idempotency.
- [ ] Full refresh vs incremental load.
- [ ] Upsert.
- [ ] Backfill.
- [ ] Late-arriving records.
- [ ] How duplicate ingestion is prevented.

---

# PHASE 18 — Architecture Documentation

## Step 18.1 — Create Architecture Document

Create:

```text
docs/architecture.md
```

Document:

- components
- data flow
- storage layers
- processing layer
- warehouse layer
- security boundaries

---

## Step 18.2 — Create Architecture Diagram

Diagram:

```text
Source Systems
      |
      v
Local Generation
      |
      v
GCS Raw
      |
      v
PySpark / Spark
      |
      +--> Validation
      +--> Rejected Records
      |
      v
GCS Curated Parquet
      |
      v
BigQuery
      |
      v
Analytical SQL
```

---

## Step 18.3 — Document Engineering Decisions

Create:

```text
docs/design_decisions.md
```

Explain decisions including:

- Why Spark?
- Why explicit schemas?
- Why Parquet?
- Why raw and curated layers?
- Why dimensional modeling?
- Why chosen fact grains?
- Why chosen partitions?
- Why chosen clustering?
- How failures are handled?
- How the platform could scale?

---

# PHASE 19 — Final README

Replace the minimal README with a professional project landing page.

Required sections:

1. Project overview
2. Business problem
3. Architecture diagram
4. Technology stack
5. Source datasets
6. Data model
7. Pipeline workflow
8. Data-quality strategy
9. PySpark implementation
10. BigQuery warehouse
11. SQL examples
12. Testing
13. Repository structure
14. Setup instructions
15. Design decisions
16. Scalability considerations
17. Productionization opportunities

A technical recruiter should be able to understand the project's purpose and technical depth quickly.

---

# PHASE 20 — Repository Quality Review

## Step 20.1 — Remove Unnecessary Files

Remove:

- abandoned experiments
- dead code
- unused packages
- temporary output
- generated bulk data
- unused configuration
- placeholders

---

## Step 20.2 — Security Review

Search the repository for:

- credentials
- API keys
- service-account files
- tokens
- passwords
- environment-specific secrets

None may remain.

---

## Step 20.3 — Run Project From Clean State

Using documented instructions:

1. Create environment.
2. Install dependencies.
3. Generate source data.
4. Run tests.
5. Run local pipeline.
6. Validate outputs.

---

## Step 20.4 — Review Git History

Commits should represent meaningful engineering milestones rather than arbitrary file saves.

---

## Completion Gate — Phase 20

The repository must be:

- [ ] reproducible
- [ ] understandable
- [ ] clean
- [ ] secure
- [ ] tested
- [ ] documented
- [ ] publicly presentable

---

# PHASE 21 — Technical Mastery

The project is not considered complete merely because the code executes.

Its implementation must be explainable.

---

## PySpark

Be able to explain:

- Spark architecture
- DataFrames
- transformations
- actions
- lazy evaluation
- DAGs
- jobs
- stages
- tasks
- partitions
- shuffles
- narrow transformations
- wide transformations
- broadcast joins
- data skew
- caching
- `repartition`
- `coalesce`
- window functions
- Parquet

---

## SQL

Be able to write and explain:

- joins
- aggregations
- CTEs
- subqueries
- window functions
- rankings
- rolling calculations
- deduplication
- conditional aggregation

---

## Data engineering

Be able to explain:

- ETL vs ELT
- batch vs streaming
- data lake vs warehouse
- schemas
- grain
- facts
- dimensions
- primary keys
- foreign keys
- surrogate keys
- data quality
- idempotency
- incremental loads
- backfills
- late-arriving data
- schema evolution
- orchestration
- observability
- retries
- rejected records

---

## Cloud

Be able to explain:

- object storage
- compute/storage separation
- IAM
- service accounts
- least privilege
- cloud Spark execution
- BigQuery
- partitioning
- clustering
- cloud cost considerations

---

# PHASE 22 — Scalability Analysis

Document how the design would change if data volume increased dramatically.

Discuss:

- distributed Spark execution
- partition sizing
- Parquet file sizing
- predicate pushdown
- partition pruning
- shuffle reduction
- broadcast joins
- skew mitigation
- incremental processing
- orchestration
- retries
- monitoring
- warehouse partitioning
- warehouse clustering

Never claim the demonstration dataset itself operates at enterprise scale unless that claim has actually been measured.

---

# PHASE 23 — Productionization Analysis

Document what would be required to convert NullMarket from a portfolio implementation into a production platform.

Potential additions:

- workflow orchestration
- infrastructure as code
- CI/CD
- secrets management
- environment separation
- data lineage
- centralized monitoring
- alerting
- SLAs
- automated retries
- audit tables
- schema contracts
- CDC ingestion
- disaster recovery
- formal IAM policies

These are architectural extensions, not requirements for the core implementation.

---

# PHASE 24 — Portfolio and Résumé Packaging

Present the project as:

```text
NullMarket: Retail Data Engineering Platform
```

Core technologies:

```text
PySpark | Apache Spark | SQL | GCP | BigQuery | Cloud Storage | Parquet
```

Résumé bullets must describe only functionality and results that were actually implemented or measured.

Strong bullet themes include:

- end-to-end data pipeline engineering
- distributed PySpark transformations
- multi-source retail data integration
- automated data-quality validation
- dimensional warehouse modeling
- Parquet-based curated storage
- cloud Spark execution
- partitioned and clustered BigQuery tables
- advanced analytical SQL
- automated testing
- idempotent/incremental processing

Do not invent performance improvements, scale, cost savings, or business impact.

---

# Final Definition of Done

NullMarket is complete only when all of the following are true.

## Pipeline

- [ ] Five source datasets exist.
- [ ] Explicit schemas exist.
- [ ] Source data can be generated reproducibly.
- [ ] Data-quality rules execute.
- [ ] Invalid records are handled.
- [ ] PySpark transformations execute.
- [ ] Curated Parquet datasets are produced.
- [ ] Cloud Storage is integrated.
- [ ] Spark executes in GCP.
- [ ] BigQuery warehouse exists.
- [ ] Analytical SQL works.

## Engineering

- [ ] Tests pass.
- [ ] Processing is repeatable.
- [ ] Duplicate execution does not corrupt data.
- [ ] Configuration is externalized.
- [ ] No credentials are committed.
- [ ] Code responsibilities are reasonably separated.

## Data modeling

- [ ] Every source grain is documented.
- [ ] Every fact grain is documented.
- [ ] Keys are understood.
- [ ] Dimensions are implemented.
- [ ] Facts are implemented.
- [ ] Warehouse results reconcile with transformed source data.

## Documentation

- [ ] Business requirements are complete.
- [ ] Data dictionary is complete.
- [ ] Data model is complete.
- [ ] Architecture is complete.
- [ ] Design decisions are complete.
- [ ] README is complete.

## Technical understanding

- [ ] Every major PySpark operation can be explained.
- [ ] Spark execution concepts can be explained.
- [ ] SQL can be written without dependence on generated solutions.
- [ ] Data-modeling decisions can be defended.
- [ ] Cloud architecture can be explained.
- [ ] Scaling tradeoffs can be discussed.
- [ ] Production improvements can be discussed.

---

# Execution Rule

Always complete the project in this order:

```text
Requirements
    ↓
Source-system design
    ↓
Warehouse design
    ↓
Environment
    ↓
Synthetic data
    ↓
Schemas
    ↓
Data quality
    ↓
Transformations
    ↓
Parquet
    ↓
Spark performance concepts
    ↓
Testing
    ↓
SQL
    ↓
Cloud Storage
    ↓
Cloud Spark
    ↓
BigQuery
    ↓
Idempotency / incremental processing
    ↓
Documentation
    ↓
Repository cleanup
    ↓
Technical mastery
    ↓
Portfolio packaging
```

Do not skip foundational design in order to reach cloud implementation faster.

Do not add optional technologies until the required system works end to end.

Do not treat functionality as complete until it has been validated.

Do not treat knowledge as mastered until it can be explained independently.
