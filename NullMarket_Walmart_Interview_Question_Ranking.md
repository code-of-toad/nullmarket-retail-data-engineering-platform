# NullMarket Phase 2 — Walmart Data Engineering Interview Ranking

**Purpose:** Rank the 75 Phase 2 questions by likely usefulness in a Walmart Data Developer III / data engineering interview.

**How to use this file**
- **Critical:** Be able to answer naturally without notes.
- **High:** Strong follow-up territory; know confidently.
- **Medium:** Understand clearly, but do not spend equal study time here.
- **Lower:** Useful context, but less likely to be a primary interview question.

> The answers below use NullMarket's documented business requirements and roadmap. The **ranking itself is interview-preparation judgment**, not a claim made by the project documentation.

---

## Ranked Questions and Concise Answers

### 1. What business problem does NullMarket solve?
**Priority: Critical**

NullMarket consolidates sales, order-line, product, store, and inventory data from separate operational sources into a validated analytical platform. Its purpose is to give downstream users a consistent, trustworthy view of retail operations instead of forcing them to reconcile disconnected source systems themselves.

### 2. Walk through NullMarket's processing sequence from ingestion through warehouse validation.
**Priority: Critical**

The flow is:

`Ingest → enforce schema → validate → separate accepted/rejected records → clean/deduplicate → transform/join → calculate metrics → write curated Parquet → load BigQuery → reconcile warehouse results`.

### 3. How do you ensure bad source data does not contaminate trusted analytical data?
**Priority: Critical**

Apply explicit schemas and reusable validation rules for nulls, key uniqueness, referential integrity, numeric ranges, dates, and business rules. Invalid records are quarantined with rejection reasons and excluded from curated analytical data.

### 4. What is idempotency?
**Priority: Critical**

Idempotency means processing the same input repeatedly produces the same intended business result without creating duplicate or corrupted records.

### 5. What is the grain of each source dataset?
**Priority: Critical**

- `orders`: one row per order  
- `order_items`: one row per product line within an order  
- `products`: one row per product  
- `stores`: one row per store  
- `inventory_snapshots`: one row per product, per store, per snapshot date

### 6. Why is grain important?
**Priority: Critical**

Grain defines exactly what one row represents. You must know it before joining or aggregating tables because mismatched grains can multiply rows and overstate measures such as revenue.

### 7. What is referential integrity?
**Priority: Critical**

Referential integrity means a foreign-key value must reference a valid parent record when the relationship requires it—for example, an `order_items.product_id` should correspond to an existing `products.product_id`.

### 8. What does schema enforcement mean, and why do we require it?
**Priority: Critical**

Schema enforcement means processing each source with explicitly defined columns and data types instead of trusting inferred structure. It catches malformed input early and prevents unexpected types from silently propagating downstream.

### 9. What is incremental processing, and why is it preferable to continually rebuilding all history?
**Priority: Critical**

Incremental processing handles only new or changed batches when possible. It avoids unnecessary reprocessing, reduces compute and data movement, and is more scalable than rebuilding the entire historical dataset every run.

### 10. How is this system designed for correctness, repeatability, and scalability?
**Priority: Critical**

Correctness comes from schemas, validation, reconciliation, and tests. Repeatability comes from deterministic generation, configuration, idempotent processing, and documented setup. Scalability comes from distributed Spark patterns, Parquet, partitioning, incremental processing, and cloud storage/warehouse separation.

### 11. Why is this primarily a data engineering problem rather than simply an analytics problem?
**Priority: Critical**

The core challenge is creating reliable data before analysis can happen: ingesting multiple sources, enforcing schemas, validating quality, resolving relationships, transforming data, storing it efficiently, and delivering trusted warehouse tables.

### 12. What are the five operational source datasets?
**Priority: Critical**

`orders`, `order_items`, `products`, `stores`, and `inventory_snapshots`.

### 13. Why are `orders` and `order_items` separate datasets?
**Priority: Critical**

They represent different grains. An order has one header record, while an order can contain many product lines. Separating them avoids repeating order-level attributes for every item and models the one-to-many relationship correctly.

### 14. What is a composite key, and where does NullMarket use one?
**Priority: Critical**

A composite key uses multiple columns together to uniquely identify a row. NullMarket uses `(order_id, line_number)` for `order_items` and `(snapshot_date, store_id, product_id)` for `inventory_snapshots`.

### 15. What could happen if a non-idempotent pipeline processes the same batch twice?
**Priority: Critical**

It could duplicate facts, double-count revenue or inventory, corrupt aggregates, and make downstream reporting unreliable.

### 16. What should happen when a source record fails validation?
**Priority: High**

It should be identifiable and separated into a rejected/quarantine dataset with enough context to explain why it failed. It should not enter trusted curated data.

### 17. Why shouldn't invalid records simply be deleted?
**Priority: High**

Deleting them destroys evidence. Keeping rejected records supports troubleshooting, auditability, root-cause analysis, correction, and possible reprocessing.

### 18. What is the difference between a technically valid record and a business-valid record?
**Priority: High**

A technically valid record may match the schema but still violate business rules. For example, `quantity = -3` is a valid integer but invalid for a sales quantity because NullMarket requires `quantity > 0`.

### 19. What role does Spark/PySpark play?
**Priority: High**

Spark/PySpark is the distributed processing layer. It reads source data, enforces schemas, validates and transforms records, performs joins and aggregations, and writes curated outputs.

### 20. What role does BigQuery play?
**Priority: High**

BigQuery is the analytical warehouse. It stores curated fact and dimension tables and supports scalable analytical SQL, partitioning, clustering, and downstream business analysis.

### 21. What role does Google Cloud Storage play?
**Priority: High**

GCS is the object-storage layer for raw source data, curated Parquet outputs, and rejected records where appropriate.

### 22. Why separate storage, processing, and warehouse responsibilities?
**Priority: High**

Separation lets each component specialize: GCS provides durable scalable storage, Spark provides distributed processing, and BigQuery provides optimized analytical querying. It also allows compute and storage to scale independently.

### 23. What transformations does the pipeline need to perform?
**Priority: High**

Cleansing, deduplication, joins, derived columns, aggregations, ranking, rolling calculations, and latest-record selection.

### 24. What types of validation must occur before data becomes trusted?
**Priority: High**

Required-value checks, key uniqueness, composite-key uniqueness, referential integrity, valid numeric ranges, valid dates/timestamps, and defined business-rule checks.

### 25. What makes a primary key valid?
**Priority: High**

It must be non-null and uniquely identify one record at the table's declared grain.

### 26. Give three referential-integrity relationships in NullMarket.
**Priority: High**

Examples:
- `order_items.order_id → orders.order_id`
- `order_items.product_id → products.product_id`
- `orders.store_id → stores.store_id`

### 27. What would an orphaned `order_items` record look like?
**Priority: High**

An `order_items` row whose `order_id` does not exist in `orders`, or whose `product_id` does not exist in `products`.

### 28. Why does the analytical layer use fact and dimension tables?
**Priority: High**

Facts store measurable business events or snapshots; dimensions provide descriptive context. This structure makes analytical queries simpler, consistent, and efficient.

### 29. Why do we need both a curated storage layer and BigQuery?
**Priority: High**

Curated Parquet provides a validated, reusable storage layer independent of the warehouse. BigQuery provides warehouse semantics and optimized SQL consumption. Keeping them separate preserves reusable transformed data and avoids coupling all processing directly to the warehouse.

### 30. Why use Parquet for curated data?
**Priority: High**

Parquet is columnar, compressed, schema-aware, and efficient for analytical workloads because engines can read only needed columns and benefit from predicate pushdown and partition pruning.

### 31. What does repeatable execution require?
**Priority: High**

Deterministic inputs where applicable, externalized configuration, reproducible dependencies, idempotent processing, documented setup, and consistent transformation logic.

### 32. What should pipeline observability tell an engineer?
**Priority: High**

Whether processing succeeded, how many records were processed and rejected, where failures occurred, and enough execution information to diagnose problems.

### 33. Why is `inventory_snapshots` a historical snapshot dataset rather than simply one current inventory value?
**Priority: High**

A snapshot dataset preserves inventory state over time. That enables trend analysis and historical reconstruction instead of only knowing the latest quantity.

### 34. Why do we need both `products` and `order_items` to calculate gross margin?
**Priority: High**

`order_items` provides quantity, selling price, and discount; `products` provides `unit_cost`. Gross margin requires both sales information and cost.

### 35. How do you calculate gross margin?
**Priority: High**

`gross_margin = net_sales - (quantity × unit_cost)`

### 36. How do you calculate net sales?
**Priority: High**

`net_sales = gross_sales - discount_amount`

### 37. How do you calculate gross sales?
**Priority: High**

`gross_sales = quantity × unit_price`

### 38. How do you calculate average order value?
**Priority: High**

`average_order_value = total net sales / number of distinct orders`

### 39. Why does average order value use distinct orders rather than order-item rows?
**Priority: High**

Because AOV measures value per order, not per product line. Counting order-item rows would incorrectly treat multi-line orders as multiple orders.

### 40. What's the difference between revenue and gross margin?
**Priority: High**

Revenue measures sales generated. Gross margin subtracts product cost from net sales, so it measures how much remains after the cost of goods sold.

### 41. Which datasets would you combine to analyze sales by province?
**Priority: High**

At minimum: `orders` for store/order context, `order_items` for sales measures, and `stores` for province. `products` is added if product/category analysis is also needed.

### 42. Which datasets would you combine to determine whether a particular product is low-stock at a store?
**Priority: High**

`inventory_snapshots` provides quantity and reorder level; `products` provides product context; `stores` provides store context. Low stock means `quantity_on_hand < reorder_level`.

### 43. What distinguishes curated data from raw data?
**Priority: High**

Raw data preserves source records as ingested. Curated data has been validated, cleaned, standardized, transformed, and made suitable for downstream warehouse loading and analytics.

### 44. Why preserve raw source data instead of immediately transforming it?
**Priority: High**

Raw preservation supports reprocessing, debugging, traceability, and recovery if transformation logic changes or an error is discovered later.

### 45. What are the raw, rejected, and curated layers?
**Priority: High**

- **Raw:** original ingested source records  
- **Rejected:** records that fail required validation  
- **Curated:** validated and transformed data suitable for downstream use

### 46. Why is NullMarket designed primarily as a batch pipeline?
**Priority: Medium**

The documented use cases do not require real-time processing. Sales and inventory analysis can be satisfied with periodic batches, keeping the implementation focused on core data-engineering fundamentals.

### 47. Why might BigQuery tables be partitioned or clustered?
**Priority: Medium**

Partitioning can reduce scanned data for date-based queries. Clustering organizes data around frequently filtered or joined columns, potentially improving query efficiency.

### 48. What does scalability mean here, given that the demonstration dataset isn't Walmart-sized?
**Priority: Medium**

It means using design patterns that can extend to larger workloads—distributed processing, efficient formats, partitioning, incremental loads, and separated storage/compute—without falsely claiming that the demo itself proves enterprise scale.

### 49. Why is it better to demonstrate scalable engineering patterns than falsely claim that the demo processes enterprise-scale volumes?
**Priority: Medium**

Because architectural understanding is defensible; fabricated scale is not. The project demonstrates techniques appropriate for larger systems while accurately describing the actual dataset and measurements.

### 50. What does correctness mean for NullMarket?
**Priority: Medium**

Analytical outputs must reconcile with valid source data and the documented business rules, including measures, row counts, keys, and relationships.

### 51. What makes the pipeline reproducible?
**Priority: Medium**

A documented environment, pinned dependencies, deterministic synthetic-data generation, centralized configuration, version-controlled code, and repeatable execution steps.

### 52. How does separating schemas, validation, transformations, configuration, and pipeline execution improve maintainability?
**Priority: Medium**

Each concern can change and be tested independently. This reduces coupling, makes failures easier to locate, and prevents orchestration code from becoming a monolithic collection of business rules.

### 53. Why does testability matter in a data pipeline?
**Priority: Medium**

Pipeline errors can silently corrupt downstream analytics. Automated tests verify validation logic, joins, calculations, deduplication, and expected row behavior before changes are trusted.

### 54. What does least privilege mean for cloud security?
**Priority: Medium**

Users and service accounts should receive only the permissions needed to perform their required tasks—nothing broader.

### 55. Why shouldn't cloud credentials ever appear in the Git repository?
**Priority: Medium**

A public or shared repository can expose credentials to unauthorized users. Secrets should be managed outside source control using supported authentication and secret-management mechanisms.

### 56. What is the end-to-end responsibility of the platform?
**Priority: Medium**

Ingest operational data, enforce schemas, validate quality, isolate failures, transform accepted data, calculate standardized metrics, write curated data, load warehouse tables, and validate analytical results.

### 57. What does it mean to create a centralized and trustworthy analytical representation of retail operations?
**Priority: Medium**

It means multiple operational sources are reconciled into consistent definitions, validated relationships, standardized metrics, and documented analytical tables that consumers can rely on.

### 58. Why can't analysts simply query the five operational datasets independently?
**Priority: Medium**

Important business questions require combining sources with different grains and relationships. Leaving that work to every analyst creates duplicated logic, inconsistent definitions, incorrect joins, and inconsistent results.

### 59. What are the major business objectives of NullMarket?
**Priority: Medium**

Consolidate multiple sources, produce validated datasets, isolate bad records, calculate standardized sales/profitability metrics, support store/product/inventory analysis, preserve history, and provide repeatable analytical processing.

### 60. Who are the primary consumers of the platform?
**Priority: Medium**

Business analysts, operations teams, merchandising teams, and data/analytics teams.

### 61. Give an example of how combining multiple source systems creates information that no individual source can provide.
**Priority: Medium**

To calculate gross margin by store, you need order/store context from `orders`, quantities and selling prices from `order_items`, costs from `products`, and store attributes from `stores`.

### 62. What does a business analyst need from the platform?
**Priority: Medium**

Trusted, consistent datasets for sales, product, store, category, and inventory analysis without repeatedly rebuilding source-system logic.

### 63. What does an operations team need from it?
**Priority: Medium**

Inventory availability, low-stock identification, store-level inventory positions, and sales trends that support operational decisions.

### 64. What does a merchandising team need from it?
**Priority: Medium**

Product demand, category performance, product rankings, and gross-margin information.

### 65. What does the data/analytics team need that business users may not directly care about?
**Priority: Medium**

Documented schemas, consistent definitions, validation results, reproducible transformations, lineage, testability, and reliable analytical tables.

### 66. What business concept does each source dataset represent?
**Priority: Medium**

- `orders`: customer orders  
- `order_items`: products purchased within each order  
- `products`: product catalogue  
- `stores`: retail locations  
- `inventory_snapshots`: product/store inventory state at points in time

### 67. What does revenue contribution mean?
**Priority: Medium**

The percentage of total net sales attributable to a particular store, product, category, or other grouping.

### 68. What makes an inventory record low stock?
**Priority: Medium**

`quantity_on_hand < reorder_level`

### 69. Why should `quantity > 0`?
**Priority: Medium**

A normal sales order line represents a positive quantity purchased. Zero or negative quantities violate the documented NullMarket business rule and should be rejected under the current model.

### 70. Why must monetary and inventory fields have validation rules?
**Priority: Medium**

Values such as negative price, cost, quantity-on-hand, or reorder level could produce misleading business metrics despite being syntactically valid numbers.

### 71. What is the difference between a functional requirement and a non-functional requirement?
**Priority: Lower**

A functional requirement defines what the system must do, such as validate data or load BigQuery. A non-functional requirement defines qualities of the system, such as correctness, reproducibility, maintainability, security, and scalability.

### 72. Why are Kafka and streaming deliberately out of scope?
**Priority: Lower**

The required use cases are batch-oriented, and streaming would add complexity without materially improving the core learning objectives. Streaming is a future extension, not a requirement.

### 73. Why isn't a dashboard required?
**Priority: Lower**

NullMarket is a data-engineering project. Its core responsibility ends with trustworthy analytical datasets and SQL capability; dashboarding is a downstream consumption concern.

### 74. Why isn't Kubernetes required?
**Priority: Lower**

The project does not need container orchestration to demonstrate its required data-engineering capabilities. Adding Kubernetes would increase scope without materially strengthening the core implementation.

### 75. If this became a production retail platform, which out-of-scope capabilities might you add?
**Priority: Lower**

Depending on requirements: orchestration, CI/CD, infrastructure as code, centralized monitoring/alerting, lineage, schema contracts, secret management, environment separation, CDC, audit tables, formal IAM, retries, disaster recovery, and possibly streaming.

---

# The 15 I Would Study First

If interview preparation time is limited, be able to answer these **without notes**:

1. What business problem does NullMarket solve?
2. Walk through the pipeline end to end.
3. How do you stop bad data from contaminating trusted data?
4. What is idempotency?
5. What is the grain of each source?
6. Why does grain matter?
7. What is referential integrity?
8. What is schema enforcement?
9. What is incremental processing?
10. How is the design correct, repeatable, and scalable?
11. Why is this a data-engineering problem?
12. Why are `orders` and `order_items` separate?
13. What are the composite keys?
14. What happens if the same batch is processed twice?
15. Explain the responsibilities of GCS, Spark, and BigQuery.

---

# Interview Compression: Five Answers to Know Cold

## 1. What business problem does NullMarket solve?

NullMarket combines disconnected retail operational data into a validated analytical platform. It ensures sales, product, store, and inventory data are consistently defined, quality-checked, transformed, and made trustworthy for downstream analytics.

## 2. Walk me through the source systems.

`orders` is one row per order; `order_items` is one row per product line; `products` is one row per product; `stores` is one row per store; and `inventory_snapshots` is one row per product, store, and date. Their different grains are intentional and determine how they can safely be joined.

## 3. How do you prevent bad source data from contaminating trusted data?

I enforce explicit schemas, validate required fields, key uniqueness, referential integrity, numeric ranges, dates, and business rules, then separate failed records into a rejected layer with reasons. Only accepted data continues into curated outputs.

## 4. Walk me through the architecture.

Operational data lands in GCS raw storage. PySpark reads it with explicit schemas, validates and transforms it, and writes curated Parquet plus rejected records. Curated datasets are then loaded into dimensional BigQuery tables, where SQL validation reconciles warehouse outputs against Spark results.

## 5. How is it designed for correctness, repeatability, and scalability?

Correctness comes from validation, testing, explicit business rules, and reconciliation. Repeatability comes from deterministic inputs, external configuration, reproducible dependencies, and idempotency. Scalability comes from Spark's distributed model, Parquet, partitioning, incremental processing, and separation of cloud storage, compute, and warehouse layers.

---

## One Important Interview Principle

Do not present NullMarket as proof that you have operated Walmart-scale production data.

A stronger and defensible framing is:

> “The demonstration dataset is intentionally modest, but the project lets me implement and explain engineering patterns used in scalable systems: explicit schemas, distributed Spark transformations, partitioned columnar storage, data-quality gates, idempotent and incremental processing, and a cloud analytical warehouse.”

That demonstrates understanding without inflating the project's scale.
