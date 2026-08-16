# NullMarket: Retail Data Engineering Platform

## Dimensional Data Model

## 1. Purpose

This document defines the Phase 4 analytical warehouse model for NullMarket. The model follows the required dimensional design in `ROADMAP.md` and the business definitions in `docs/business_requirements.md`.

Required warehouse tables:

```text
dim_date
dim_product
dim_store
fact_sales
fact_inventory_snapshot
```

The design uses two fact tables that share conformed date, product, and store dimensions.

---

## 2. Modeling Principles

1. **Declare fact grain before selecting keys or measures.** Every fact row must represent one clearly defined business event or snapshot.
2. **Use warehouse keys for dimension relationships.** Product and store dimensions use surrogate keys while retaining operational IDs as business keys.
3. **Keep measures at the fact's declared grain.** Measures from incompatible grains are not stored together.
4. **Retain source business identifiers where analytically necessary.** `order_id` and `line_number` remain in `fact_sales` as degenerate identifiers.
5. **Do not invent business rules.** The current requirements do not define a revenue inclusion/exclusion rule based on `order_status`, so this model does not introduce one.
6. **Keep the core model scoped to documented analytical needs.** No customer dimension is created because no customer source table or customer-analysis requirement exists.

---

# 3. Dimensions

## 3.1 `dim_date`

**Grain:** One row per calendar date.

**Key strategy:**

- `date_key` — deterministic warehouse key in `YYYYMMDD` integer form.
- `full_date` — natural calendar-date identifier; unique.

`date_key` is deterministic rather than a generated sequence. Product and store keys are the true surrogate keys in this model.

| Column | Role | Source / Derivation |
|---|---|---|
| `date_key` | Warehouse key | Derived from `full_date` as `YYYYMMDD` |
| `full_date` | Date attribute / natural key | Calendar date |
| `day_of_week` | Attribute | Derived from `full_date` |
| `day_name` | Attribute | Derived from `full_date` |
| `day_of_month` | Attribute | Derived from `full_date` |
| `week_of_year` | Attribute | Derived from `full_date` |
| `month_number` | Attribute | Derived from `full_date` |
| `month_name` | Attribute | Derived from `full_date` |
| `quarter_number` | Attribute | Derived from `full_date` |
| `year` | Attribute | Derived from `full_date` |

**Source mapping:** The dimension is generated as a continuous calendar covering the required date range from `DATE(orders.order_timestamp)` and `inventory_snapshots.snapshot_date`.

**Rationale:** A shared date dimension provides consistent calendar attributes for both sales and inventory and supports daily, ranking, trend, and seven-day rolling analysis. A continuous date range also permits analysis of dates with no sales activity.

---

## 3.2 `dim_product`

**Grain:** One row per product in the current warehouse version.

**Key strategy:**

- `product_key` — warehouse surrogate key; primary warehouse identifier.
- `product_id` — operational natural/business key; unique alternate key.

| Column | Role | Source |
|---|---|---|
| `product_key` | Surrogate key | Warehouse-generated |
| `product_id` | Business key | `products.product_id` |
| `product_name` | Attribute | `products.product_name` |
| `category` | Attribute | `products.category` |
| `subcategory` | Attribute | `products.subcategory` |
| `brand` | Attribute | `products.brand` |
| `list_price` | Attribute | `products.list_price` |
| `active_flag` | Attribute | `products.active_flag` |

**Rationale:** Product descriptions and classifications belong in a dimension because they provide context for measures. A surrogate key decouples fact-table relationships from operational identifiers and allows future warehouse history strategies without redesigning the fact foreign-key structure.

**History limitation:** The current source contains one row per product and does not provide effective dates or version history. Phase 4 therefore does **not** claim SCD Type 2 behavior.

---

## 3.3 `dim_store`

**Grain:** One row per store in the current warehouse version.

**Key strategy:**

- `store_key` — warehouse surrogate key; primary warehouse identifier.
- `store_id` — operational natural/business key; unique alternate key.

| Column | Role | Source |
|---|---|---|
| `store_key` | Surrogate key | Warehouse-generated |
| `store_id` | Business key | `stores.store_id` |
| `store_name` | Attribute | `stores.store_name` |
| `city` | Attribute | `stores.city` |
| `province` | Attribute | `stores.province` |
| `region` | Attribute | `stores.region` |
| `store_type` | Attribute | `stores.store_type` |
| `open_date` | Attribute | `stores.open_date` |

**Rationale:** Store geography and classification are descriptive context used to group, filter, and rank sales and inventory measures. The surrogate key separates warehouse relationships from source-system identifiers.

**History limitation:** The current source provides one current row per store, so historical store-attribute versioning is not implemented in this phase.

---

# 4. Facts

## 4.1 `fact_sales`

**Grain:** One row per validated order line, uniquely identified by the source business key (`order_id`, `line_number`).

### Foreign keys

| Column | References | Source mapping |
|---|---|---|
| `date_key` | `dim_date.date_key` | `DATE(orders.order_timestamp)` |
| `store_key` | `dim_store.store_key` | `orders.store_id` -> `dim_store.store_id` |
| `product_key` | `dim_product.product_key` | `order_items.product_id` -> `dim_product.product_id` |

### Degenerate identifiers

| Column | Purpose | Source |
|---|---|---|
| `order_id` | Preserves order identity; required for distinct-order calculations such as average order value | `order_items.order_id` / `orders.order_id` |
| `line_number` | Identifies the individual line within the order and completes the fact's business key | `order_items.line_number` |

No separate fact surrogate key is required for the core design because (`order_id`, `line_number`) already expresses and validates the declared grain.

### Measures

| Measure | Type | Definition / Source | Additivity |
|---|---|---|---|
| `quantity` | INTEGER | `order_items.quantity` | Additive |
| `unit_price` | DECIMAL(12,2) | `order_items.unit_price` | Non-additive unit value |
| `discount_amount` | DECIMAL(12,2) | `order_items.discount_amount` | Additive |
| `unit_cost` | DECIMAL(12,2) | `products.unit_cost` captured into the fact during transformation | Non-additive unit value |
| `gross_sales` | DECIMAL | `quantity * unit_price` | Additive |
| `net_sales` | DECIMAL | `gross_sales - discount_amount` | Additive |
| `gross_margin` | DECIMAL | `net_sales - (quantity * unit_cost)` | Additive |

### Source mapping

```text
orders
  + order_items
  + products
  -> dimension-key lookups
  -> fact_sales
```

`stores` is not required to calculate sales measures; store attributes are reached through `dim_store` after `orders.store_id` is mapped to `store_key`.

### Design rationale

Order-line grain is required because price, quantity, discount, product, and therefore gross margin are defined at the line level. Modeling one row per order would require premature aggregation and would lose product-level analysis.

`unit_cost` is stored in the fact because it is an input to the line-level margin calculation. Under the current source design it comes from the product record available during processing; this does not claim reconstruction of historical transaction-time cost if product costs later change without source history.

The documented business requirements do not specify that revenue should include or exclude particular `order_status` values. The warehouse must therefore not silently apply a status filter unless the requirements are explicitly changed.

---

## 4.2 `fact_inventory_snapshot`

**Grain:** One row per validated product, per store, per snapshot date.

**Business uniqueness:** (`date_key`, `store_key`, `product_key`) represents the declared snapshot grain.

### Foreign keys

| Column | References | Source mapping |
|---|---|---|
| `date_key` | `dim_date.date_key` | `inventory_snapshots.snapshot_date` |
| `store_key` | `dim_store.store_key` | `inventory_snapshots.store_id` -> `dim_store.store_id` |
| `product_key` | `dim_product.product_key` | `inventory_snapshots.product_id` -> `dim_product.product_id` |

### Measures and derived indicator

| Column | Role | Definition / Source | Additivity |
|---|---|---|---|
| `quantity_on_hand` | Measure | `inventory_snapshots.quantity_on_hand` | Semi-additive: additive across products/stores for the same snapshot date; not additive across dates |
| `reorder_level` | Measure / threshold | `inventory_snapshots.reorder_level` | Non-additive across dates; aggregation must match analytical intent |
| `is_low_stock` | Derived Boolean indicator | `quantity_on_hand < reorder_level` | Not additive |

### Source mapping

```text
inventory_snapshots
  -> dimension-key lookups
  -> fact_inventory_snapshot
```

### Design rationale

Inventory represents **state at a point in time**, not a transaction. Snapshot grain preserves historical inventory positions so analysts can retrieve the latest state or analyze inventory over time.

`quantity_on_hand` must not be summed across snapshot dates because doing so double-counts repeated observations of inventory state.

---

# 5. Relationships

```text
                    dim_date
                    /      \
                   /        \
          fact_sales      fact_inventory_snapshot
             |   \            /   |
             |    \          /    |
             v     v        v     v
       dim_store  dim_product
```

Logical relationships:

```text
dim_date.date_key      1 -> many fact_sales.date_key
dim_store.store_key    1 -> many fact_sales.store_key
dim_product.product_key 1 -> many fact_sales.product_key

dim_date.date_key      1 -> many fact_inventory_snapshot.date_key
dim_store.store_key    1 -> many fact_inventory_snapshot.store_key
dim_product.product_key 1 -> many fact_inventory_snapshot.product_key
```

The two facts share **conformed dimensions** but remain separate because they represent incompatible grains and different business processes.

---

# 6. Grain-Safety Rules

### Sales

```text
One fact row = one order line
```

Do not join another one-to-many dataset to `fact_sales` before confirming that the join preserves (`order_id`, `line_number`) uniqueness.

### Inventory

```text
One fact row = one product x one store x one snapshot date
```

Do not aggregate `quantity_on_hand` across dates as if inventory snapshots were transactions.

### Sales vs inventory

Do **not** directly join raw `fact_sales` rows to `fact_inventory_snapshot` rows and then sum measures. A store/product/date may have many sales lines but one inventory snapshot, which would repeat the inventory measure once per sales line.

For combined sales/inventory analysis, first aggregate sales to a compatible grain such as:

```text
date x store x product
```

and then join that aggregate to the inventory snapshot at the same grain.

---

# 7. Scope Decisions

The following source fields are not promoted into new dimensions in the core Phase 4 model:

- `customer_id` — no customer source table or required customer analytics.
- `payment_method` — no required analysis by payment method.
- `channel` — no required analysis by channel.
- `order_status` — no documented analytical grouping or revenue-status rule.

They remain available in the raw source layer and can be modeled later if business requirements expand.

---

# 8. Interview-Defensible Summary

- A **dimension** describes business entities used to filter, group, and label facts; NullMarket uses date, product, and store dimensions.
- A **fact** records measurable business activity or state; sales is transactional, while inventory is periodic snapshot state.
- A **measure** is a numeric value analyzed or aggregated, such as `net_sales`; an **attribute** describes context, such as `category` or `province`.
- A **natural key** originates from the business/source system, such as `product_id`; a **surrogate key** is warehouse-generated, such as `product_key`.
- `fact_sales` uses order-line grain because sales price, quantity, discount, product, and margin are defined at that level.
- `fact_inventory_snapshot` uses date/store/product grain because inventory is observed state that changes over time.
- The facts stay separate because combining transaction and snapshot measures in one table would mix incompatible grains and make aggregation unsafe.
