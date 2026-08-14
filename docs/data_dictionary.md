# NullMarket: Retail Data Engineering Platform

## Source Data Dictionary

This document defines the grain, keys, proposed source data types, business meaning, and validation requirements for NullMarket's five operational source datasets.

---

# 1. `orders`

**Grain:** One row per order.

**Primary key:** `order_id`

| Column | Data Type | Nullable? | Key Type | Business Definition | Validation Rule |
|---|---|---:|---|---|---|
| `order_id` | STRING | No | Primary Key | Unique business identifier for an order. | Required; non-blank; unique. |
| `customer_id` | STRING | No | — | Identifier of the customer associated with the order. | Required; non-blank. |
| `store_id` | STRING | No | Foreign Key | Store at which the order is attributed. | Required; must exist in `stores.store_id`. |
| `order_timestamp` | TIMESTAMP | No | — | Date and time at which the order was recorded. | Required; must be a valid timestamp. |
| `order_status` | STRING | No | — | Operational status of the order. | Required; non-blank. |
| `payment_method` | STRING | No | — | Method used to pay for the order. | Required; non-blank. |
| `channel` | STRING | No | — | Sales channel through which the order was placed. | Required; non-blank. |

---

# 2. `order_items`

**Grain:** One product line within an order.

**Primary key:** (`order_id`, `line_number`)

| Column | Data Type | Nullable? | Key Type | Business Definition | Validation Rule |
|---|---|---:|---|---|---|
| `order_id` | STRING | No | Composite PK / Foreign Key | Order containing the line item. | Required; must exist in `orders.order_id`; unique with `line_number`. |
| `line_number` | INTEGER | No | Composite PK | Sequence number identifying a line within an order. | Required; positive integer; unique within an order. |
| `product_id` | STRING | No | Foreign Key | Product purchased on the order line. | Required; must exist in `products.product_id`. |
| `quantity` | INTEGER | No | — | Number of units purchased. | `quantity > 0`. |
| `unit_price` | DECIMAL(12,2) | No | — | Selling price per unit associated with the order line. | `unit_price >= 0`. |
| `discount_amount` | DECIMAL(12,2) | No | — | Total discount applied to the order line. | `discount_amount >= 0`. |

---

# 3. `products`

**Grain:** One row per product.

**Primary key:** `product_id`

| Column | Data Type | Nullable? | Key Type | Business Definition | Validation Rule |
|---|---|---:|---|---|---|
| `product_id` | STRING | No | Primary Key | Unique business identifier for a product. | Required; non-blank; unique. |
| `product_name` | STRING | No | — | Business-facing name of the product. | Required; non-blank. |
| `category` | STRING | No | — | High-level merchandise category. | Required; non-blank. |
| `subcategory` | STRING | No | — | More specific classification within a category. | Required; non-blank. |
| `brand` | STRING | No | — | Brand associated with the product. | Required; non-blank. |
| `unit_cost` | DECIMAL(12,2) | No | — | NullMarket's cost for one unit of the product. | `unit_cost >= 0`. |
| `list_price` | DECIMAL(12,2) | No | — | Standard listed selling price of the product. | `list_price >= 0`. |
| `active_flag` | BOOLEAN | No | — | Indicates whether the product is currently active in the catalogue. | Required; valid Boolean value. |

---

# 4. `stores`

**Grain:** One row per store.

**Primary key:** `store_id`

| Column | Data Type | Nullable? | Key Type | Business Definition | Validation Rule |
|---|---|---:|---|---|---|
| `store_id` | STRING | No | Primary Key | Unique business identifier for a NullMarket store. | Required; non-blank; unique. |
| `store_name` | STRING | No | — | Business-facing name of the store. | Required; non-blank. |
| `city` | STRING | No | — | Canadian city in which the store operates. | Required; non-blank. |
| `province` | STRING | No | — | Canadian province in which the store operates. | Required; non-blank. |
| `region` | STRING | No | — | Business region used to group stores geographically. | Required; non-blank. |
| `store_type` | STRING | No | — | Operational classification of the store. | Required; non-blank. |
| `open_date` | DATE | No | — | Date on which the store opened. | Required; valid date. |

---

# 5. `inventory_snapshots`

**Grain:** One product, at one store, on one snapshot date.

**Primary key:** (`snapshot_date`, `store_id`, `product_id`)

| Column | Data Type | Nullable? | Key Type | Business Definition | Validation Rule |
|---|---|---:|---|---|---|
| `snapshot_date` | DATE | No | Composite PK | Date on which the inventory position was observed. | Required; valid date; unique with `store_id` and `product_id`. |
| `store_id` | STRING | No | Composite PK / Foreign Key | Store whose inventory is being measured. | Required; must exist in `stores.store_id`. |
| `product_id` | STRING | No | Composite PK / Foreign Key | Product whose inventory is being measured. | Required; must exist in `products.product_id`. |
| `quantity_on_hand` | INTEGER | No | — | Number of units physically available at the snapshot time. | `quantity_on_hand >= 0`. |
| `reorder_level` | INTEGER | No | — | Inventory threshold below which replenishment is indicated. | `reorder_level >= 0`. |

---

# 6. Source Relationships

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

`customer_id` is retained as an operational identifier, but no customer source dataset is included in the current NullMarket project scope.

---

# 7. Grain Summary

| Dataset | Grain | Unique Identifier |
|---|---|---|
| `orders` | One order | `order_id` |
| `order_items` | One product line within one order | (`order_id`, `line_number`) |
| `products` | One product | `product_id` |
| `stores` | One store | `store_id` |
| `inventory_snapshots` | One product at one store on one snapshot date | (`snapshot_date`, `store_id`, `product_id`) |

Grain must be established before joins and aggregations because it defines what each row represents and therefore how records may safely be combined without unintentionally duplicating measures.
