-- =============================================================================
-- NullMarket — Phase 13 SQL Layer
-- File: sql/validation/01_warehouse_validation.sql
--
-- PURPOSE
--   Independently validate the persisted warehouse-shaped curated data.
--
-- ASSUMPTION
--   Run this file with sql/ddl/00_curated_views.sql as the Spark SQL
--   initialization file so the five temporary views already exist.
--
-- VALIDATION COVERAGE
--   1. Row counts
--   2. Uniqueness / declared grain
--   3. Null values
--   4. Invalid fact-to-dimension relationships
--   5. Sales measure totals and row-level arithmetic
--
-- INTERVIEW POINT
--   These checks do not trust the PySpark transformation code merely because it
--   completed successfully. They query the persisted Parquet layer again using
--   SQL, which gives us an independent reconciliation path.
-- =============================================================================


-- =============================================================================
-- VALIDATION 1 — ROW COUNTS
-- =============================================================================
-- Row counts are not, by themselves, proof of correctness, but they are a basic
-- reconciliation signal and make unexpected row loss/multiplication visible.

SELECT 'dim_date' AS dataset, COUNT(*) AS row_count
FROM dim_date
UNION ALL
SELECT 'dim_product', COUNT(*)
FROM dim_product
UNION ALL
SELECT 'dim_store', COUNT(*)
FROM dim_store
UNION ALL
SELECT 'fact_sales', COUNT(*)
FROM fact_sales
UNION ALL
SELECT 'fact_inventory_snapshot', COUNT(*)
FROM fact_inventory_snapshot
ORDER BY dataset;


-- =============================================================================
-- VALIDATION 2 — UNIQUENESS / DECLARED GRAIN
-- =============================================================================
-- Expected result: duplicate_key_groups = 0 for every check.
--
-- HAVING is the natural SQL tool here because duplicate detection happens only
-- after rows have been grouped by the complete key. Checking the FULL composite
-- key is critical: checking its columns individually would not validate grain.

SELECT
    'dim_date.date_key' AS key_check,
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT date_key
    FROM dim_date
    GROUP BY date_key
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'dim_date.full_date',
    COUNT(*)
FROM (
    SELECT full_date
    FROM dim_date
    GROUP BY full_date
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'dim_product.product_key',
    COUNT(*)
FROM (
    SELECT product_key
    FROM dim_product
    GROUP BY product_key
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'dim_product.product_id',
    COUNT(*)
FROM (
    SELECT product_id
    FROM dim_product
    GROUP BY product_id
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'dim_store.store_key',
    COUNT(*)
FROM (
    SELECT store_key
    FROM dim_store
    GROUP BY store_key
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'dim_store.store_id',
    COUNT(*)
FROM (
    SELECT store_id
    FROM dim_store
    GROUP BY store_id
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'fact_sales(order_id,line_number)',
    COUNT(*)
FROM (
    SELECT order_id, line_number
    FROM fact_sales
    GROUP BY order_id, line_number
    HAVING COUNT(*) > 1
) AS duplicates

UNION ALL

SELECT
    'fact_inventory_snapshot(date_key,store_key,product_key)',
    COUNT(*)
FROM (
    SELECT date_key, store_key, product_key
    FROM fact_inventory_snapshot
    GROUP BY date_key, store_key, product_key
    HAVING COUNT(*) > 1
) AS duplicates

ORDER BY key_check;


-- =============================================================================
-- VALIDATION 3 — NULL VALUES
-- =============================================================================
-- Expected result: every reported null count = 0.
--
-- Each SUM(CASE ...) is conditional aggregation: it counts violations without
-- losing the one-row validation summary for the table.

SELECT
    'dim_date' AS dataset,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) AS null_date_key,
    SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END) AS null_full_date
FROM dim_date;

SELECT
    'dim_product' AS dataset,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS null_product_name,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category
FROM dim_product;

SELECT
    'dim_store' AS dataset,
    SUM(CASE WHEN store_key IS NULL THEN 1 ELSE 0 END) AS null_store_key,
    SUM(CASE WHEN store_id IS NULL THEN 1 ELSE 0 END) AS null_store_id,
    SUM(CASE WHEN store_name IS NULL THEN 1 ELSE 0 END) AS null_store_name,
    SUM(CASE WHEN province IS NULL THEN 1 ELSE 0 END) AS null_province
FROM dim_store;

SELECT
    'fact_sales' AS dataset,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) AS null_date_key,
    SUM(CASE WHEN store_key IS NULL THEN 1 ELSE 0 END) AS null_store_key,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN line_number IS NULL THEN 1 ELSE 0 END) AS null_line_number,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
    SUM(CASE WHEN discount_amount IS NULL THEN 1 ELSE 0 END) AS null_discount_amount,
    SUM(CASE WHEN unit_cost IS NULL THEN 1 ELSE 0 END) AS null_unit_cost,
    SUM(CASE WHEN gross_sales IS NULL THEN 1 ELSE 0 END) AS null_gross_sales,
    SUM(CASE WHEN net_sales IS NULL THEN 1 ELSE 0 END) AS null_net_sales,
    SUM(CASE WHEN gross_margin IS NULL THEN 1 ELSE 0 END) AS null_gross_margin
FROM fact_sales;

SELECT
    'fact_inventory_snapshot' AS dataset,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) AS null_date_key,
    SUM(CASE WHEN store_key IS NULL THEN 1 ELSE 0 END) AS null_store_key,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_key,
    SUM(CASE WHEN quantity_on_hand IS NULL THEN 1 ELSE 0 END) AS null_quantity_on_hand,
    SUM(CASE WHEN reorder_level IS NULL THEN 1 ELSE 0 END) AS null_reorder_level,
    SUM(CASE WHEN is_low_stock IS NULL THEN 1 ELSE 0 END) AS null_is_low_stock
FROM fact_inventory_snapshot;


-- =============================================================================
-- VALIDATION 4 — INVALID RELATIONSHIPS
-- =============================================================================
-- Expected result: all orphan counts = 0.
--
-- DISTINCT parent-key CTEs make the referential-integrity check resistant to
-- accidental duplicate rows in a dimension. Dimension duplication is reported
-- separately by Validation 2 rather than multiplying fact rows here.

WITH
date_keys AS (
    SELECT DISTINCT date_key
    FROM dim_date
),
store_keys AS (
    SELECT DISTINCT store_key
    FROM dim_store
),
product_keys AS (
    SELECT DISTINCT product_key
    FROM dim_product
)
SELECT
    'fact_sales' AS dataset,
    SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS invalid_date_relationships,
    SUM(CASE WHEN s.store_key IS NULL THEN 1 ELSE 0 END) AS invalid_store_relationships,
    SUM(CASE WHEN p.product_key IS NULL THEN 1 ELSE 0 END) AS invalid_product_relationships
FROM fact_sales AS f
LEFT JOIN date_keys AS d
    ON f.date_key = d.date_key
LEFT JOIN store_keys AS s
    ON f.store_key = s.store_key
LEFT JOIN product_keys AS p
    ON f.product_key = p.product_key;

WITH
date_keys AS (
    SELECT DISTINCT date_key
    FROM dim_date
),
store_keys AS (
    SELECT DISTINCT store_key
    FROM dim_store
),
product_keys AS (
    SELECT DISTINCT product_key
    FROM dim_product
)
SELECT
    'fact_inventory_snapshot' AS dataset,
    SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS invalid_date_relationships,
    SUM(CASE WHEN s.store_key IS NULL THEN 1 ELSE 0 END) AS invalid_store_relationships,
    SUM(CASE WHEN p.product_key IS NULL THEN 1 ELSE 0 END) AS invalid_product_relationships
FROM fact_inventory_snapshot AS f
LEFT JOIN date_keys AS d
    ON f.date_key = d.date_key
LEFT JOIN store_keys AS s
    ON f.store_key = s.store_key
LEFT JOIN product_keys AS p
    ON f.product_key = p.product_key;


-- =============================================================================
-- VALIDATION 5A — SALES MEASURE TOTALS
-- =============================================================================
-- The stored totals are compared with totals independently recalculated from
-- fact-level base columns using the authoritative business formulas:
--
-- gross_sales  = quantity * unit_price
-- net_sales    = gross_sales - discount_amount
-- gross_margin = net_sales - (quantity * unit_cost)
--
-- Expected result: each stored total equals its recalculated total.

SELECT
    SUM(gross_sales) AS stored_gross_sales,
    SUM(CAST(quantity * unit_price AS DECIMAL(18, 2))) AS recalculated_gross_sales,

    SUM(net_sales) AS stored_net_sales,
    SUM(
        CAST(
            (quantity * unit_price) - discount_amount
            AS DECIMAL(18, 2)
        )
    ) AS recalculated_net_sales,

    SUM(gross_margin) AS stored_gross_margin,
    SUM(
        CAST(
            ((quantity * unit_price) - discount_amount)
            - (quantity * unit_cost)
            AS DECIMAL(18, 2)
        )
    ) AS recalculated_gross_margin
FROM fact_sales;


-- =============================================================================
-- VALIDATION 5B — ROW-LEVEL SALES ARITHMETIC
-- =============================================================================
-- Aggregate totals can occasionally hide offsetting row errors. This second
-- check therefore validates each row's stored measures independently.
--
-- Expected result: all mismatch counts = 0.

SELECT
    SUM(
        CASE
            WHEN gross_sales IS NULL
                OR gross_sales
                   <> CAST(quantity * unit_price AS DECIMAL(18, 2))
            THEN 1
            ELSE 0
        END
    ) AS gross_sales_mismatch_rows,

    SUM(
        CASE
            WHEN net_sales IS NULL
                OR net_sales
                   <> CAST(
                        (quantity * unit_price) - discount_amount
                        AS DECIMAL(18, 2)
                   )
            THEN 1
            ELSE 0
        END
    ) AS net_sales_mismatch_rows,

    SUM(
        CASE
            WHEN gross_margin IS NULL
                OR gross_margin
                   <> CAST(
                        ((quantity * unit_price) - discount_amount)
                        - (quantity * unit_cost)
                        AS DECIMAL(18, 2)
                   )
            THEN 1
            ELSE 0
        END
    ) AS gross_margin_mismatch_rows
FROM fact_sales;


-- =============================================================================
-- VALIDATION 6 — INVENTORY LOW-STOCK DERIVATION
-- =============================================================================
-- This is a small additional consistency check because the warehouse already
-- persists is_low_stock. It must agree with the documented rule:
-- quantity_on_hand < reorder_level.
--
-- Expected result: low_stock_mismatch_rows = 0.

SELECT
    SUM(
        CASE
            WHEN is_low_stock <> (quantity_on_hand < reorder_level)
            THEN 1
            ELSE 0
        END
    ) AS low_stock_mismatch_rows
FROM fact_inventory_snapshot;
