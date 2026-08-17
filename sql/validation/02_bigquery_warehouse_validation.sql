-- ============================================================================
-- NullMarket — Phase 16: BigQuery Warehouse Validation
-- ============================================================================
-- PURPOSE
--   Validate the final canonical BigQuery warehouse and reconcile it directly
--   against the persisted Spark-curated Parquet layer in GCS.
--
-- EXPECTATION
--   All duplicate, null, orphan, date-mismatch, arithmetic-mismatch, and
--   reconciliation-difference checks should return 0.
--
-- AUTHORITATIVE BUSINESS RULES
--   gross_sales  = quantity * unit_price
--   net_sales    = gross_sales - discount_amount
--   gross_margin = net_sales - (quantity * unit_cost)
--   low stock    = quantity_on_hand < reorder_level
--
-- No order_status filter is introduced.
-- ============================================================================


-- ============================================================================
-- 1. ROW COUNTS
-- ============================================================================
-- Expected Phase 16 warehouse counts:
--   dim_date                 = 90
--   dim_product              = 99
--   dim_store                = 12
--   fact_sales               = 1503
--   fact_inventory_snapshot  = 8316
-- ============================================================================

SELECT 'dim_date' AS dataset, COUNT(*) AS row_count
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`

UNION ALL

SELECT 'dim_product', COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`

UNION ALL

SELECT 'dim_store', COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`

UNION ALL

SELECT 'fact_sales', COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`

UNION ALL

SELECT 'fact_inventory_snapshot', COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`

ORDER BY dataset;


-- ============================================================================
-- 2. DECLARED GRAIN / UNIQUENESS
-- ============================================================================
-- Expected: duplicate_key_groups = 0 for every check.
-- ============================================================================

SELECT
    'dim_date.date_key' AS key_check,
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT date_key
    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
    GROUP BY date_key
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'dim_date.full_date',
    COUNT(*)
FROM (
    SELECT full_date
    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
    GROUP BY full_date
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'dim_product.product_key',
    COUNT(*)
FROM (
    SELECT product_key
    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`
    GROUP BY product_key
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'dim_product.product_id',
    COUNT(*)
FROM (
    SELECT product_id
    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`
    GROUP BY product_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'dim_store.store_key',
    COUNT(*)
FROM (
    SELECT store_key
    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`
    GROUP BY store_key
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'dim_store.store_id',
    COUNT(*)
FROM (
    SELECT store_id
    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`
    GROUP BY store_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'fact_sales(order_id,line_number)',
    COUNT(*)
FROM (
    SELECT order_id, line_number
    FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
    GROUP BY order_id, line_number
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'fact_inventory_snapshot(date_key,store_key,product_key)',
    COUNT(*)
FROM (
    SELECT date_key, store_key, product_key
    FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
    GROUP BY date_key, store_key, product_key
    HAVING COUNT(*) > 1
)

ORDER BY key_check;


-- ============================================================================
-- 3. REQUIRED-VALUE NULL CHECKS
-- ============================================================================
-- Expected: all returned null counts = 0.
-- ============================================================================

SELECT
    'dim_date' AS dataset,
    COUNTIF(date_key IS NULL) AS null_date_key,
    COUNTIF(full_date IS NULL) AS null_full_date
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`;

SELECT
    'dim_product' AS dataset,
    COUNTIF(product_key IS NULL) AS null_product_key,
    COUNTIF(product_id IS NULL) AS null_product_id,
    COUNTIF(product_name IS NULL) AS null_product_name,
    COUNTIF(category IS NULL) AS null_category,
    COUNTIF(subcategory IS NULL) AS null_subcategory,
    COUNTIF(brand IS NULL) AS null_brand,
    COUNTIF(list_price IS NULL) AS null_list_price,
    COUNTIF(active_flag IS NULL) AS null_active_flag
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`;

SELECT
    'dim_store' AS dataset,
    COUNTIF(store_key IS NULL) AS null_store_key,
    COUNTIF(store_id IS NULL) AS null_store_id,
    COUNTIF(store_name IS NULL) AS null_store_name,
    COUNTIF(city IS NULL) AS null_city,
    COUNTIF(province IS NULL) AS null_province,
    COUNTIF(region IS NULL) AS null_region,
    COUNTIF(store_type IS NULL) AS null_store_type,
    COUNTIF(open_date IS NULL) AS null_open_date
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`;

SELECT
    'fact_sales' AS dataset,
    COUNTIF(order_date IS NULL) AS null_order_date,
    COUNTIF(date_key IS NULL) AS null_date_key,
    COUNTIF(store_key IS NULL) AS null_store_key,
    COUNTIF(product_key IS NULL) AS null_product_key,
    COUNTIF(order_id IS NULL) AS null_order_id,
    COUNTIF(line_number IS NULL) AS null_line_number,
    COUNTIF(quantity IS NULL) AS null_quantity,
    COUNTIF(unit_price IS NULL) AS null_unit_price,
    COUNTIF(discount_amount IS NULL) AS null_discount_amount,
    COUNTIF(unit_cost IS NULL) AS null_unit_cost,
    COUNTIF(gross_sales IS NULL) AS null_gross_sales,
    COUNTIF(net_sales IS NULL) AS null_net_sales,
    COUNTIF(gross_margin IS NULL) AS null_gross_margin
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`;

SELECT
    'fact_inventory_snapshot' AS dataset,
    COUNTIF(snapshot_date IS NULL) AS null_snapshot_date,
    COUNTIF(date_key IS NULL) AS null_date_key,
    COUNTIF(store_key IS NULL) AS null_store_key,
    COUNTIF(product_key IS NULL) AS null_product_key,
    COUNTIF(quantity_on_hand IS NULL) AS null_quantity_on_hand,
    COUNTIF(reorder_level IS NULL) AS null_reorder_level,
    COUNTIF(is_low_stock IS NULL) AS null_is_low_stock
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`;


-- ============================================================================
-- 4. FACT-TO-DIMENSION REFERENTIAL INTEGRITY
-- ============================================================================
-- Expected: all invalid relationship counts = 0.
-- ============================================================================

SELECT
    'fact_sales' AS dataset,
    COUNTIF(d.date_key IS NULL) AS invalid_date_relationships,
    COUNTIF(s.store_key IS NULL) AS invalid_store_relationships,
    COUNTIF(p.product_key IS NULL) AS invalid_product_relationships
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales` AS f
LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
    ON f.date_key = d.date_key
LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_store` AS s
    ON f.store_key = s.store_key
LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_product` AS p
    ON f.product_key = p.product_key;

SELECT
    'fact_inventory_snapshot' AS dataset,
    COUNTIF(d.date_key IS NULL) AS invalid_date_relationships,
    COUNTIF(s.store_key IS NULL) AS invalid_store_relationships,
    COUNTIF(p.product_key IS NULL) AS invalid_product_relationships
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot` AS f
LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
    ON f.date_key = d.date_key
LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_store` AS s
    ON f.store_key = s.store_key
LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_product` AS p
    ON f.product_key = p.product_key;


-- ============================================================================
-- 5. BUSINESS-DATE CONSISTENCY
-- ============================================================================
-- The BigQuery-only physical partition date must agree with dim_date.
-- Expected: both mismatch counts = 0.
-- ============================================================================

SELECT
    COUNTIF(
        f.order_date IS NULL
        OR f.order_date != d.full_date
    ) AS sales_date_mismatches
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales` AS f
INNER JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
    ON f.date_key = d.date_key;

SELECT
    COUNTIF(
        f.snapshot_date IS NULL
        OR f.snapshot_date != d.full_date
    ) AS inventory_date_mismatches
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot` AS f
INNER JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
    ON f.date_key = d.date_key;


-- ============================================================================
-- 6. SALES MEASURE VALIDATION
-- ============================================================================
-- First compare stored totals with independently recalculated totals.
-- Then validate every row so aggregate equality cannot hide offsetting errors.
-- ============================================================================

SELECT
    SUM(gross_sales) AS stored_gross_sales,
    SUM(CAST(quantity * unit_price AS NUMERIC))
        AS recalculated_gross_sales,

    SUM(net_sales) AS stored_net_sales,
    SUM(
        CAST(
            (quantity * unit_price) - discount_amount
            AS NUMERIC
        )
    ) AS recalculated_net_sales,

    SUM(gross_margin) AS stored_gross_margin,
    SUM(
        CAST(
            ((quantity * unit_price) - discount_amount)
            - (quantity * unit_cost)
            AS NUMERIC
        )
    ) AS recalculated_gross_margin
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`;

SELECT
    COUNTIF(
        gross_sales != CAST(quantity * unit_price AS NUMERIC)
    ) AS gross_sales_mismatch_rows,

    COUNTIF(
        net_sales != CAST(
            (quantity * unit_price) - discount_amount
            AS NUMERIC
        )
    ) AS net_sales_mismatch_rows,

    COUNTIF(
        gross_margin != CAST(
            ((quantity * unit_price) - discount_amount)
            - (quantity * unit_cost)
            AS NUMERIC
        )
    ) AS gross_margin_mismatch_rows
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`;


-- ============================================================================
-- 7. INVENTORY LOW-STOCK DERIVATION
-- ============================================================================
-- Expected: low_stock_mismatch_rows = 0.
-- ============================================================================

SELECT
    COUNTIF(
        is_low_stock != (quantity_on_hand < reorder_level)
    ) AS low_stock_mismatch_rows
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`;


-- ============================================================================
-- 8. PHYSICAL DESIGN METADATA
-- ============================================================================
-- Verify the canonical fact tables retain the intended partitioning and
-- clustering configuration.
-- ============================================================================

SELECT
    table_name,
    column_name,
    is_partitioning_column,
    clustering_ordinal_position
FROM `nullmarket-retail-de.nullmarket_warehouse.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN (
    'fact_sales',
    'fact_inventory_snapshot'
)
AND (
    is_partitioning_column = 'YES'
    OR clustering_ordinal_position IS NOT NULL
)
ORDER BY
    table_name,
    clustering_ordinal_position,
    column_name;


-- ============================================================================
-- 9. DIRECT SPARK-PARQUET -> BIGQUERY RECONCILIATION
-- ============================================================================
-- Temporary external tables let BigQuery query the actual Spark-curated
-- Parquet in GCS without loading another persisted warehouse copy.
-- They are dropped at the end of this section.
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_dim_date_ext`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/dim_date/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_dim_product_ext`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/dim_product/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_dim_store_ext`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/dim_store/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/fact_sales/part-*.parquet'
    ]
);

-- Inventory uses the Spark Hive-style date_key directory layout.
CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`
WITH PARTITION COLUMNS (
    date_key INT64
)
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/fact_inventory_snapshot/*'
    ],
    hive_partition_uri_prefix =
        'gs://nullmarket-retail-de-data/curated/fact_inventory_snapshot',
    require_hive_partition_filter = false
);


-- ---------------------------------------------------------------------------
-- 9A. Aggregate reconciliation
--
-- Every difference below must be 0.
-- ---------------------------------------------------------------------------

SELECT
    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
    ) -
    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_date_ext`
    ) AS dim_date_row_difference,

    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`
    ) -
    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_product_ext`
    ) AS dim_product_row_difference,

    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`
    ) -
    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_store_ext`
    ) AS dim_store_row_difference,

    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
    ) -
    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`
    ) AS fact_sales_row_difference,

    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
    ) -
    (
        SELECT COUNT(*)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`
    ) AS inventory_row_difference,

    (
        SELECT SUM(gross_sales)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
    ) -
    (
        SELECT SUM(gross_sales)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`
    ) AS gross_sales_difference,

    (
        SELECT SUM(net_sales)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
    ) -
    (
        SELECT SUM(net_sales)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`
    ) AS net_sales_difference,

    (
        SELECT SUM(gross_margin)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
    ) -
    (
        SELECT SUM(gross_margin)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`
    ) AS gross_margin_difference,

    (
        SELECT SUM(quantity_on_hand)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
    ) -
    (
        SELECT SUM(quantity_on_hand)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`
    ) AS quantity_on_hand_difference,

    (
        SELECT SUM(reorder_level)
        FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
    ) -
    (
        SELECT SUM(reorder_level)
        FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`
    ) AS reorder_level_difference;


-- ---------------------------------------------------------------------------
-- 9B. Exact bidirectional dataset reconciliation
--
-- EXCEPT DISTINCT is checked in both directions:
--   Spark -> BigQuery catches missing/changed warehouse rows.
--   BigQuery -> Spark catches unexpected/changed warehouse rows.
--
-- order_date and snapshot_date are intentionally excluded because they are
-- BigQuery-only physical partition columns derived from date_key.
--
-- Expected: every mismatch count = 0.
-- ---------------------------------------------------------------------------

SELECT
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_date_ext`

            EXCEPT DISTINCT

            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
        )
    )
    +
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`

            EXCEPT DISTINCT

            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_date_ext`
        )
    ) AS dim_date_mismatches,

    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_product_ext`

            EXCEPT DISTINCT

            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`
        )
    )
    +
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`

            EXCEPT DISTINCT

            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_product_ext`
        )
    ) AS dim_product_mismatches,

    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_store_ext`

            EXCEPT DISTINCT

            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`
        )
    )
    +
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`

            EXCEPT DISTINCT

            SELECT *
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_dim_store_ext`
        )
    ) AS dim_store_mismatches,

    (
        SELECT COUNT(*)
        FROM (
            SELECT
                date_key,
                store_key,
                product_key,
                order_id,
                line_number,
                quantity,
                unit_price,
                discount_amount,
                unit_cost,
                gross_sales,
                net_sales,
                gross_margin
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`

            EXCEPT DISTINCT

            SELECT
                date_key,
                store_key,
                product_key,
                order_id,
                line_number,
                quantity,
                unit_price,
                discount_amount,
                unit_cost,
                gross_sales,
                net_sales,
                gross_margin
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
        )
    )
    +
    (
        SELECT COUNT(*)
        FROM (
            SELECT
                date_key,
                store_key,
                product_key,
                order_id,
                line_number,
                quantity,
                unit_price,
                discount_amount,
                unit_cost,
                gross_sales,
                net_sales,
                gross_margin
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`

            EXCEPT DISTINCT

            SELECT
                date_key,
                store_key,
                product_key,
                order_id,
                line_number,
                quantity,
                unit_price,
                discount_amount,
                unit_cost,
                gross_sales,
                net_sales,
                gross_margin
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`
        )
    ) AS fact_sales_mismatches,

    (
        SELECT COUNT(*)
        FROM (
            SELECT
                date_key,
                store_key,
                product_key,
                quantity_on_hand,
                reorder_level,
                is_low_stock
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`

            EXCEPT DISTINCT

            SELECT
                date_key,
                store_key,
                product_key,
                quantity_on_hand,
                reorder_level,
                is_low_stock
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
        )
    )
    +
    (
        SELECT COUNT(*)
        FROM (
            SELECT
                date_key,
                store_key,
                product_key,
                quantity_on_hand,
                reorder_level,
                is_low_stock
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`

            EXCEPT DISTINCT

            SELECT
                date_key,
                store_key,
                product_key,
                quantity_on_hand,
                reorder_level,
                is_low_stock
            FROM `nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`
        )
    ) AS inventory_mismatches;


-- ============================================================================
-- 10. RECONCILIATION CLEANUP
-- ============================================================================
-- External tables were created only for the one-time validation workflow.
-- They should not remain in the final warehouse.
-- ============================================================================

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_dim_date_ext`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_dim_product_ext`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_dim_store_ext`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_fact_sales_ext`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse.recon_fact_inventory_snapshot_ext`;

-- Final cleanup assertion:
-- expected result = 0 rows.
SELECT
    table_name,
    table_type
FROM `nullmarket-retail-de.nullmarket_warehouse.INFORMATION_SCHEMA.TABLES`
WHERE STARTS_WITH(table_name, 'recon_')
ORDER BY table_name;
