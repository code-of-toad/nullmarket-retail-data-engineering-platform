-- ============================================================================
-- NullMarket — Phase 17.4: Historical and Incremental Warehouse Validation
-- ============================================================================
-- PURPOSE
--   Prove that the Phase 17 incremental load:
--
--   1. preserved every pre-existing Phase 16 warehouse row exactly;
--   2. added the deterministic Phase 17 batch exactly once;
--   3. preserved declared grains, relationships, business calculations,
--      and BigQuery physical design.
--
-- BASELINE
--   Spark-curated Phase 16 Parquet:
--       gs://nullmarket-retail-de-data/curated/...
--
-- INCREMENTAL BATCH
--   Spark-curated Phase 17 Parquet:
--       gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/...
--
-- EXPECTATION
--   The final consolidated result should contain only PASS statuses.
--
-- BUSINESS RULES
--   gross_sales  = quantity * unit_price
--   net_sales    = gross_sales - discount_amount
--   gross_margin = net_sales - (quantity * unit_cost)
--   low stock    = quantity_on_hand < reorder_level
--
-- No order_status filter is introduced.
-- No SCD Type 2 behavior is introduced.
-- ============================================================================


-- ============================================================================
-- 1. CREATE TEMPORARY EXTERNAL TABLES OVER THE UNCHANGED PHASE 16 BASELINE
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_date`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/dim_date/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_product`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/dim_product/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_store`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/dim_store/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_fact_sales`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/fact_sales/part-*.parquet'
    ]
);

-- The baseline inventory Parquet uses Hive-style date_key partitions.
CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_inventory`
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


-- ============================================================================
-- 2. CREATE TEMPORARY EXTERNAL TABLES OVER THE PHASE 17 INCREMENTAL BATCH
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_batch_dim_date`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/dim_date/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_batch_fact_sales`
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/fact_sales/part-*.parquet'
    ]
);

CREATE OR REPLACE EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_batch_inventory`
WITH PARTITION COLUMNS (
    date_key INT64
)
OPTIONS (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/fact_inventory_snapshot/*'
    ],
    hive_partition_uri_prefix =
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/fact_inventory_snapshot',
    require_hive_partition_filter = false
);


-- ============================================================================
-- 3. CONSOLIDATED VALIDATION RESULT
-- ============================================================================
-- Historical boundaries:
--   Phase 16 sales history ends 2026-03-31.
--   Phase 16 inventory history ends 2026-03-26.
--
-- New Phase 17 dates:
--   sales     = 2026-04-01 through 2026-04-03
--   inventory = 2026-04-04
-- ============================================================================

SELECT
    check_name,
    actual_value,
    expected_value,
    IF(actual_value = expected_value, 'PASS', 'FAIL') AS status
FROM (

    -- ------------------------------------------------------------------------
    -- Final row counts after one or more retries of the identical batch.
    -- ------------------------------------------------------------------------
    SELECT
        'warehouse dim_date rows' AS check_name,
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
        ) AS STRING) AS actual_value,
        '94' AS expected_value

    UNION ALL

    SELECT
        'warehouse dim_product rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`
        ) AS STRING),
        '99'

    UNION ALL

    SELECT
        'warehouse dim_store rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`
        ) AS STRING),
        '12'

    UNION ALL

    SELECT
        'warehouse fact_sales rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
        ) AS STRING),
        '1515'

    UNION ALL

    SELECT
        'warehouse inventory rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
        ) AS STRING),
        '8328'


    -- ------------------------------------------------------------------------
    -- Historical row counts must still match the Phase 16 baseline exactly.
    -- ------------------------------------------------------------------------

    UNION ALL

    SELECT
        'historical dim_date rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
            WHERE full_date <= DATE '2026-03-31'
        ) AS STRING),
        '90'

    UNION ALL

    SELECT
        'historical fact_sales rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
            WHERE order_date <= DATE '2026-03-31'
        ) AS STRING),
        '1503'

    UNION ALL

    SELECT
        'historical inventory rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
            WHERE snapshot_date <= DATE '2026-03-26'
        ) AS STRING),
        '8316'


    -- ------------------------------------------------------------------------
    -- Exact historical reconciliation: baseline Parquet -> current BigQuery.
    --
    -- Each direction must return zero differences. This proves the incremental
    -- load did not alter or remove pre-existing history.
    -- ------------------------------------------------------------------------

    UNION ALL

    SELECT
        'historical dim_date missing/changed',
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    date_key,
                    full_date,
                    day_of_week,
                    day_name,
                    day_of_month,
                    week_of_year,
                    month_number,
                    month_name,
                    quarter_number,
                    year
                FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_date`

                EXCEPT DISTINCT

                SELECT
                    date_key,
                    full_date,
                    day_of_week,
                    day_name,
                    day_of_month,
                    week_of_year,
                    month_number,
                    month_name,
                    quarter_number,
                    year
                FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
                WHERE full_date <= DATE '2026-03-31'
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'historical dim_date unexpected/changed',
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    date_key,
                    full_date,
                    day_of_week,
                    day_name,
                    day_of_month,
                    week_of_year,
                    month_number,
                    month_name,
                    quarter_number,
                    year
                FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
                WHERE full_date <= DATE '2026-03-31'

                EXCEPT DISTINCT

                SELECT
                    date_key,
                    full_date,
                    day_of_week,
                    day_name,
                    day_of_month,
                    week_of_year,
                    month_number,
                    month_name,
                    quarter_number,
                    year
                FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_date`
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'dim_product changed from baseline',
        CAST((
            SELECT COUNT(*)
            FROM (
                (
                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_product`

                    EXCEPT DISTINCT

                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`
                )

                UNION ALL

                (
                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`

                    EXCEPT DISTINCT

                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_product`
                )
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'dim_store changed from baseline',
        CAST((
            SELECT COUNT(*)
            FROM (
                (
                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_store`

                    EXCEPT DISTINCT

                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`
                )

                UNION ALL

                (
                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`

                    EXCEPT DISTINCT

                    SELECT *
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_store`
                )
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'historical fact_sales missing/changed',
        CAST((
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
                FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_fact_sales`

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
                WHERE order_date <= DATE '2026-03-31'
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'historical fact_sales unexpected/changed',
        CAST((
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
                WHERE order_date <= DATE '2026-03-31'

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
                FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_fact_sales`
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'historical inventory missing/changed',
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    date_key,
                    store_key,
                    product_key,
                    quantity_on_hand,
                    reorder_level,
                    is_low_stock
                FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_inventory`

                EXCEPT DISTINCT

                SELECT
                    date_key,
                    store_key,
                    product_key,
                    quantity_on_hand,
                    reorder_level,
                    is_low_stock
                FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
                WHERE snapshot_date <= DATE '2026-03-26'
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'historical inventory unexpected/changed',
        CAST((
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
                WHERE snapshot_date <= DATE '2026-03-26'

                EXCEPT DISTINCT

                SELECT
                    date_key,
                    store_key,
                    product_key,
                    quantity_on_hand,
                    reorder_level,
                    is_low_stock
                FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_inventory`
            )
        ) AS STRING),
        '0'


    -- ------------------------------------------------------------------------
    -- Exact incremental reconciliation: Phase 17 Parquet -> current BigQuery.
    --
    -- These checks prove that the intended new rows were actually incorporated
    -- into the warehouse and that retrying did not multiply them.
    -- ------------------------------------------------------------------------

    UNION ALL

    SELECT
        'incremental dim_date rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
            WHERE full_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-04'
        ) AS STRING),
        '4'

    UNION ALL

    SELECT
        'incremental fact_sales rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
            WHERE order_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-03'
        ) AS STRING),
        '12'

    UNION ALL

    SELECT
        'incremental inventory rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
            WHERE snapshot_date = DATE '2026-04-04'
        ) AS STRING),
        '12'

    UNION ALL

    SELECT
        'incremental dim_date exact differences',
        CAST((
            SELECT COUNT(*)
            FROM (
                (
                    SELECT
                        date_key,
                        full_date,
                        day_of_week,
                        day_name,
                        day_of_month,
                        week_of_year,
                        month_number,
                        month_name,
                        quarter_number,
                        year
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_batch_dim_date`

                    EXCEPT DISTINCT

                    SELECT
                        date_key,
                        full_date,
                        day_of_week,
                        day_name,
                        day_of_month,
                        week_of_year,
                        month_number,
                        month_name,
                        quarter_number,
                        year
                    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
                    WHERE full_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-04'
                )

                UNION ALL

                (
                    SELECT
                        date_key,
                        full_date,
                        day_of_week,
                        day_name,
                        day_of_month,
                        week_of_year,
                        month_number,
                        month_name,
                        quarter_number,
                        year
                    FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`
                    WHERE full_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-04'

                    EXCEPT DISTINCT

                    SELECT
                        date_key,
                        full_date,
                        day_of_week,
                        day_name,
                        day_of_month,
                        week_of_year,
                        month_number,
                        month_name,
                        quarter_number,
                        year
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_batch_dim_date`
                )
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'incremental fact_sales exact differences',
        CAST((
            SELECT COUNT(*)
            FROM (
                (
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
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_batch_fact_sales`

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
                    WHERE order_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-03'
                )

                UNION ALL

                (
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
                    WHERE order_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-03'

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
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_batch_fact_sales`
                )
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'incremental inventory exact differences',
        CAST((
            SELECT COUNT(*)
            FROM (
                (
                    SELECT
                        date_key,
                        store_key,
                        product_key,
                        quantity_on_hand,
                        reorder_level,
                        is_low_stock
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_batch_inventory`

                    EXCEPT DISTINCT

                    SELECT
                        date_key,
                        store_key,
                        product_key,
                        quantity_on_hand,
                        reorder_level,
                        is_low_stock
                    FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
                    WHERE snapshot_date = DATE '2026-04-04'
                )

                UNION ALL

                (
                    SELECT
                        date_key,
                        store_key,
                        product_key,
                        quantity_on_hand,
                        reorder_level,
                        is_low_stock
                    FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
                    WHERE snapshot_date = DATE '2026-04-04'

                    EXCEPT DISTINCT

                    SELECT
                        date_key,
                        store_key,
                        product_key,
                        quantity_on_hand,
                        reorder_level,
                        is_low_stock
                    FROM `nullmarket-retail-de.nullmarket_warehouse._recon17_batch_inventory`
                )
            )
        ) AS STRING),
        '0'


    -- ------------------------------------------------------------------------
    -- Whole-warehouse grain, relationship, and business-rule checks.
    -- ------------------------------------------------------------------------

    UNION ALL

    SELECT
        'fact_sales duplicate business keys',
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    order_id,
                    line_number
                FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
                GROUP BY order_id, line_number
                HAVING COUNT(*) > 1
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'inventory duplicate business keys',
        CAST((
            SELECT COUNT(*)
            FROM (
                SELECT
                    date_key,
                    store_key,
                    product_key
                FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
                GROUP BY date_key, store_key, product_key
                HAVING COUNT(*) > 1
            )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'fact_sales orphan relationships',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales` AS f
            LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
                ON f.date_key = d.date_key
            LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_store` AS s
                ON f.store_key = s.store_key
            LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_product` AS p
                ON f.product_key = p.product_key
            WHERE
                d.date_key IS NULL
                OR s.store_key IS NULL
                OR p.product_key IS NULL
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'inventory orphan relationships',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot` AS f
            LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
                ON f.date_key = d.date_key
            LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_store` AS s
                ON f.store_key = s.store_key
            LEFT JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_product` AS p
                ON f.product_key = p.product_key
            WHERE
                d.date_key IS NULL
                OR s.store_key IS NULL
                OR p.product_key IS NULL
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'sales business-date mismatches',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales` AS f
            INNER JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
                ON f.date_key = d.date_key
            WHERE f.order_date != d.full_date
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'inventory business-date mismatches',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot` AS f
            INNER JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
                ON f.date_key = d.date_key
            WHERE f.snapshot_date != d.full_date
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'sales measure mismatch rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`
            WHERE
                gross_sales != CAST(quantity * unit_price AS NUMERIC)
                OR net_sales != CAST(
                    (quantity * unit_price) - discount_amount
                    AS NUMERIC
                )
                OR gross_margin != CAST(
                    ((quantity * unit_price) - discount_amount)
                    - (quantity * unit_cost)
                    AS NUMERIC
                )
        ) AS STRING),
        '0'

    UNION ALL

    SELECT
        'inventory low-stock mismatch rows',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`
            WHERE
                is_low_stock != (quantity_on_hand < reorder_level)
        ) AS STRING),
        '0'


    -- ------------------------------------------------------------------------
    -- Confirm the incremental MERGE did not replace the Phase 16 physical
    -- design of the final fact tables.
    -- ------------------------------------------------------------------------

    UNION ALL

    SELECT
        'fact_sales partition column preserved',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.INFORMATION_SCHEMA.COLUMNS`
            WHERE
                table_name = 'fact_sales'
                AND column_name = 'order_date'
                AND is_partitioning_column = 'YES'
        ) AS STRING),
        '1'

    UNION ALL

    SELECT
        'fact_sales clustering preserved',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.INFORMATION_SCHEMA.COLUMNS`
            WHERE
                table_name = 'fact_sales'
                AND (
                    (column_name = 'product_key' AND clustering_ordinal_position = 1)
                    OR
                    (column_name = 'store_key' AND clustering_ordinal_position = 2)
                )
        ) AS STRING),
        '2'

    UNION ALL

    SELECT
        'inventory partition column preserved',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.INFORMATION_SCHEMA.COLUMNS`
            WHERE
                table_name = 'fact_inventory_snapshot'
                AND column_name = 'snapshot_date'
                AND is_partitioning_column = 'YES'
        ) AS STRING),
        '1'

    UNION ALL

    SELECT
        'inventory clustering preserved',
        CAST((
            SELECT COUNT(*)
            FROM `nullmarket-retail-de.nullmarket_warehouse.INFORMATION_SCHEMA.COLUMNS`
            WHERE
                table_name = 'fact_inventory_snapshot'
                AND (
                    (column_name = 'store_key' AND clustering_ordinal_position = 1)
                    OR
                    (column_name = 'product_key' AND clustering_ordinal_position = 2)
                )
        ) AS STRING),
        '2'
)
ORDER BY check_name;


-- ============================================================================
-- 4. CLEAN UP TEMPORARY EXTERNAL TABLES
-- ============================================================================

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_date`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_product`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_dim_store`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_fact_sales`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_baseline_inventory`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_batch_dim_date`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_batch_fact_sales`;

DROP EXTERNAL TABLE
`nullmarket-retail-de.nullmarket_warehouse._recon17_batch_inventory`;
