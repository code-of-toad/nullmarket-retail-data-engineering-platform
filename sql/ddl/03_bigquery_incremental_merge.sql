-- ============================================================================
-- NullMarket — Phase 17: Idempotent Incremental BigQuery Load
-- ============================================================================
-- PURPOSE
--   Load the deterministic Phase 17 incremental Spark outputs into the existing
--   BigQuery warehouse without rebuilding historical fact tables.
--
-- BATCH
--   phase17_batch_01
--
-- SOURCE
--   gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01
--
-- TARGET
--   nullmarket-retail-de.nullmarket_warehouse
--
-- IDEMPOTENCY STRATEGY
--   - Stage the batch with LOAD DATA OVERWRITE.
--   - MERGE on the documented deterministic business/warehouse keys.
--   - Insert only when the target key does not already exist.
--   - Retrying the identical batch therefore leaves the warehouse unchanged.
--
-- IMPORTANT DESIGN BOUNDARY
--   This phase does NOT invent source-record correction semantics.
--   A matching key is treated as an already-loaded record rather than an
--   instruction to update history. General update-or-insert "upsert" behavior
--   is an important concept, but NullMarket's current requirements only require
--   safe ingestion of a subsequent new batch and duplicate prevention.
--
-- EXISTING PHYSICAL DESIGN
--   fact_sales:
--       PARTITION BY order_date
--       CLUSTER BY product_key, store_key
--
--   fact_inventory_snapshot:
--       PARTITION BY snapshot_date
--       CLUSTER BY store_key, product_key
--
-- No order_status filter is introduced.
-- No SCD Type 2 behavior is introduced.
-- ============================================================================


-- ============================================================================
-- 1. STAGE AND MERGE NEW DATE DIMENSION ROWS
-- ============================================================================

-- Spark produced four genuinely new calendar rows: 2026-04-01 through
-- 2026-04-04. The staging schema matches the curated Parquet exactly.
CREATE OR REPLACE TABLE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_dim_date`
(
    date_key        INT64,
    full_date       DATE,
    day_of_week     INT64,
    day_name        STRING,
    day_of_month    INT64,
    week_of_year    INT64,
    month_number    INT64,
    month_name      STRING,
    quarter_number  INT64,
    year            INT64
);

-- OVERWRITE is safe here because this is only a temporary batch-scoped staging
-- table. It also makes repeated execution deterministic.
LOAD DATA OVERWRITE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_dim_date`
FROM FILES (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/dim_date/part-*.parquet'
    ]
);

-- The source generator and Spark pipeline already validate date_key uniqueness,
-- but keep the warehouse boundary defensive as well.
ASSERT (
    SELECT COUNT(*) = COUNT(DISTINCT date_key)
    FROM `nullmarket-retail-de.nullmarket_warehouse._stg_phase17_dim_date`
) AS 'Phase 17 dim_date staging contains duplicate date_key values';

-- date_key is deterministic YYYYMMDD. Existing dates are left unchanged; only
-- genuinely missing dates are inserted.
MERGE `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS target
USING `nullmarket-retail-de.nullmarket_warehouse._stg_phase17_dim_date` AS source
ON target.date_key = source.date_key

WHEN NOT MATCHED THEN
    INSERT (
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
    )
    VALUES (
        source.date_key,
        source.full_date,
        source.day_of_week,
        source.day_name,
        source.day_of_month,
        source.week_of_year,
        source.month_number,
        source.month_name,
        source.quarter_number,
        source.year
    );


-- ============================================================================
-- 2. STAGE AND MERGE NEW SALES FACT ROWS
-- ============================================================================

-- This staging schema matches Spark-curated fact_sales. order_date is not
-- physically stored in Parquet; it is derived through dim_date exactly as in
-- the Phase 16 warehouse build.
CREATE OR REPLACE TABLE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_sales`
(
    date_key         INT64,
    store_key        INT64,
    product_key      INT64,
    order_id         STRING,
    line_number      INT64,
    quantity         INT64,
    unit_price       NUMERIC,
    discount_amount  NUMERIC,
    unit_cost        NUMERIC,
    gross_sales      NUMERIC,
    net_sales        NUMERIC,
    gross_margin     NUMERIC
);

LOAD DATA OVERWRITE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_sales`
FROM FILES (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/fact_sales/part-*.parquet'
    ]
);

-- fact_sales grain is one validated order line.
-- Business uniqueness = (order_id, line_number).
ASSERT NOT EXISTS (
    SELECT 1
    FROM `nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_sales`
    GROUP BY order_id, line_number
    HAVING COUNT(*) > 1
) AS 'Phase 17 fact_sales staging violates (order_id, line_number) grain';

-- Build the BigQuery-ready source rows by deriving the business partition date
-- from the newly extended conformed date dimension.
MERGE `nullmarket-retail-de.nullmarket_warehouse.fact_sales` AS target
USING (
    SELECT
        d.full_date AS order_date,
        f.date_key,
        f.store_key,
        f.product_key,
        f.order_id,
        f.line_number,
        f.quantity,
        f.unit_price,
        f.discount_amount,
        f.unit_cost,
        f.gross_sales,
        f.net_sales,
        f.gross_margin
    FROM `nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_sales` AS f
    INNER JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
        ON f.date_key = d.date_key
) AS source
ON target.order_id = source.order_id
AND target.line_number = source.line_number

-- Do not update a matching business key because the authoritative project
-- requirements do not define correction/update semantics for source records.
-- An identical retry therefore becomes a no-op for already-loaded lines.
WHEN NOT MATCHED THEN
    INSERT (
        order_date,
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
    )
    VALUES (
        source.order_date,
        source.date_key,
        source.store_key,
        source.product_key,
        source.order_id,
        source.line_number,
        source.quantity,
        source.unit_price,
        source.discount_amount,
        source.unit_cost,
        source.gross_sales,
        source.net_sales,
        source.gross_margin
    );


-- ============================================================================
-- 3. STAGE AND MERGE NEW INVENTORY SNAPSHOT FACT ROWS
-- ============================================================================

-- Spark physically partitions the incremental inventory Parquet by date_key:
--
-- fact_inventory_snapshot/
--     date_key=20260404/
--         part-....parquet
--
-- BigQuery therefore reconstructs date_key from the Hive-style path.
CREATE OR REPLACE TABLE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_inventory_snapshot`
(
    date_key          INT64,
    store_key         INT64,
    product_key       INT64,
    quantity_on_hand  INT64,
    reorder_level     INT64,
    is_low_stock      BOOL
);

LOAD DATA OVERWRITE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_inventory_snapshot`
FROM FILES (
    format = 'PARQUET',
    uris = [
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/fact_inventory_snapshot/*.parquet'
    ],
    hive_partition_uri_prefix =
        'gs://nullmarket-retail-de-data/curated/incremental/phase17_batch_01/fact_inventory_snapshot'
)
WITH PARTITION COLUMNS (
    date_key INT64
);

-- Inventory fact grain:
-- one row per date x store x product snapshot.
ASSERT NOT EXISTS (
    SELECT 1
    FROM `nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_inventory_snapshot`
    GROUP BY date_key, store_key, product_key
    HAVING COUNT(*) > 1
) AS 'Phase 17 inventory staging violates date/store/product grain';

MERGE
`nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot` AS target
USING (
    SELECT
        d.full_date AS snapshot_date,
        f.date_key,
        f.store_key,
        f.product_key,
        f.quantity_on_hand,
        f.reorder_level,
        f.is_low_stock
    FROM
        `nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_inventory_snapshot` AS f
    INNER JOIN `nullmarket-retail-de.nullmarket_warehouse.dim_date` AS d
        ON f.date_key = d.date_key
) AS source
ON target.date_key = source.date_key
AND target.store_key = source.store_key
AND target.product_key = source.product_key

WHEN NOT MATCHED THEN
    INSERT (
        snapshot_date,
        date_key,
        store_key,
        product_key,
        quantity_on_hand,
        reorder_level,
        is_low_stock
    )
    VALUES (
        source.snapshot_date,
        source.date_key,
        source.store_key,
        source.product_key,
        source.quantity_on_hand,
        source.reorder_level,
        source.is_low_stock
    );


-- ============================================================================
-- 4. CLEAN UP TEMPORARY STAGING TABLES
-- ============================================================================

DROP TABLE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_dim_date`;

DROP TABLE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_sales`;

DROP TABLE
`nullmarket-retail-de.nullmarket_warehouse._stg_phase17_fact_inventory_snapshot`;


-- ============================================================================
-- 5. POST-LOAD SUMMARY
-- ============================================================================
-- Expected after the FIRST successful incremental load:
--   dim_date                  94 rows  (90 baseline + 4 new)
--   dim_product               99 rows  (unchanged)
--   dim_store                 12 rows  (unchanged)
--   fact_sales              1515 rows  (1503 baseline + 12 new)
--   fact_inventory_snapshot 8328 rows  (8316 baseline + 12 new)
--
-- Running this complete script a SECOND time with the identical staged batch
-- should return the exact same counts because every batch business key already
-- exists in the target.
-- ============================================================================

SELECT
    'dim_date' AS table_name,
    COUNT(*) AS row_count
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_date`

UNION ALL

SELECT
    'dim_product',
    COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_product`

UNION ALL

SELECT
    'dim_store',
    COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.dim_store`

UNION ALL

SELECT
    'fact_sales',
    COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_sales`

UNION ALL

SELECT
    'fact_inventory_snapshot',
    COUNT(*)
FROM `nullmarket-retail-de.nullmarket_warehouse.fact_inventory_snapshot`

ORDER BY table_name;
