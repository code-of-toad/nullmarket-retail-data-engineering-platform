-- =============================================================================
-- NullMarket — Phase 13 SQL Layer
-- File: sql/ddl/00_curated_views.sql
--
-- PURPOSE
--   Register the five curated Parquet datasets as TEMPORARY Spark SQL views.
--   Phase 13 is intentionally querying the persisted curated layer rather than
--   reusing PySpark DataFrames, so the SQL layer can independently inspect the
--   warehouse-shaped outputs.
--
-- IMPORTANT
--   These are session-scoped TEMPORARY views, not permanent warehouse tables.
--   Permanent BigQuery DDL belongs to Phase 16 and is deliberately not added
--   early.
--
-- RUN FROM
--   Repository root, after Phase 10 has produced data/curated/* Parquet.
-- =============================================================================

-- One row per calendar date.
CREATE OR REPLACE TEMPORARY VIEW dim_date
USING parquet
OPTIONS (
    path 'data/curated/dim_date'
);

-- One row per accepted product in the current warehouse version.
CREATE OR REPLACE TEMPORARY VIEW dim_product
USING parquet
OPTIONS (
    path 'data/curated/dim_product'
);

-- One row per accepted store in the current warehouse version.
CREATE OR REPLACE TEMPORARY VIEW dim_store
USING parquet
OPTIONS (
    path 'data/curated/dim_store'
);

-- Grain: one row per validated order line.
-- Business uniqueness: (order_id, line_number).
CREATE OR REPLACE TEMPORARY VIEW fact_sales
USING parquet
OPTIONS (
    path 'data/curated/fact_sales'
);

-- Grain: one row per product, per store, per snapshot date.
-- Business uniqueness: (date_key, store_key, product_key).
CREATE OR REPLACE TEMPORARY VIEW fact_inventory_snapshot
USING parquet
OPTIONS (
    path 'data/curated/fact_inventory_snapshot'
);

-- This final statement makes the initialized session easy to inspect.
SHOW TABLES;
