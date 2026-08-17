-- =============================================================================
-- NullMarket — Phase 13 SQL Layer
-- File: sql/analytics/01_required_analytics.sql
--
-- PURPOSE
--   Implement the twelve roadmap-required analytical queries against the
--   warehouse-shaped curated Parquet layer.
--
-- IMPORTANT BUSINESS RULE
--   Revenue uses fact_sales exactly as modeled. No order_status filter is
--   applied because the authoritative NullMarket requirements do not define
--   revenue inclusion/exclusion by order_status.
--
-- ASSUMPTION
--   Run this file with sql/ddl/00_curated_views.sql as the Spark SQL
--   initialization file so the five temporary views already exist.
--
-- REQUIRED TECHNIQUE COVERAGE
--   INNER JOIN   -> Queries 2, 3, 4, 6, 9, 10
--   LEFT JOIN    -> Queries 1, 7, 12
--   GROUP BY     -> Queries 1-9, 11, 12 as appropriate
--   HAVING       -> Validation file: duplicate-grain detection
--   CASE         -> Queries 11 and 12
--   CTEs         -> Queries 1, 5, 6, 7, 10, 11, 12
--   subqueries   -> Query 8; validation duplicate checks
--   ROW_NUMBER   -> Queries 5 and 10
--   RANK         -> Queries 5 and 6
--   DENSE_RANK   -> Queries 5 and 6
--   SUM OVER     -> Queries 7 and 11
--   AVG OVER     -> Query 7
--   LAG          -> Query 12
--   LEAD         -> Query 12
-- =============================================================================


-- =============================================================================
-- QUERY 1 — DAILY REVENUE
-- OUTPUT GRAIN: one row per calendar date.
-- =============================================================================
-- Aggregate fact_sales first, then LEFT JOIN to the continuous date dimension.
-- This preserves dates with no sales and reports them as 0.00 instead of
-- dropping them from the calendar.

WITH daily_sales AS (
    SELECT
        date_key,
        SUM(net_sales) AS daily_net_sales
    FROM fact_sales
    GROUP BY date_key
)
SELECT
    d.full_date,
    COALESCE(
        s.daily_net_sales,
        CAST(0.00 AS DECIMAL(18, 2))
    ) AS daily_net_sales
FROM dim_date AS d
LEFT JOIN daily_sales AS s
    ON d.date_key = s.date_key
ORDER BY d.full_date;


-- =============================================================================
-- QUERY 2 — REVENUE BY STORE
-- OUTPUT GRAIN: one row per store.
-- =============================================================================
-- fact_sales -> dim_store is many-to-one, so the INNER JOIN preserves sales
-- fact grain before aggregation.

SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.province,
    SUM(f.net_sales) AS total_net_sales
FROM fact_sales AS f
INNER JOIN dim_store AS s
    ON f.store_key = s.store_key
GROUP BY
    s.store_id,
    s.store_name,
    s.city,
    s.province
ORDER BY
    total_net_sales DESC,
    s.store_id;


-- =============================================================================
-- QUERY 3 — REVENUE BY PRODUCT
-- OUTPUT GRAIN: one row per product with sales.
-- =============================================================================

SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.net_sales) AS total_net_sales
FROM fact_sales AS f
INNER JOIN dim_product AS p
    ON f.product_key = p.product_key
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY
    total_net_sales DESC,
    p.product_id;


-- =============================================================================
-- QUERY 4 — REVENUE BY CATEGORY
-- OUTPUT GRAIN: one row per product category represented in sales.
-- =============================================================================
-- Category is descriptive dimensional context; net_sales remains additive at
-- the order-line fact grain and can safely be summed after the many-to-one join.

SELECT
    p.category,
    SUM(f.net_sales) AS total_net_sales
FROM fact_sales AS f
INNER JOIN dim_product AS p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY
    total_net_sales DESC,
    p.category;


-- =============================================================================
-- QUERY 5 — TOP 10 PRODUCTS
-- OUTPUT GRAIN: one row per ranked product, limited to exactly 10 rows.
-- =============================================================================
-- Revenue is aggregated BEFORE ranking so the window functions operate at
-- product grain rather than order-line grain.
--
-- ROW_NUMBER gives a deterministic exact top 10 by using product_id as a
-- tie-breaker. RANK and DENSE_RANK are shown alongside it to demonstrate how
-- tied revenue values behave differently.

WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        SUM(f.net_sales) AS total_net_sales
    FROM fact_sales AS f
    INNER JOIN dim_product AS p
        ON f.product_key = p.product_key
    GROUP BY
        p.product_id,
        p.product_name
),
ranked_products AS (
    SELECT
        product_id,
        product_name,
        total_net_sales,

        ROW_NUMBER() OVER (
            ORDER BY total_net_sales DESC, product_id ASC
        ) AS row_number_position,

        RANK() OVER (
            ORDER BY total_net_sales DESC
        ) AS revenue_rank,

        DENSE_RANK() OVER (
            ORDER BY total_net_sales DESC
        ) AS dense_revenue_rank
    FROM product_revenue
)
SELECT
    product_id,
    product_name,
    total_net_sales,
    row_number_position,
    revenue_rank,
    dense_revenue_rank
FROM ranked_products
WHERE row_number_position <= 10
ORDER BY row_number_position;


-- =============================================================================
-- QUERY 6 — STORE RANKING WITHIN PROVINCE
-- OUTPUT GRAIN: one row per store.
-- =============================================================================
-- The PARTITION BY province clause restarts the ranking independently inside
-- each province. RANK leaves gaps after ties; DENSE_RANK does not.

WITH store_revenue AS (
    SELECT
        s.store_id,
        s.store_name,
        s.province,
        SUM(f.net_sales) AS total_net_sales
    FROM fact_sales AS f
    INNER JOIN dim_store AS s
        ON f.store_key = s.store_key
    GROUP BY
        s.store_id,
        s.store_name,
        s.province
)
SELECT
    store_id,
    store_name,
    province,
    total_net_sales,

    RANK() OVER (
        PARTITION BY province
        ORDER BY total_net_sales DESC
    ) AS province_revenue_rank,

    DENSE_RANK() OVER (
        PARTITION BY province
        ORDER BY total_net_sales DESC
    ) AS province_dense_revenue_rank
FROM store_revenue
ORDER BY
    province,
    province_revenue_rank,
    store_id;


-- =============================================================================
-- QUERY 7 — SEVEN-DAY ROLLING REVENUE
-- OUTPUT GRAIN: one row per calendar date.
-- =============================================================================
-- The continuous dim_date is essential. A 7-ROW frame is only a true
-- seven-calendar-day frame when zero-sales dates are retained.
--
-- SUM OVER gives rolling 7-day revenue.
-- AVG OVER is included as a useful companion metric and fulfills the required
-- AVG OVER technique without changing the authoritative revenue definition.

WITH daily_sales AS (
    SELECT
        date_key,
        SUM(net_sales) AS daily_net_sales
    FROM fact_sales
    GROUP BY date_key
),
calendar_sales AS (
    SELECT
        d.full_date,
        COALESCE(
            s.daily_net_sales,
            CAST(0.00 AS DECIMAL(18, 2))
        ) AS daily_net_sales
    FROM dim_date AS d
    LEFT JOIN daily_sales AS s
        ON d.date_key = s.date_key
)
SELECT
    full_date,
    daily_net_sales,

    SUM(daily_net_sales) OVER (
        ORDER BY full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_net_sales,

    AVG(daily_net_sales) OVER (
        ORDER BY full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_average_net_sales
FROM calendar_sales
ORDER BY full_date;


-- =============================================================================
-- QUERY 8 — AVERAGE ORDER VALUE
-- OUTPUT GRAIN: one summary row.
-- =============================================================================
-- Authoritative definition:
-- average_order_value = total net sales / number of distinct orders.
--
-- The subquery first restores ORDER grain by summing all lines for each order.
-- Averaging those order totals is mathematically equivalent to total net sales
-- divided by distinct order count, while making the grain transition explicit.

SELECT
    AVG(order_net_sales) AS average_order_value
FROM (
    SELECT
        order_id,
        SUM(net_sales) AS order_net_sales
    FROM fact_sales
    GROUP BY order_id
) AS order_totals;


-- =============================================================================
-- QUERY 9 — GROSS MARGIN BY CATEGORY
-- OUTPUT GRAIN: one row per product category represented in sales.
-- =============================================================================

SELECT
    p.category,
    SUM(f.gross_margin) AS total_gross_margin
FROM fact_sales AS f
INNER JOIN dim_product AS p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY
    total_gross_margin DESC,
    p.category;


-- =============================================================================
-- QUERY 10 — LOW-STOCK PRODUCTS
-- OUTPUT GRAIN: one row per latest store/product combination that is low stock.
-- =============================================================================
-- Inventory is snapshot state, so we must not sum snapshots across dates.
-- ROW_NUMBER selects the latest valid snapshot independently for each
-- store/product pair before the low-stock rule is applied.
--
-- Authoritative rule:
-- quantity_on_hand < reorder_level.

WITH ranked_inventory AS (
    SELECT
        i.*,
        ROW_NUMBER() OVER (
            PARTITION BY i.store_key, i.product_key
            ORDER BY i.date_key DESC
        ) AS snapshot_recency
    FROM fact_inventory_snapshot AS i
),
latest_inventory AS (
    SELECT
        date_key,
        store_key,
        product_key,
        quantity_on_hand,
        reorder_level
    FROM ranked_inventory
    WHERE snapshot_recency = 1
)
SELECT
    d.full_date AS snapshot_date,
    s.store_id,
    s.store_name,
    p.product_id,
    p.product_name,
    i.quantity_on_hand,
    i.reorder_level
FROM latest_inventory AS i
INNER JOIN dim_date AS d
    ON i.date_key = d.date_key
INNER JOIN dim_store AS s
    ON i.store_key = s.store_key
INNER JOIN dim_product AS p
    ON i.product_key = p.product_key
WHERE i.quantity_on_hand < i.reorder_level
ORDER BY
    s.store_id,
    p.product_id;


-- =============================================================================
-- QUERY 11 — REVENUE CONTRIBUTION PERCENTAGE
-- OUTPUT GRAIN: one row per product with sales.
-- =============================================================================
-- Revenue contribution is defined as a grouping's share of total net sales.
-- Here the grouping is product.
--
-- SUM(total_net_sales) OVER () calculates the grand total without collapsing
-- the product-level rows. CASE protects the percentage calculation if total
-- revenue were ever zero.

WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        SUM(f.net_sales) AS total_net_sales
    FROM fact_sales AS f
    INNER JOIN dim_product AS p
        ON f.product_key = p.product_key
    GROUP BY
        p.product_id,
        p.product_name
)
SELECT
    product_id,
    product_name,
    total_net_sales,
    CASE
        WHEN SUM(total_net_sales) OVER () = 0
        THEN CAST(0.00 AS DECIMAL(18, 2))
        ELSE ROUND(
            100.0 * total_net_sales
            / SUM(total_net_sales) OVER (),
            2
        )
    END AS revenue_contribution_pct
FROM product_revenue
ORDER BY
    revenue_contribution_pct DESC,
    product_id;


-- =============================================================================
-- QUERY 12 — SALES TREND COMPARISONS
-- OUTPUT GRAIN: one row per calendar date.
-- =============================================================================
-- LAG exposes the prior calendar day's revenue.
-- LEAD exposes the following calendar day's revenue.
-- CASE labels the day-over-day direction without changing the sales metric.

WITH daily_sales AS (
    SELECT
        date_key,
        SUM(net_sales) AS daily_net_sales
    FROM fact_sales
    GROUP BY date_key
),
calendar_sales AS (
    SELECT
        d.full_date,
        COALESCE(
            s.daily_net_sales,
            CAST(0.00 AS DECIMAL(18, 2))
        ) AS daily_net_sales
    FROM dim_date AS d
    LEFT JOIN daily_sales AS s
        ON d.date_key = s.date_key
),
trend_context AS (
    SELECT
        full_date,
        daily_net_sales,

        LAG(daily_net_sales, 1) OVER (
            ORDER BY full_date
        ) AS previous_day_net_sales,

        LEAD(daily_net_sales, 1) OVER (
            ORDER BY full_date
        ) AS next_day_net_sales
    FROM calendar_sales
)
SELECT
    full_date,
    daily_net_sales,
    previous_day_net_sales,
    next_day_net_sales,

    daily_net_sales - previous_day_net_sales
        AS day_over_day_change,

    next_day_net_sales - daily_net_sales
        AS next_day_change,

    CASE
        WHEN previous_day_net_sales IS NULL THEN 'NO_PRIOR_DAY'
        WHEN daily_net_sales > previous_day_net_sales THEN 'UP'
        WHEN daily_net_sales < previous_day_net_sales THEN 'DOWN'
        ELSE 'FLAT'
    END AS day_over_day_trend
FROM trend_context
ORDER BY full_date;
