-- =============================================================================
-- SECTION 20.1: CREATING AND USING MATERIALIZED VIEWS
-- =============================================================================
--
-- OBJECTIVE:
--   Learn how to create materialized views that physically store precomputed
--   results for faster query performance, and observe automatic refresh.
--
-- WHAT YOU WILL LEARN:
--   1. What materialized views are and how they differ from regular views
--   2. Creating materialized views with aggregations
--   3. Automatic refresh when base data changes
--   4. Disabling cache to test true performance gains
--
-- MATERIALIZED VIEW vs REGULAR VIEW (CRITICAL INTERVIEW TOPIC):
-- ┌──────────────────────┬──────────────────────┬──────────────────────────┐
-- │ Aspect               │ Regular View         │ Materialized View        │
-- ├──────────────────────┼──────────────────────┼──────────────────────────┤
-- │ Data storage         │ No (query definition)│ Yes (precomputed results)│
-- │ Query speed          │ Re-executes each time│ Reads stored results     │
-- │ Storage cost         │ None                 │ Yes (stores result set)  │
-- │ Refresh              │ N/A (always live)    │ Automatic (background)   │
-- │ Maintenance cost     │ None                 │ Compute credits for refresh│
-- │ Data freshness       │ Always current       │ Near real-time (slight lag)│
-- │ SQL restrictions     │ Minimal              │ Many (no UDFs, joins, etc)│
-- │ CREATE syntax        │ CREATE VIEW          │ CREATE MATERIALIZED VIEW │
-- └──────────────────────┴──────────────────────┴──────────────────────────┘
--
-- MATERIALIZED VIEW RESTRICTIONS:
--   Cannot contain: JOINs, subqueries, UDFs, HAVING, ORDER BY, LIMIT,
--                   window functions, nested views, non-deterministic functions
--   Can contain:    Aggregations (SUM, COUNT, MIN, MAX, AVG), GROUP BY,
--                   WHERE filters, deterministic scalar functions, UNION ALL
--
-- KEY INTERVIEW CONCEPTS:
--   - Materialized views store results physically (unlike regular views)
--   - Snowflake automatically refreshes them (no manual REFRESH command)
--   - Refresh is incremental (only changed micro-partitions are reprocessed)
--   - Best for: expensive aggregations on large tables with infrequent changes
--   - Avoid on: tables with very frequent DML (high refresh cost)
-- =============================================================================


-- =============================================
-- STEP 1: Disable caching for fair testing
-- =============================================
-- Disable result cache so we can see true query performance
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Suspend and resume warehouse to clear local disk cache
ALTER WAREHOUSE compute_wh SUSPEND;
ALTER WAREHOUSE compute_wh RESUME;


-- =============================================
-- STEP 2: Set up a large test table
-- =============================================
CREATE OR REPLACE TRANSIENT DATABASE ORDERS;
CREATE OR REPLACE SCHEMA TPCH_SF100;

-- Copy 150M+ rows from the sample data (this is a large table!)
CREATE OR REPLACE TABLE TPCH_SF100.ORDERS AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

SELECT * FROM ORDERS LIMIT 100;


-- =============================================
-- STEP 3: Run the aggregation query directly
-- =============================================
-- This query scans the full 150M+ row table every time
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT)    AS MAX_COMMENT,
    MIN(O_COMMENT)    AS MIN_COMMENT,
    MAX(O_CLERK)      AS MAX_CLERK,
    MIN(O_CLERK)      AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);
-- Note the execution time: this is the "without MV" baseline


-- =============================================
-- STEP 4: Create a materialized view
-- =============================================
CREATE OR REPLACE MATERIALIZED VIEW ORDERS_MV
AS
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT)    AS MAX_COMMENT,
    MIN(O_COMMENT)    AS MIN_COMMENT,
    MAX(O_CLERK)      AS MAX_CLERK,
    MIN(O_CLERK)      AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE);

-- Verify the materialized view exists
SHOW MATERIALIZED VIEWS;

-- Query the MV (much faster — reads precomputed results!)
SELECT * FROM ORDERS_MV
ORDER BY YEAR;
-- Compare execution time: should be significantly faster than Step 3


-- =============================================
-- STEP 5: Test automatic refresh after DML
-- =============================================
-- Update a row in the base table
UPDATE ORDERS
SET O_CLERK = 'Clerk#99900000'
WHERE O_ORDERDATE = '1992-01-01';

-- Query the base table directly (shows updated data immediately)
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_CLERK)      AS MAX_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

-- Query the MV (may show updated data after a brief refresh delay)
SELECT * FROM ORDERS_MV
ORDER BY YEAR;

-- Check the MV refresh state
SHOW MATERIALIZED VIEWS;
-- Look at the "is_current" column:
--   'Y' = MV is up to date with base table
--   'N' = MV is being refreshed (brief delay)


-- =============================================================================
-- CONCEPT GAP: WHEN TO USE MATERIALIZED VIEWS
-- =============================================================================
-- USE MATERIALIZED VIEWS WHEN:
--   ✓ Query involves expensive aggregations (SUM, COUNT, AVG over millions of rows)
--   ✓ Base table is large but changes infrequently
--   ✓ The same aggregation query is run frequently by many users
--   ✓ Query latency is critical (dashboards, reports)
--   ✓ Query involves common filter patterns on large tables
--
-- DO NOT USE MATERIALIZED VIEWS WHEN:
--   ✗ Base table has very frequent DML (constant inserts/updates)
--   ✗ The query is already fast enough without an MV
--   ✗ The query requires JOINs (not supported in MVs)
--   ✗ The query uses window functions, subqueries, or UDFs
--   ✗ Storage and refresh costs outweigh performance benefits
--
-- ALTERNATIVES TO MATERIALIZED VIEWS:
--   - Clustering keys: Improve scan efficiency on large tables
--   - Result caching: Free, automatic caching of identical queries
--   - Transient summary tables: Manual refresh via tasks + streams
--   - Regular views: No storage cost, always current
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: MATERIALIZED VIEWS vs SUMMARY TABLES
-- =============================================================================
-- ┌──────────────────────┬──────────────────────┬──────────────────────────┐
-- │ Aspect               │ Materialized View    │ Summary Table (manual)   │
-- ├──────────────────────┼──────────────────────┼──────────────────────────┤
-- │ Refresh              │ Automatic (Snowflake)│ Manual (task + stream)   │
-- │ Supports JOINs       │ No                   │ Yes                      │
-- │ Supports UDFs        │ No                   │ Yes                      │
-- │ Supports subqueries  │ No                   │ Yes                      │
-- │ Refresh cost         │ Serverless credits   │ Your warehouse credits   │
-- │ Setup complexity     │ Low (one SQL command) │ Higher (task+stream+MERGE)│
-- │ Flexibility          │ Limited by MV rules  │ Full SQL flexibility     │
-- └──────────────────────┴──────────────────────┴──────────────────────────┘
--
-- TIP: If your aggregation query needs JOINs, use a summary table
-- with tasks + streams for automated refresh instead of an MV.
-- =============================================================================
