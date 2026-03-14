-- =============================================================================
-- SECTION 20.2: REFRESHING MATERIALIZED VIEWS
-- =============================================================================
--
-- OBJECTIVE:
--   Understand how Snowflake automatically refreshes materialized views after
--   DML changes, and learn to monitor refresh history using system functions.
--
-- WHAT YOU WILL LEARN:
--   1. Materialized views are refreshed automatically (no manual REFRESH)
--   2. How to verify refresh state with SHOW MATERIALIZED VIEWS
--   3. How to monitor refresh history with information_schema
--   4. Automatic vs manual refresh model differences
--
-- HOW MATERIALIZED VIEW REFRESH WORKS (CRITICAL INTERVIEW TOPIC):
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │                     MV REFRESH LIFECYCLE                               │
-- │                                                                        │
-- │  1. DML on base table ──► Snowflake detects change                    │
-- │  2. Background service ──► Identifies affected micro-partitions       │
-- │  3. Incremental refresh ──► Only recomputes changed partitions        │
-- │  4. MV updated ──► is_current = 'Y'                                   │
-- │                                                                        │
-- │  NOTE: There is a brief delay (seconds to minutes) between the DML    │
-- │  and the MV reflecting the change. During this window, is_current='N' │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- REFRESH MODEL COMPARISON (INTERVIEW TOPIC):
-- ┌──────────────────────┬──────────────────────┬──────────────────────────┐
-- │ Feature              │ Snowflake MV         │ Other Platforms (e.g.    │
-- │                      │                      │ PostgreSQL, Oracle)      │
-- ├──────────────────────┼──────────────────────┼──────────────────────────┤
-- │ Refresh trigger      │ Automatic (DML)      │ Manual (REFRESH command) │
-- │ Refresh type         │ Incremental          │ Full or incremental      │
-- │ Compute used         │ Snowflake-managed    │ User's session/resources │
-- │ Manual refresh cmd   │ NOT supported        │ REFRESH MATERIALIZED VIEW│
-- │ Refresh scheduling   │ Not configurable     │ Configurable (pg_cron)   │
-- │ Staleness control    │ Automatic            │ User-managed             │
-- └──────────────────────┴──────────────────────┴──────────────────────────┘
--
-- KEY INTERVIEW CONCEPTS:
--   - Snowflake refreshes MVs automatically — no REFRESH command exists
--   - Refresh is INCREMENTAL: only changed micro-partitions are reprocessed
--   - Refresh uses Snowflake-managed compute (serverless), not your warehouse
--   - Brief delay between base table change and MV update (is_current='N')
--   - Use SHOW MATERIALIZED VIEWS to check is_current status
--   - Use information_schema.materialized_view_refresh_history() to audit
-- =============================================================================


-- =============================================
-- STEP 1: Disable caching for fair testing
-- =============================================
-- Same as Part 1: disable cache to observe true MV behavior
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE compute_wh SUSPEND;
ALTER WAREHOUSE compute_wh RESUME;


-- =============================================
-- STEP 2: Set up the base table
-- =============================================
CREATE OR REPLACE TRANSIENT DATABASE ORDERS;
CREATE OR REPLACE SCHEMA TPCH_SF100;

-- Copy a large dataset for testing
CREATE OR REPLACE TABLE TPCH_SF100.ORDERS AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

SELECT * FROM ORDERS LIMIT 100;


-- =============================================
-- STEP 3: Run the aggregation as a regular query
-- =============================================
-- This scans the full table every time it runs
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT)    AS MAX_COMMENT,
    MIN(O_COMMENT)    AS MIN_COMMENT,
    MAX(O_CLERK)      AS MAX_CLERK,
    MIN(O_CLERK)      AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);


-- =============================================
-- STEP 4: Create the materialized view
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

-- Verify MV exists and check is_current status
SHOW MATERIALIZED VIEWS;

-- Query the MV (reads precomputed results)
SELECT * FROM ORDERS_MV
ORDER BY YEAR;


-- =============================================
-- STEP 5: Modify base table and observe refresh
-- =============================================
-- Update a row — this triggers an automatic background refresh
UPDATE ORDERS
SET O_CLERK = 'Clerk#99900000'
WHERE O_ORDERDATE = '1992-01-01';


-- =============================================
-- STEP 6: Verify the automatic refresh
-- =============================================
-- Query the base table directly (shows updated data immediately)
SELECT
    YEAR(O_ORDERDATE) AS YEAR,
    MAX(O_COMMENT)    AS MAX_COMMENT,
    MIN(O_COMMENT)    AS MIN_COMMENT,
    MAX(O_CLERK)      AS MAX_CLERK,
    MIN(O_CLERK)      AS MIN_CLERK
FROM ORDERS.TPCH_SF100.ORDERS
GROUP BY YEAR(O_ORDERDATE)
ORDER BY YEAR(O_ORDERDATE);

-- Query the MV (may show updated data after a brief refresh delay)
SELECT * FROM ORDERS_MV
ORDER BY YEAR;

-- Check refresh state
SHOW MATERIALIZED VIEWS;
-- Look at the "is_current" column:
--   'Y' = MV is fully up to date with base table
--   'N' = MV is being refreshed (brief delay in progress)


-- =============================================
-- STEP 7: Monitor refresh history
-- =============================================
-- This function shows all refresh operations, timestamps, and credits used
SELECT *
FROM TABLE(information_schema.materialized_view_refresh_history());
-- Key columns to examine:
--   - MATERIALIZED_VIEW_NAME: which MV was refreshed
--   - START_TIME / END_TIME: when the refresh occurred
--   - CREDITS_USED: how many serverless credits the refresh consumed


-- =============================================================================
-- CONCEPT GAP: REFRESH BEHAVIOR DETAILS
-- =============================================================================
-- WHEN DOES REFRESH HAPPEN?
--   - After ANY DML on the base table (INSERT, UPDATE, DELETE, MERGE)
--   - After COPY INTO on the base table
--   - NOT after DDL changes (ALTER TABLE ADD COLUMN, etc.)
--
-- WHAT IF THE MV IS STALE (is_current = 'N')?
--   - Snowflake STILL uses the MV for queries (returns slightly stale data)
--   - If the MV is too far behind, Snowflake may fall back to base table scan
--   - You CANNOT force a refresh — you must wait for the background service
--
-- REFRESH GRANULARITY:
--   - Snowflake tracks which micro-partitions changed in the base table
--   - Only those partitions are recomputed in the MV
--   - This makes refresh much cheaper than a full recomputation
--
-- MULTIPLE MVs ON ONE BASE TABLE:
--   - Each MV is refreshed independently
--   - DML on the base table triggers refresh for ALL MVs referencing it
--   - More MVs = more serverless refresh credits consumed
-- =============================================================================


-- =============================================================================
-- CONCEPT GAP: REFRESH LATENCY AND CONSISTENCY
-- =============================================================================
-- ┌──────────────────────────────────────────────────────────────────────────┐
-- │ TIMELINE EXAMPLE:                                                       │
-- │                                                                         │
-- │  T=0s   INSERT INTO base_table ...    (DML executes)                   │
-- │  T=1s   Query MV → returns OLD data   (refresh not yet started)        │
-- │  T=5s   Background refresh starts     (Snowflake detects change)       │
-- │  T=15s  Refresh completes             (is_current = 'Y')              │
-- │  T=16s  Query MV → returns NEW data   (MV is now current)             │
-- │                                                                         │
-- │  NOTE: Actual latency varies by data volume and system load.           │
-- │  For critical real-time needs, query the base table directly.          │
-- └──────────────────────────────────────────────────────────────────────────┘
-- =============================================================================
