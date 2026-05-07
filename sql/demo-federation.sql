-- watsonx.data Demo Script: OLTP to OLAP
-- ========================================
-- Showcases the CockroachDB -> watsonx.data architecture:
--   1. Federation as the on-ramp (verify connection, explore data)
--   2. CTAS materialization (OLTP -> Iceberg for analytics)
--   3. CDC history in Iceberg (streaming change audit trail)
--   4. Hybrid JOINs (live + CDC + snapshot in one query)
--
-- Prerequisites:
--   1. CockroachDB registered as PostgreSQL datasource (catalog: cockroachdb)
--   2. CDC pipeline has run and populated iceberg_data.banko.expenses
--   3. Both catalogs associated with the Presto engine
--
-- Run each section in order. Copy/paste into the Query workspace.

-- ============================================================
-- STEP 1: Verify both data sources are accessible
-- ============================================================

SHOW CATALOGS;

-- Should show at least: cockroachdb, iceberg_data

SELECT 'CockroachDB (Live)' AS source, COUNT(*) AS rows
FROM cockroachdb.public.expenses

UNION ALL

SELECT 'Iceberg (CDC History)' AS source, COUNT(*) AS rows
FROM iceberg_data.banko.expenses;

-- ============================================================
-- STEP 2: FEDERATION ON-RAMP (Verify live connection)
-- ============================================================
-- Federation lets Presto reach into CockroachDB directly.
-- This is useful for exploration and building CTAS queries,
-- but NOT for heavy analytics (that would hit your OLTP).

-- Quick look at live data
SELECT shopping_type,
       COUNT(*) AS transactions,
       ROUND(SUM(expense_amount), 2) AS total_spend
FROM cockroachdb.public.expenses
GROUP BY shopping_type
ORDER BY total_spend DESC;

-- ============================================================
-- STEP 3: CDC HISTORY IN ICEBERG (Streaming audit trail)
-- ============================================================

-- Full change history (every insert, update, delete ever recorded)
SELECT cdc_operation,
       COUNT(*) AS event_count,
       ROUND(SUM(expense_amount), 2) AS total_amount
FROM iceberg_data.banko.expenses
GROUP BY cdc_operation
ORDER BY event_count DESC;

-- Audit trail: all changes for the most-modified expenses
SELECT expense_id,
       cdc_operation,
       expense_amount,
       description,
       cdc_timestamp
FROM iceberg_data.banko.expenses
WHERE expense_id IN (
    SELECT expense_id
    FROM iceberg_data.banko.expenses
    GROUP BY expense_id
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
ORDER BY expense_id, cdc_timestamp;

-- Time travel: view Iceberg snapshots
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg_data.banko."expenses$snapshots"
ORDER BY committed_at DESC;

-- ============================================================
-- STEP 4: CTAS MATERIALIZATION (OLTP -> Iceberg for Analytics)
-- ============================================================
-- This is the primary use case for federation: pull live data
-- INTO Iceberg where it becomes columnar Parquet, partitioned,
-- and decoupled from CockroachDB. Run heavy analytics here.

-- Drop previous snapshot if exists
DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot;

-- Materialize current CockroachDB state into Iceberg.
-- NOTE: cast expense_id and user_id to VARCHAR. CockroachDB's federated
-- columns come through as Presto UUID type, but the CDC table stores them
-- as VARCHAR. Casting at CTAS time means downstream JOINs don't need to
-- cast (Presto refuses '=' between varchar and uuid).
CREATE TABLE iceberg_data.banko.expenses_snapshot
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['shopping_type']
)
AS
SELECT CAST(expense_id AS VARCHAR) AS expense_id,
       CAST(user_id    AS VARCHAR) AS user_id,
       description,
       merchant,
       expense_amount,
       CAST(expense_date AS VARCHAR) AS expense_date,
       shopping_type,
       payment_method,
       recurring,
       CAST(CURRENT_TIMESTAMP AS VARCHAR) AS snapshot_timestamp
FROM cockroachdb.public.expenses;

-- Verify the snapshot
SELECT COUNT(*) AS snapshot_rows FROM iceberg_data.banko.expenses_snapshot;

-- Analytics on the snapshot (zero load on CockroachDB)
SELECT shopping_type,
       COUNT(*) AS transactions,
       ROUND(SUM(expense_amount), 2) AS total_spend
FROM iceberg_data.banko.expenses_snapshot
GROUP BY shopping_type
ORDER BY total_spend DESC;

-- ============================================================
-- STEP 5: HYBRID QUERIES - The Killer Demo
-- ============================================================

-- Compare all three sources side-by-side
SELECT
    'Live (CockroachDB)' AS source,
    COUNT(*) AS rows,
    ROUND(SUM(expense_amount), 2) AS total_spend,
    COUNT(DISTINCT merchant) AS merchants
FROM cockroachdb.public.expenses

UNION ALL

SELECT
    'CDC History (Iceberg)' AS source,
    COUNT(*) AS rows,
    ROUND(SUM(expense_amount), 2) AS total_spend,
    COUNT(DISTINCT merchant) AS merchants
FROM iceberg_data.banko.expenses

UNION ALL

SELECT
    'Snapshot (Iceberg)' AS source,
    COUNT(*) AS rows,
    ROUND(SUM(expense_amount), 2) AS total_spend,
    COUNT(DISTINCT merchant) AS merchants
FROM iceberg_data.banko.expenses_snapshot;

-- JOIN live data with CDC history: find expenses with amount changes
SELECT
    live.expense_id,
    live.merchant,
    live.expense_amount AS current_amount,
    cdc.expense_amount AS previous_amount,
    ROUND(live.expense_amount - cdc.expense_amount, 2) AS difference,
    cdc.cdc_operation,
    cdc.cdc_timestamp
FROM cockroachdb.public.expenses live
JOIN iceberg_data.banko.expenses cdc
  ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
WHERE cdc.cdc_operation = 'update'
ORDER BY ABS(live.expense_amount - cdc.expense_amount) DESC
LIMIT 10;

-- Ghost records: expenses deleted from OLTP but preserved in CDC history
SELECT
    cdc.expense_id,
    cdc.merchant,
    cdc.expense_amount,
    cdc.shopping_type,
    cdc.cdc_timestamp AS deleted_at
FROM iceberg_data.banko.expenses cdc
LEFT JOIN cockroachdb.public.expenses live
  ON cdc.expense_id = CAST(live.expense_id AS VARCHAR)
WHERE live.expense_id IS NULL
  AND cdc.cdc_operation = 'delete'
ORDER BY cdc.cdc_timestamp DESC;

-- Full lifecycle: join live + CDC + snapshot for complete picture
SELECT
    COALESCE(CAST(live.expense_id AS VARCHAR), cdc.expense_id) AS expense_id,
    CASE WHEN live.expense_id IS NOT NULL THEN 'Active' ELSE 'Deleted' END AS status,
    COALESCE(live.merchant, cdc.merchant) AS merchant,
    live.expense_amount AS live_amount,
    snap.expense_amount AS snapshot_amount,
    COUNT(cdc.cdc_operation) AS total_cdc_events,
    MIN(cdc.cdc_timestamp) AS first_seen,
    MAX(cdc.cdc_timestamp) AS last_changed
FROM iceberg_data.banko.expenses cdc
LEFT JOIN cockroachdb.public.expenses live
  ON cdc.expense_id = CAST(live.expense_id AS VARCHAR)
LEFT JOIN iceberg_data.banko.expenses_snapshot snap
  ON cdc.expense_id = CAST(snap.expense_id AS VARCHAR)
GROUP BY live.expense_id, cdc.expense_id, live.merchant, cdc.merchant,
         live.expense_amount, snap.expense_amount
ORDER BY total_cdc_events DESC
LIMIT 20;
