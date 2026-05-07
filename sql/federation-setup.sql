-- CockroachDB Federation Setup for watsonx.data
-- ===============================================
--
-- Federation is the ON-RAMP to Iceberg, not the destination.
--
-- Querying CockroachDB live through Presto is no different from querying
-- it directly with any SQL client. The real value of federation is:
--   1. CTAS materialization -- pull OLTP data INTO Iceberg for analytics
--   2. Hybrid JOINs -- combine live data with Iceberg CDC history
--
-- Prerequisites:
--   1. CockroachDB Cloud cluster (Serverless or Dedicated) OR
--      self-hosted CockroachDB reachable from watsonx.data
--   2. watsonx.data instance with a Presto engine
--   3. CockroachDB CA cert uploaded when registering the data source
--
-- How to register in watsonx.data console:
--   1. Go to Infrastructure Manager > Add component > Add database
--   2. Select "PostgreSQL" as the database type
--   3. Fill in your CockroachDB connection details:
--        - Host: <your-cluster>.cockroachlabs.cloud (or self-hosted host)
--        - Port: 26257
--        - Database: defaultdb
--        - Username: <your-user>
--        - Password: <your-password>
--        - SSL: enabled (upload the CockroachDB CA cert)
--   4. Set the catalog name (e.g. cockroachdb)
--   5. Associate the catalog with your Presto engine
--   6. Restart the Presto engine (pause + resume)
--
-- After setup, verify with:
--   SHOW CATALOGS;  -- should list 'cockroachdb'
--   SHOW SCHEMAS IN cockroachdb;
--   SHOW TABLES IN cockroachdb.public;

-- ============================================================
-- VERIFY FEDERATION (Quick sanity check)
-- ============================================================

SELECT * FROM cockroachdb.public.expenses LIMIT 5;
SELECT COUNT(*) AS total_expenses FROM cockroachdb.public.expenses;

-- ============================================================
-- PATH A: CTAS MATERIALIZATION (The Primary Use Case)
-- ============================================================
-- Materialize live CockroachDB data into Iceberg tables for heavy
-- analytics. Once in Iceberg, data is columnar Parquet on COS --
-- optimized for analytical scans, decoupled from OLTP.
-- Run periodically (e.g. hourly/daily) to refresh the snapshot.

-- Create a point-in-time snapshot of all expenses.
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

-- To refresh the snapshot (drop and recreate, or use INSERT OVERWRITE):
-- DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot;
-- Then re-run the CREATE TABLE AS SELECT above.

-- Alternatively, append incremental snapshots with a timestamp:
-- INSERT INTO iceberg_data.banko.expenses_snapshot
-- SELECT *, CAST(CURRENT_TIMESTAMP AS VARCHAR) AS snapshot_timestamp
-- FROM cockroachdb.public.expenses;

-- Query the materialized snapshot (no load on CockroachDB)
SELECT shopping_type,
       SUM(expense_amount) AS total_spend,
       COUNT(*) AS txn_count
FROM iceberg_data.banko.expenses_snapshot
GROUP BY shopping_type
ORDER BY total_spend DESC;

-- Compare snapshots over time (after multiple materializations)
-- SELECT snapshot_timestamp, COUNT(*) AS rows, SUM(expense_amount) AS total
-- FROM iceberg_data.banko.expenses_snapshot
-- GROUP BY snapshot_timestamp
-- ORDER BY snapshot_timestamp DESC;

-- ============================================================
-- PATH B: HYBRID QUERIES (Live + CDC History)
-- ============================================================
-- The killer query: JOIN live CockroachDB data with CDC change
-- history stored in Iceberg. This requires both federation AND
-- the CDC pipeline to be running.

-- Join current state with full change history
SELECT
    live.expense_id,
    live.merchant,
    live.expense_amount AS current_amount,
    live.shopping_type,
    cdc.expense_amount AS historical_amount,
    cdc.cdc_operation,
    cdc.cdc_timestamp
FROM cockroachdb.public.expenses live
JOIN iceberg_data.banko.expenses cdc
  ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
ORDER BY cdc.cdc_timestamp DESC
LIMIT 20;

-- Detect expenses whose amount changed (update events in CDC)
SELECT
    live.expense_id,
    live.merchant,
    live.expense_amount AS current_amount,
    cdc.expense_amount AS previous_amount,
    (live.expense_amount - cdc.expense_amount) AS amount_change,
    cdc.cdc_timestamp AS changed_at
FROM cockroachdb.public.expenses live
JOIN iceberg_data.banko.expenses cdc
  ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
WHERE cdc.cdc_operation = 'update'
ORDER BY ABS(live.expense_amount - cdc.expense_amount) DESC
LIMIT 20;

-- Expenses that exist in CDC history but were deleted from OLTP
SELECT
    cdc.expense_id,
    cdc.merchant,
    cdc.expense_amount,
    cdc.cdc_operation,
    cdc.cdc_timestamp
FROM iceberg_data.banko.expenses cdc
LEFT JOIN cockroachdb.public.expenses live
  ON cdc.expense_id = CAST(live.expense_id AS VARCHAR)
WHERE live.expense_id IS NULL
  AND cdc.cdc_operation = 'delete'
ORDER BY cdc.cdc_timestamp DESC;

-- Full audit: how many times was each expense modified?
SELECT
    live.expense_id,
    live.merchant,
    live.expense_amount AS current_amount,
    COUNT(cdc.cdc_operation) AS total_changes,
    SUM(CASE WHEN cdc.cdc_operation = 'update' THEN 1 ELSE 0 END) AS updates,
    MIN(cdc.cdc_timestamp) AS first_seen,
    MAX(cdc.cdc_timestamp) AS last_changed
FROM cockroachdb.public.expenses live
JOIN iceberg_data.banko.expenses cdc
  ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
GROUP BY live.expense_id, live.merchant, live.expense_amount
HAVING COUNT(cdc.cdc_operation) > 1
ORDER BY total_changes DESC
LIMIT 20;

-- Cross-source analytics: live aggregates vs CDC aggregates
SELECT
    'Live (CockroachDB)' AS source,
    COUNT(*) AS total_rows,
    SUM(expense_amount) AS total_spend,
    COUNT(DISTINCT merchant) AS unique_merchants
FROM cockroachdb.public.expenses

UNION ALL

SELECT
    'CDC History (Iceberg)' AS source,
    COUNT(*) AS total_rows,
    SUM(expense_amount) AS total_spend,
    COUNT(DISTINCT merchant) AS unique_merchants
FROM iceberg_data.banko.expenses

UNION ALL

SELECT
    'Snapshot (Iceberg)' AS source,
    COUNT(*) AS total_rows,
    SUM(expense_amount) AS total_spend,
    COUNT(DISTINCT merchant) AS unique_merchants
FROM iceberg_data.banko.expenses_snapshot;
