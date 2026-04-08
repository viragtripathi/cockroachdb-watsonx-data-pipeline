-- watsonx.data Iceberg table setup and showcase queries
-- Run in the watsonx.data Query workspace using the Presto engine.
-- Replace <your-bucket-name> with your actual COS bucket name.
--
-- If SHOW CATALOGS does not list iceberg_data, restart the Presto
-- engine (pause + resume) and try again.

-- ============================================================
-- SETUP
-- ============================================================

-- 1. Create the banko namespace
CREATE SCHEMA IF NOT EXISTS iceberg_data.banko
WITH (location = 's3a://<your-bucket-name>/banko');

-- 2. Create the expenses table matching CDC Parquet schema
CREATE TABLE iceberg_data.banko.expenses (
    expense_id VARCHAR,
    user_id VARCHAR,
    description VARCHAR,
    merchant VARCHAR,
    expense_amount DOUBLE,
    expense_date VARCHAR,
    shopping_type VARCHAR,
    payment_method VARCHAR,
    recurring BOOLEAN,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['shopping_type']
);

-- 3. Verify
SHOW TABLES IN iceberg_data.banko;

-- ============================================================
-- CDC QUERIES
-- ============================================================

-- All CDC events (inserts, updates, deletes)
SELECT expense_id, merchant, expense_amount, cdc_operation, cdc_timestamp
FROM iceberg_data.banko.expenses
ORDER BY cdc_timestamp DESC;

-- CDC audit trail for a single expense (insert -> update -> delete)
SELECT expense_id, description, expense_amount, payment_method,
       cdc_operation, cdc_timestamp
FROM iceberg_data.banko.expenses
WHERE expense_id = 'demo-001'
ORDER BY cdc_timestamp;

-- CDC operation counts
SELECT cdc_operation, COUNT(*) AS event_count
FROM iceberg_data.banko.expenses
GROUP BY cdc_operation;

-- ============================================================
-- ANALYTICS QUERIES
-- ============================================================

-- Spending by category (exclude deleted records)
SELECT shopping_type,
       SUM(expense_amount) AS total,
       COUNT(*) AS txn_count
FROM iceberg_data.banko.expenses
WHERE cdc_operation != 'delete'
GROUP BY shopping_type
ORDER BY total DESC;

-- Current state: latest version of each expense
-- (deduplicates updates, excludes deletes)
SELECT e.*
FROM iceberg_data.banko.expenses e
INNER JOIN (
    SELECT expense_id, MAX(cdc_timestamp) AS latest
    FROM iceberg_data.banko.expenses
    GROUP BY expense_id
) latest ON e.expense_id = latest.expense_id
        AND e.cdc_timestamp = latest.latest
WHERE e.cdc_operation != 'delete';

-- Top merchants by spend
SELECT merchant,
       SUM(expense_amount) AS total_spend,
       COUNT(*) AS txn_count,
       AVG(expense_amount) AS avg_txn
FROM iceberg_data.banko.expenses
WHERE cdc_operation IN ('insert', 'update')
GROUP BY merchant
ORDER BY total_spend DESC;

-- Spending by user
SELECT user_id,
       SUM(expense_amount) AS total_spend,
       COUNT(*) AS txn_count
FROM iceberg_data.banko.expenses
WHERE cdc_operation != 'delete'
GROUP BY user_id
ORDER BY total_spend DESC;

-- Recurring vs one-time expenses
SELECT recurring,
       SUM(expense_amount) AS total,
       COUNT(*) AS txn_count
FROM iceberg_data.banko.expenses
WHERE cdc_operation != 'delete'
GROUP BY recurring;

-- ============================================================
-- FEDERATION: CockroachDB as PostgreSQL Data Source
-- ============================================================
-- After registering CockroachDB as a PostgreSQL datasource in
-- watsonx.data (see sql/federation-setup.sql for instructions),
-- you can query live OLTP data directly through Presto.

-- Verify federation catalog
-- SHOW CATALOGS;  -- should list 'cockroachdb'
-- SELECT * FROM cockroachdb.public.expenses LIMIT 5;

-- CTAS: Materialize live data into Iceberg for heavy analytics
-- CREATE TABLE iceberg_data.banko.expenses_snapshot
-- WITH (format = 'PARQUET', partitioning = ARRAY['shopping_type'])
-- AS SELECT *, CAST(CURRENT_TIMESTAMP AS VARCHAR) AS snapshot_timestamp
-- FROM cockroachdb.public.expenses;

-- Hybrid JOIN: live OLTP + CDC history in one query
-- SELECT live.expense_id, live.merchant,
--        live.expense_amount AS current_amount,
--        cdc.expense_amount AS historical_amount,
--        cdc.cdc_operation, cdc.cdc_timestamp
-- FROM cockroachdb.public.expenses live
-- JOIN iceberg_data.banko.expenses cdc
--   ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
-- ORDER BY cdc.cdc_timestamp DESC LIMIT 20;

-- See sql/federation-setup.sql and sql/demo-federation.sql for
-- complete federation queries, CTAS examples, and hybrid JOINs.

-- ============================================================
-- ICEBERG TABLE FEATURES
-- ============================================================

-- Time travel: view table snapshots
-- Each batch flush creates a new snapshot
SELECT * FROM iceberg_data.banko."expenses$snapshots"
ORDER BY committed_at DESC;

-- Query table as of a previous snapshot
-- Replace <snapshot-id> with an actual snapshot ID from the query above
-- SELECT * FROM iceberg_data.banko.expenses FOR VERSION AS OF <snapshot-id>;

-- Table history
SELECT * FROM iceberg_data.banko."expenses$history";

-- Partition statistics (partitioned by shopping_type)
SELECT * FROM iceberg_data.banko."expenses$partitions";

-- Manifest files (Iceberg metadata)
SELECT * FROM iceberg_data.banko."expenses$manifests";

-- Data files (underlying Parquet files)
SELECT * FROM iceberg_data.banko."expenses$files";
