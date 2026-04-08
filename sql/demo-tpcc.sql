-- TPC-C Analytics Queries for watsonx.data
-- ==========================================
-- Run in the watsonx.data Query workspace after the CDC pipeline
-- has captured TPC-C workload events.
--
-- Prerequisites:
--   1. TPC-C Iceberg tables created (sql/setup-tpcc.sql)
--   2. CDC pipeline running with TPC-C changefeed active
--   3. cockroach workload run tpcc has generated transactions

-- ============================================================
-- STEP 1: CDC Event Overview
-- ============================================================

-- Total CDC events across all TPC-C tables
SELECT cdc_table, cdc_operation, COUNT(*) AS event_count
FROM (
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc."order"
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.order_line
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.new_order
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.customer
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.district
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.warehouse
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.stock
    UNION ALL
    SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.history
)
GROUP BY cdc_table, cdc_operation
ORDER BY cdc_table, event_count DESC;

-- ============================================================
-- STEP 2: Order Analytics
-- ============================================================

-- Orders per district
SELECT o_d_id AS district,
       COUNT(*) AS total_orders
FROM iceberg_data.tpcc."order"
WHERE cdc_operation IN ('insert', 'snapshot')
GROUP BY o_d_id
ORDER BY total_orders DESC;

-- Order line revenue by district
SELECT ol_d_id AS district,
       COUNT(*) AS line_items,
       ROUND(SUM(CAST(ol_amount AS DOUBLE)), 2) AS total_revenue
FROM iceberg_data.tpcc.order_line
WHERE cdc_operation IN ('insert', 'snapshot')
GROUP BY ol_d_id
ORDER BY total_revenue DESC;

-- Average items per order
SELECT ROUND(AVG(CAST(o_ol_cnt AS DOUBLE)), 2) AS avg_items_per_order,
       COUNT(*) AS total_orders
FROM iceberg_data.tpcc."order"
WHERE cdc_operation IN ('insert', 'snapshot');

-- ============================================================
-- STEP 3: Customer Analytics
-- ============================================================

-- Customer balance distribution
SELECT
    CASE
        WHEN CAST(c_balance AS DOUBLE) < -100 THEN 'Owing > $100'
        WHEN CAST(c_balance AS DOUBLE) < 0 THEN 'Owing < $100'
        WHEN CAST(c_balance AS DOUBLE) = 0 THEN 'Zero Balance'
        ELSE 'Credit'
    END AS balance_bucket,
    COUNT(*) AS customer_count
FROM iceberg_data.tpcc.customer
WHERE cdc_operation IN ('insert', 'snapshot')
GROUP BY 1
ORDER BY customer_count DESC;

-- Top customers by payment count
SELECT c_id, c_first, c_last, c_d_id,
       c_payment_cnt, c_balance
FROM iceberg_data.tpcc.customer
WHERE cdc_operation IN ('insert', 'update', 'snapshot')
ORDER BY CAST(c_payment_cnt AS BIGINT) DESC
LIMIT 10;

-- Customer credit types
SELECT c_credit, COUNT(*) AS customer_count
FROM iceberg_data.tpcc.customer
WHERE cdc_operation IN ('insert', 'snapshot')
GROUP BY c_credit;

-- ============================================================
-- STEP 4: Stock & Inventory Analytics
-- ============================================================

-- Low stock items (quantity < 15)
SELECT s_i_id AS item_id, s_w_id AS warehouse,
       s_quantity AS quantity, s_order_cnt AS order_count
FROM iceberg_data.tpcc.stock
WHERE cdc_operation IN ('insert', 'update', 'snapshot')
  AND CAST(s_quantity AS BIGINT) < 15
ORDER BY CAST(s_quantity AS BIGINT)
LIMIT 20;

-- Stock turnover: items with highest order count
SELECT s_i_id AS item_id,
       s_order_cnt AS times_ordered,
       s_quantity AS current_qty,
       s_remote_cnt AS remote_orders
FROM iceberg_data.tpcc.stock
WHERE cdc_operation IN ('insert', 'update', 'snapshot')
ORDER BY CAST(s_order_cnt AS BIGINT) DESC
LIMIT 10;

-- ============================================================
-- STEP 5: CDC Change Tracking
-- ============================================================

-- Customer balance changes over time (update events)
SELECT c_id, c_balance, cdc_operation, cdc_timestamp
FROM iceberg_data.tpcc.customer
WHERE cdc_operation = 'update'
ORDER BY cdc_timestamp DESC
LIMIT 20;

-- District YTD (year-to-date) changes
SELECT d_id, d_name, d_ytd, cdc_operation, cdc_timestamp
FROM iceberg_data.tpcc.district
WHERE cdc_operation IN ('update', 'snapshot')
ORDER BY cdc_timestamp DESC;

-- New order lifecycle: insert then delete (delivery)
SELECT no_o_id, no_d_id, cdc_operation, cdc_timestamp
FROM iceberg_data.tpcc.new_order
ORDER BY no_o_id, cdc_timestamp
LIMIT 20;

-- ============================================================
-- STEP 6: Iceberg Features on TPC-C Data
-- ============================================================

-- Snapshots across TPC-C tables
SELECT * FROM iceberg_data.tpcc."order$snapshots"
ORDER BY committed_at DESC;

-- Partition stats (if partitioned)
-- SELECT * FROM iceberg_data.tpcc."order$partitions";

-- Time travel: view orders at a previous point in time
-- SELECT * FROM iceberg_data.tpcc."order"
-- FOR VERSION AS OF <snapshot-id>;

-- ============================================================
-- STEP 7: OLTP vs OLAP Comparison
-- ============================================================
-- This demonstrates why you need Iceberg for analytics.
-- The same aggregation query runs against:
--   (a) CockroachDB directly (OLTP -- impacts production)
--   (b) Iceberg (OLAP -- zero impact on production)

-- (a) OLTP: heavy aggregation hits CockroachDB directly
--     In production this competes with real transactions.
-- SELECT ol_d_id AS district,
--        COUNT(*) AS line_items,
--        SUM(ol_amount) AS revenue
-- FROM cockroachdb.tpcc.order_line
-- GROUP BY ol_d_id
-- ORDER BY revenue DESC;

-- (b) OLAP: same query on Iceberg -- decoupled from OLTP
SELECT ol_d_id AS district,
       COUNT(*) AS line_items,
       ROUND(SUM(CAST(ol_amount AS DOUBLE)), 2) AS revenue
FROM iceberg_data.tpcc.order_line
WHERE cdc_operation IN ('insert', 'snapshot')
GROUP BY ol_d_id
ORDER BY revenue DESC;

-- The Iceberg query scans columnar Parquet (fast aggregation)
-- while CockroachDB stays focused on serving transactions.

-- ============================================================
-- STEP 8: FEDERATION - Hybrid JOINs (Live OLTP + CDC History)
-- ============================================================
-- Requires cockroachdb catalog registered in watsonx.data
-- pointing to the CockroachDB instance running TPC-C.

-- Compare live vs CDC row counts across all TPC-C tables
-- SELECT 'Live (CockroachDB)' AS source, COUNT(*) AS rows
-- FROM cockroachdb.tpcc."order"
-- UNION ALL
-- SELECT 'CDC History (Iceberg)', COUNT(*)
-- FROM iceberg_data.tpcc."order";

-- JOIN live customers with CDC balance history
-- SELECT live.c_id, live.c_first, live.c_last,
--        CAST(live.c_balance AS DOUBLE) AS current_balance,
--        CAST(cdc.c_balance AS DOUBLE) AS historical_balance,
--        cdc.cdc_operation, cdc.cdc_timestamp
-- FROM cockroachdb.tpcc.customer live
-- JOIN iceberg_data.tpcc.customer cdc
--   ON CAST(live.c_id AS VARCHAR) = cdc.c_id
--   AND CAST(live.c_d_id AS VARCHAR) = cdc.c_d_id
--   AND CAST(live.c_w_id AS VARCHAR) = cdc.c_w_id
-- WHERE cdc.cdc_operation = 'update'
-- ORDER BY cdc.cdc_timestamp DESC
-- LIMIT 20;
