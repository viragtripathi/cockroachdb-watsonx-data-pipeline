-- Full cleanup for a fresh demo.
-- Run in the watsonx.data Query workspace using the Presto engine.
-- Run each section individually (watsonx.data may not support multi-statement).

-- ============================================================
-- EXPENSES (Banko demo)
-- ============================================================

DROP TABLE IF EXISTS iceberg_data.banko.expenses;
DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot;

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

-- ============================================================
-- TPC-C (workload demo)
-- ============================================================

DROP TABLE IF EXISTS iceberg_data.tpcc.warehouse;
DROP TABLE IF EXISTS iceberg_data.tpcc.district;
DROP TABLE IF EXISTS iceberg_data.tpcc.customer;
DROP TABLE IF EXISTS iceberg_data.tpcc."order";
DROP TABLE IF EXISTS iceberg_data.tpcc.order_line;
DROP TABLE IF EXISTS iceberg_data.tpcc.new_order;
DROP TABLE IF EXISTS iceberg_data.tpcc.item;
DROP TABLE IF EXISTS iceberg_data.tpcc.stock;
DROP TABLE IF EXISTS iceberg_data.tpcc.history;

-- Recreate TPC-C tables (run sql/setup-tpcc.sql after this)
-- Or drop the schema entirely:
-- DROP SCHEMA IF EXISTS iceberg_data.tpcc;

-- ============================================================
-- VERIFY
-- ============================================================

SELECT COUNT(*) AS row_count FROM iceberg_data.banko.expenses;
