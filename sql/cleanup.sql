-- Cleanup: drop and recreate the Iceberg table for a fresh demo.
-- Run in the watsonx.data Query workspace using the Presto engine.

-- 1. Drop the existing table (deletes all data and snapshots)
DROP TABLE IF EXISTS iceberg_data.banko.expenses;

-- 2. Recreate the table
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
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['shopping_type']
);

-- 3. Verify
SELECT COUNT(*) AS row_count FROM iceberg_data.banko.expenses;
