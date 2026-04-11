-- Create changefeed that writes Parquet directly to IBM COS (S3-compatible).
-- Eliminates the pipeline's Parquet conversion step -- CockroachDB writes native Parquet.
--
-- Prerequisites:
--   1. IBM COS bucket with HMAC credentials (Access Key + Secret Key)
--      Create at: COS instance > Service credentials > New credential (with HMAC)
--   2. COS S3 endpoint (e.g., s3.us-south.cloud-object-storage.appdomain.cloud)
--   3. kv.rangefeed.enabled = true
--
-- NOTE on VECTOR columns: CockroachDB's Parquet writer does not support PGVectorFamily.
-- If your table has a VECTOR column (e.g., embedding), use a CDC query (AS SELECT ...)
-- to exclude it.
--
-- NOTE on Iceberg: These are raw Parquet files on COS, NOT Iceberg tables.
-- To get Iceberg features (time travel, snapshots), load them into Iceberg via
-- Spark or Presto.

SET CLUSTER SETTING kv.rangefeed.enabled = true;

-- ============================================================
-- Expenses table (excludes embedding VECTOR column)
-- ============================================================
-- Replace: {COS_BUCKET}, {COS_ACCESS_KEY}, {COS_SECRET_KEY}, {COS_ENDPOINT}
CREATE CHANGEFEED
INTO 's3://{COS_BUCKET}/cdc-parquet/expenses/?AWS_ACCESS_KEY_ID={COS_ACCESS_KEY}&AWS_SECRET_ACCESS_KEY={COS_SECRET_KEY}&AWS_ENDPOINT=https://{COS_ENDPOINT}&AWS_REGION=us-south&partition_format=daily&file_size=256kB'
WITH format = parquet,
     updated,
     diff,
     resolved = '10s',
     min_checkpoint_frequency = '10s'
AS SELECT expense_id, user_id, expense_date, expense_amount, shopping_type,
          description, merchant, payment_method, recurring, tags, created_at
   FROM expenses;

-- ============================================================
-- TPC-C tables (no VECTOR columns, standard changefeed works)
-- ============================================================
-- CREATE CHANGEFEED FOR "order", order_line, new_order, customer, district, stock, history
-- INTO 's3://{COS_BUCKET}/cdc-parquet/tpcc/?AWS_ACCESS_KEY_ID={COS_ACCESS_KEY}&AWS_SECRET_ACCESS_KEY={COS_SECRET_KEY}&AWS_ENDPOINT=https://{COS_ENDPOINT}&AWS_REGION=us-south&partition_format=daily&file_size=256kB'
-- WITH format = parquet,
--      updated,
--      diff,
--      resolved = '10s',
--      min_checkpoint_frequency = '10s';

-- ============================================================
-- Verify
-- ============================================================
-- SHOW CHANGEFEED JOBS;
--
-- CockroachDB auto-adds these metadata columns to each Parquet file:
--   __crdb__event_type  : 'c' (create/insert), 'u' (update), 'd' (delete)
--   __crdb__updated     : CDC timestamp
--   __crdb__before      : Previous row state (JSON, when diff is enabled)
--
-- File path format: s3://{BUCKET}/cdc-parquet/expenses/{YYYY-MM-DD}/{timestamp}-{table}.parquet
-- Plus RESOLVED marker files for checkpoint tracking.
