-- watsonx.data Developer Edition: schema + table setup for the banko expenses demo.
--
-- Run this against the local DE Presto (https://localhost:8443) using basic auth:
--   curl -sk -u ibmlhadmin:password \
--        -H "X-Presto-User: ibmlhadmin" -H "Content-Type: text/plain" \
--        -X POST https://localhost:8443/v1/statement \
--        --data-binary @sql/watsonx-data-local-setup.sql
--
-- Or paste into the watsonx.data console at https://localhost:6443.
--
-- Storage location: the iceberg_data catalog is preconfigured against MinIO
-- bucket `iceberg-bucket`, so the schema location is s3a://iceberg-bucket/banko.

CREATE SCHEMA IF NOT EXISTS iceberg_data.banko
WITH (location = 's3a://iceberg-bucket/banko');

CREATE TABLE IF NOT EXISTS iceberg_data.banko.expenses (
    expense_id     VARCHAR,
    user_id        VARCHAR,
    description    VARCHAR,
    merchant       VARCHAR,
    expense_amount DOUBLE,
    expense_date   VARCHAR,
    shopping_type  VARCHAR,
    payment_method VARCHAR,
    recurring      BOOLEAN,
    cdc_table      VARCHAR,
    cdc_operation  VARCHAR,
    cdc_timestamp  VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['shopping_type']
);

-- Verify
SHOW TABLES IN iceberg_data.banko;
