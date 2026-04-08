-- TPC-C Iceberg tables for watsonx.data
-- =======================================
-- Run in the watsonx.data Query workspace after initializing the
-- TPC-C workload in CockroachDB:
--   cockroach workload init tpcc 'postgresql://root@localhost:26257/tpcc?sslmode=disable' --warehouses=1
--
-- The CDC pipeline writes all columns as VARCHAR (generic mode).
-- cdc_table, cdc_operation, and cdc_timestamp are appended by the pipeline.

-- Create a namespace for TPC-C data
CREATE SCHEMA IF NOT EXISTS iceberg_data.tpcc
WITH (location = 's3a://<your-bucket-name>/tpcc');

-- ============================================================
-- TPC-C tables (all VARCHAR for generic CDC compatibility)
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.warehouse (
    w_id VARCHAR,
    w_name VARCHAR,
    w_street_1 VARCHAR,
    w_street_2 VARCHAR,
    w_city VARCHAR,
    w_state VARCHAR,
    w_zip VARCHAR,
    w_tax VARCHAR,
    w_ytd VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.district (
    d_id VARCHAR,
    d_w_id VARCHAR,
    d_name VARCHAR,
    d_street_1 VARCHAR,
    d_street_2 VARCHAR,
    d_city VARCHAR,
    d_state VARCHAR,
    d_zip VARCHAR,
    d_tax VARCHAR,
    d_ytd VARCHAR,
    d_next_o_id VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.customer (
    c_id VARCHAR,
    c_d_id VARCHAR,
    c_w_id VARCHAR,
    c_first VARCHAR,
    c_middle VARCHAR,
    c_last VARCHAR,
    c_street_1 VARCHAR,
    c_street_2 VARCHAR,
    c_city VARCHAR,
    c_state VARCHAR,
    c_zip VARCHAR,
    c_phone VARCHAR,
    c_since VARCHAR,
    c_credit VARCHAR,
    c_credit_lim VARCHAR,
    c_discount VARCHAR,
    c_balance VARCHAR,
    c_ytd_payment VARCHAR,
    c_payment_cnt VARCHAR,
    c_delivery_cnt VARCHAR,
    c_data VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc."order" (
    o_id VARCHAR,
    o_d_id VARCHAR,
    o_w_id VARCHAR,
    o_c_id VARCHAR,
    o_entry_d VARCHAR,
    o_carrier_id VARCHAR,
    o_ol_cnt VARCHAR,
    o_all_local VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.order_line (
    ol_o_id VARCHAR,
    ol_d_id VARCHAR,
    ol_w_id VARCHAR,
    ol_number VARCHAR,
    ol_i_id VARCHAR,
    ol_supply_w_id VARCHAR,
    ol_delivery_d VARCHAR,
    ol_quantity VARCHAR,
    ol_amount VARCHAR,
    ol_dist_info VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.new_order (
    no_o_id VARCHAR,
    no_d_id VARCHAR,
    no_w_id VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.item (
    i_id VARCHAR,
    i_im_id VARCHAR,
    i_name VARCHAR,
    i_price VARCHAR,
    i_data VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.stock (
    s_i_id VARCHAR,
    s_w_id VARCHAR,
    s_quantity VARCHAR,
    s_dist_01 VARCHAR,
    s_dist_02 VARCHAR,
    s_dist_03 VARCHAR,
    s_dist_04 VARCHAR,
    s_dist_05 VARCHAR,
    s_dist_06 VARCHAR,
    s_dist_07 VARCHAR,
    s_dist_08 VARCHAR,
    s_dist_09 VARCHAR,
    s_dist_10 VARCHAR,
    s_ytd VARCHAR,
    s_order_cnt VARCHAR,
    s_remote_cnt VARCHAR,
    s_data VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS iceberg_data.tpcc.history (
    rowid VARCHAR,
    h_c_id VARCHAR,
    h_c_d_id VARCHAR,
    h_c_w_id VARCHAR,
    h_d_id VARCHAR,
    h_w_id VARCHAR,
    h_date VARCHAR,
    h_amount VARCHAR,
    h_data VARCHAR,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (format = 'PARQUET');

-- Verify
SHOW TABLES IN iceberg_data.tpcc;
