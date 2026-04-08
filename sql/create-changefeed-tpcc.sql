-- Create webhook changefeed for TPC-C tables.
-- Run against the CockroachDB instance running the TPC-C workload.
--
-- Usage:
--   cockroach sql --insecure -d tpcc < sql/create-changefeed-tpcc.sql
--
-- The most interesting TPC-C tables for CDC are:
--   order, order_line, new_order (high insert/delete rate)
--   customer (balance updates on every payment)
--   district (YTD updates)
--   stock (quantity updates on every order)
--   history (append-only payment log)

SET CLUSTER SETTING kv.rangefeed.enabled = true;

-- CDC for the high-activity TPC-C tables
CREATE CHANGEFEED FOR "order", order_line, new_order, customer, district, stock, history
INTO 'webhook-https://localhost:5002/cdc/events?insecure_tls_skip_verify=true'
WITH updated, diff,
     resolved = '10s',
     min_checkpoint_frequency = '10s';
