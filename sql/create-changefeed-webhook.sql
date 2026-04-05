-- Create a webhook changefeed from CockroachDB to the pipeline receiver.
-- Run this AFTER the pipeline webhook receiver is started.
--
-- Usage:
--   cockroach sql --insecure < sql/create-changefeed-webhook.sql

CREATE CHANGEFEED FOR TABLE expenses
INTO 'webhook-http://pipeline-webhook:5002/cdc/events?insecure_tls_skip_verify=true'
WITH
    updated,
    diff,
    resolved = '30s',
    min_checkpoint_frequency = '10s';
