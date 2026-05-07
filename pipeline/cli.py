"""CLI for the CockroachDB to watsonx.data pipeline."""

import click

from .config import PipelineConfig
from .cos_writer import COSWriter, LocalWriter
from .processor import CDCProcessor
from .s3_writer import S3Writer


def create_processor(config: PipelineConfig) -> CDCProcessor:
    """Create a CDC processor with the appropriate sink."""
    if config.s3_configured:
        writer = S3Writer(config)
        print(f"Sink: S3-compatible ({config.s3_endpoint} / bucket={config.s3_bucket})")
    elif config.cos_configured:
        writer = COSWriter(config)
        print(f"Sink: IBM COS ({config.cos_bucket})")
    else:
        writer = LocalWriter()
        print(f"Sink: Local filesystem ({writer.output_dir})")
        print("  Set S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY (MinIO) "
              "or COS_ENDPOINT/COS_API_KEY/COS_INSTANCE_ID (IBM COS)")

    presto_writer = None
    presto_authed = config.wxd_api_key or (config.wxd_username and config.wxd_password)
    if config.presto_engine_host and presto_authed:
        from .presto_writer import PrestoWriter
        try:
            presto_writer = PrestoWriter(config)
            mode = "local DE (basic auth)" if config.wxd_local_mode else "cloud (IAM)"
            print(f"Presto: {config.presto_engine_host}:{config.presto_port} -- {mode}")
            presto_writer.verify_connection()
            print(f"Iceberg: {config.wxd_catalog}.{config.wxd_namespace}.expenses via Presto")
        except Exception as e:
            print(f"⚠️  Presto writer disabled: {e}")
            presto_writer = None
    else:
        print("  Set PRESTO_ENGINE_HOST + (WATSONX_DATA_API_KEY for cloud, "
              "or WATSONX_DATA_USERNAME/PASSWORD for local DE) for Iceberg table insert")

    return CDCProcessor(config, writer.write, presto_writer)


@click.group()
def cli():
    """CockroachDB CDC to watsonx.data Lakehouse Pipeline.

    Streams CockroachDB row changes to Apache Iceberg tables in
    watsonx.data via IBM Cloud Object Storage.

    Two CDC modes:
      webhook  -- CockroachDB webhook changefeed (zero infrastructure)
      kafka    -- Debezium CockroachDB connector via Kafka
    """
    pass


@cli.command()
@click.option("--port", default=None, type=int, help="Webhook receiver port (default: 5002)")
@click.option("--host", default=None, help="Webhook receiver host (default: 0.0.0.0)")
@click.option("--https", is_flag=True, default=None,
              help="Serve over HTTPS with an adhoc self-signed cert (required by CRDB webhook sinks). "
                   "Also enabled if CDC_WEBHOOK_HTTPS=true.")
def webhook(port, host, https):
    """Start the CDC webhook receiver.

    Receives events from CockroachDB webhook changefeed.
    """
    import os as _os

    from flask import Flask

    from .cdc_webhook import init_webhook
    from .dashboard_api import init_dashboard

    config = PipelineConfig()
    if port:
        config.webhook_port = port
    if host:
        config.webhook_host = host

    use_https = https or _os.getenv("CDC_WEBHOOK_HTTPS", "").lower() in ("1", "true", "yes")

    processor = create_processor(config)
    blueprint = init_webhook(processor)

    app = Flask(__name__)
    app.register_blueprint(blueprint)

    if processor.presto_writer:
        app.register_blueprint(init_dashboard(processor.presto_writer))

    scheme = "https" if use_https else "http"
    print(f"\nCDC Webhook receiver starting on {scheme}://{config.webhook_host}:{config.webhook_port}")
    print("  Endpoint: POST /cdc/events")
    print("  Stats:    GET  /cdc/stats")
    print(f"  Batch size: {config.batch_size}, timeout: {config.batch_timeout_seconds}s")
    print("\n  CockroachDB changefeed SQL:")
    print("  CREATE CHANGEFEED FOR expenses")
    print(f"  INTO 'webhook-{scheme}://localhost:{config.webhook_port}/cdc/events?insecure_tls_skip_verify=true'")
    print("  WITH updated, diff;\n")

    ssl_context = "adhoc" if use_https else None
    app.run(host=config.webhook_host, port=config.webhook_port, ssl_context=ssl_context)


@cli.command()
@click.option("--brokers", default=None, help="Kafka bootstrap servers")
@click.option("--topics", default=None, help="Comma-separated Kafka topics")
@click.option("--group", default=None, help="Kafka consumer group ID")
def kafka(brokers, topics, group):
    """Start the Kafka CDC consumer.

    Consumes Debezium CockroachDB connector events from Kafka.
    """
    config = PipelineConfig()
    if brokers:
        config.kafka_bootstrap_servers = brokers
    if topics:
        config.kafka_topics = topics.split(",")
    if group:
        config.kafka_group_id = group

    processor = create_processor(config)

    print("\nCDC Kafka consumer starting")
    print(f"  Brokers: {config.kafka_bootstrap_servers}")
    print(f"  Topics:  {config.kafka_topics}")
    print(f"  Group:   {config.kafka_group_id}")
    print(f"  Batch size: {config.batch_size}, timeout: {config.batch_timeout_seconds}s\n")

    from .cdc_kafka import run_kafka_consumer

    run_kafka_consumer(config, processor)


@cli.command()
@click.option("--cos", is_flag=True, help="List IBM COS objects")
@click.option("--s3", "s3_flag", is_flag=True, help="List S3/MinIO objects")
@click.option("--local", is_flag=True, help="List local output files")
def stats(cos, s3_flag, local):
    """Show pipeline output stats."""
    config = PipelineConfig()

    if s3_flag and config.s3_configured:
        writer = S3Writer(config)
        objects = writer.list_objects()
        print(f"S3 bucket '{config.s3_bucket}' @ {config.s3_endpoint} ({len(objects)} objects):")
        for obj in objects[:20]:
            print(f"  {obj}")
        if len(objects) > 20:
            print(f"  ... and {len(objects) - 20} more")
    elif cos and config.cos_configured:
        writer = COSWriter(config)
        objects = writer.list_objects()
        print(f"IBM COS bucket '{config.cos_bucket}' ({len(objects)} objects):")
        for obj in objects[:20]:
            print(f"  {obj}")
        if len(objects) > 20:
            print(f"  ... and {len(objects) - 20} more")
    elif local or not (cos or s3_flag):
        writer = LocalWriter()
        files = writer.list_objects()
        print(f"Local output '{writer.output_dir}' ({len(files)} files):")
        for f in files[:20]:
            print(f"  {f}")
        if len(files) > 20:
            print(f"  ... and {len(files) - 20} more")
    else:
        print("Storage not configured. Use --local, --s3, or --cos with the matching env vars.")


def main():
    cli()


if __name__ == "__main__":
    main()
