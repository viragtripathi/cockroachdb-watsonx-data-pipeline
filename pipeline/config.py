"""Pipeline configuration."""

import os
from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    """Configuration for the CDC-to-lakehouse pipeline."""

    # CDC mode: "webhook" or "kafka"
    cdc_mode: str = field(default_factory=lambda: os.getenv("CDC_MODE", "webhook"))

    # Webhook receiver
    webhook_host: str = field(default_factory=lambda: os.getenv("CDC_WEBHOOK_HOST", "0.0.0.0"))
    webhook_port: int = field(default_factory=lambda: int(os.getenv("CDC_WEBHOOK_PORT", "5002")))
    webhook_auth_token: str = field(default_factory=lambda: os.getenv("CDC_WEBHOOK_AUTH_TOKEN", ""))

    # Kafka consumer (Debezium)
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_group_id: str = field(default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "crdb-wxd-pipeline"))
    kafka_topics: list[str] = field(
        default_factory=lambda: os.getenv("KAFKA_CDC_TOPICS", "crdb.public.expenses").split(",")
    )
    kafka_auto_offset_reset: str = field(
        default_factory=lambda: os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    )

    # Batching
    batch_size: int = field(default_factory=lambda: int(os.getenv("CDC_BATCH_SIZE", "1000")))
    batch_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("CDC_BATCH_TIMEOUT_SECONDS", "60"))
    )

    # IBM Cloud Object Storage
    cos_endpoint: str = field(default_factory=lambda: os.getenv("COS_ENDPOINT", ""))
    cos_api_key: str = field(default_factory=lambda: os.getenv("COS_API_KEY", ""))
    cos_instance_id: str = field(default_factory=lambda: os.getenv("COS_INSTANCE_ID", ""))
    cos_bucket: str = field(default_factory=lambda: os.getenv("COS_BUCKET", "crdb-lakehouse"))
    cos_prefix: str = field(default_factory=lambda: os.getenv("COS_PREFIX", "cdc/expenses/"))

    # watsonx.data
    wxd_url: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_URL", ""))
    wxd_instance_id: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_INSTANCE_ID", ""))
    wxd_api_key: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_API_KEY", ""))
    wxd_catalog: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_CATALOG", "iceberg_data"))
    wxd_namespace: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_NAMESPACE", "banko"))

    # Presto engine (for Iceberg table inserts)
    presto_engine_host: str = field(
        default_factory=lambda: os.getenv("PRESTO_ENGINE_HOST", "")
    )

    # CockroachDB source (for initial snapshot / changefeed setup)
    database_url: str = field(
        default_factory=lambda: os.getenv("DATABASE_URL", "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable")
    )

    @property
    def cos_configured(self) -> bool:
        return bool(self.cos_endpoint and self.cos_api_key and self.cos_instance_id)

    @property
    def wxd_configured(self) -> bool:
        return bool(self.wxd_url and self.wxd_instance_id)

    @property
    def kafka_configured(self) -> bool:
        return bool(self.kafka_bootstrap_servers)
