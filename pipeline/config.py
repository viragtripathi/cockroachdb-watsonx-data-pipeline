"""Pipeline configuration."""

import os
from dataclasses import dataclass, field


def _envbool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes", "on")


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

    # IBM Cloud Object Storage (cloud watsonx.data)
    cos_endpoint: str = field(default_factory=lambda: os.getenv("COS_ENDPOINT", ""))
    cos_api_key: str = field(default_factory=lambda: os.getenv("COS_API_KEY", ""))
    cos_instance_id: str = field(default_factory=lambda: os.getenv("COS_INSTANCE_ID", ""))
    cos_bucket: str = field(default_factory=lambda: os.getenv("COS_BUCKET", "crdb-lakehouse"))
    cos_prefix: str = field(default_factory=lambda: os.getenv("COS_PREFIX", "cdc/expenses/"))

    # Generic S3 (MinIO for local watsonx.data Developer Edition, or any S3-compatible store)
    s3_endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", ""))
    s3_access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", ""))
    s3_secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", ""))
    s3_region: str = field(default_factory=lambda: os.getenv("S3_REGION", "us-east-1"))
    s3_bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET", "iceberg-bucket"))
    s3_prefix: str = field(default_factory=lambda: os.getenv("S3_PREFIX", "cdc/expenses/"))

    # watsonx.data
    wxd_url: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_URL", ""))
    wxd_instance_id: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_INSTANCE_ID", ""))
    wxd_api_key: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_API_KEY", ""))
    wxd_username: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_USERNAME", ""))
    wxd_password: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_PASSWORD", ""))
    wxd_verify_ssl: bool = field(default_factory=lambda: _envbool("WATSONX_DATA_VERIFY_SSL", True))
    wxd_catalog: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_CATALOG", "iceberg_data"))
    wxd_namespace: str = field(default_factory=lambda: os.getenv("WATSONX_DATA_NAMESPACE", "banko"))

    # Presto engine (for Iceberg table inserts)
    presto_engine_host: str = field(
        default_factory=lambda: os.getenv("PRESTO_ENGINE_HOST", "")
    )
    presto_port: int = field(default_factory=lambda: int(os.getenv("PRESTO_PORT", "443")))

    # CockroachDB source (for initial snapshot / changefeed setup)
    database_url: str = field(
        default_factory=lambda: os.getenv("DATABASE_URL", "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable")
    )

    @property
    def cos_configured(self) -> bool:
        return bool(self.cos_endpoint and self.cos_api_key and self.cos_instance_id)

    @property
    def s3_configured(self) -> bool:
        return bool(self.s3_endpoint and self.s3_access_key and self.s3_secret_key)

    @property
    def storage_configured(self) -> bool:
        return self.cos_configured or self.s3_configured

    @property
    def wxd_local_mode(self) -> bool:
        """True when local watsonx.data Developer Edition credentials are configured."""
        return bool(self.wxd_username and self.wxd_password)

    @property
    def wxd_cloud_mode(self) -> bool:
        """True when IBM Cloud watsonx.data credentials are configured."""
        return bool(self.wxd_api_key) and not self.wxd_local_mode

    @property
    def wxd_configured(self) -> bool:
        return bool(self.wxd_url and self.wxd_instance_id)

    @property
    def kafka_configured(self) -> bool:
        return bool(self.kafka_bootstrap_servers)
