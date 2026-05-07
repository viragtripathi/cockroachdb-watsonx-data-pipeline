"""Generic S3-compatible object storage writer.

Used for MinIO (local watsonx.data Developer Edition) or any S3-compatible store.
For IBM Cloud Object Storage with IAM auth, use COSWriter instead.
"""

from .config import PipelineConfig


class S3Writer:
    """Writes Parquet files to any S3-compatible object store via boto3."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._client = None

    def _get_client(self):
        if self._client is None:
            import boto3
            from botocore.client import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=self.config.s3_endpoint,
                aws_access_key_id=self.config.s3_access_key,
                aws_secret_access_key=self.config.s3_secret_key,
                region_name=self.config.s3_region,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                ),
            )
        return self._client

    def write(self, data: bytes, object_key: str) -> None:
        client = self._get_client()
        client.put_object(
            Bucket=self.config.s3_bucket,
            Key=object_key,
            Body=data,
            ContentType="application/octet-stream",
        )

    def list_objects(self, prefix: str = "") -> list[str]:
        client = self._get_client()
        prefix = prefix or self.config.s3_prefix
        paginator = client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
