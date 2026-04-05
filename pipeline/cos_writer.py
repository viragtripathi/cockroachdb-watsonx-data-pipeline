"""IBM Cloud Object Storage writer for CDC Parquet files."""

import os

from .config import PipelineConfig


class COSWriter:
    """Writes Parquet files to IBM Cloud Object Storage."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._client = None

    def _get_client(self):
        if self._client is None:
            import ibm_boto3
            from ibm_botocore.client import Config

            self._client = ibm_boto3.client(
                "s3",
                ibm_api_key_id=self.config.cos_api_key,
                ibm_service_instance_id=self.config.cos_instance_id,
                config=Config(signature_version="oauth"),
                endpoint_url=self.config.cos_endpoint,
            )
        return self._client

    def write(self, data: bytes, object_key: str) -> None:
        """Write bytes to COS."""
        client = self._get_client()
        client.put_object(
            Bucket=self.config.cos_bucket,
            Key=object_key,
            Body=data,
            ContentType="application/octet-stream",
        )

    def list_objects(self, prefix: str = "") -> list[str]:
        """List objects in the bucket with the given prefix."""
        client = self._get_client()
        prefix = prefix or self.config.cos_prefix
        response = client.list_objects_v2(Bucket=self.config.cos_bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]


class LocalWriter:
    """Writes Parquet files to local filesystem (for testing without COS)."""

    def __init__(self, output_dir: str = ""):
        self.output_dir = output_dir or os.getenv("CDC_LOCAL_OUTPUT", "./cdc-output")
        os.makedirs(self.output_dir, exist_ok=True)

    def write(self, data: bytes, object_key: str) -> None:
        """Write bytes to local filesystem."""
        path = os.path.join(self.output_dir, object_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    def list_objects(self, prefix: str = "") -> list[str]:
        """List files under the output directory."""
        result = []
        for root, _dirs, files in os.walk(self.output_dir):
            for f in files:
                path = os.path.join(root, f)
                rel = os.path.relpath(path, self.output_dir)
                if not prefix or rel.startswith(prefix):
                    result.append(rel)
        return result
