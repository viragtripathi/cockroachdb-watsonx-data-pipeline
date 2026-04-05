"""
CockroachDB CDC to IBM watsonx.data Lakehouse Pipeline.

Streams CockroachDB row changes to Apache Iceberg tables in watsonx.data
via IBM Cloud Object Storage, supporting two CDC ingestion modes:

  - Webhook: CockroachDB changefeed -> webhook endpoint -> Parquet -> COS -> Iceberg
  - Kafka:   CockroachDB -> Debezium -> Kafka -> consumer -> Parquet -> COS -> Iceberg
"""
