# CockroachDB to watsonx.data Pipeline

Bridge CockroachDB (OLTP) to IBM watsonx.data (OLAP) -- stream every row change to Apache Iceberg tables for analytics, audit trails, and ML training without impacting production.

```
CockroachDB (OLTP)                                    watsonx.data (OLAP)
┌─────────────────────┐                                ┌──────────────────────────┐
│ Transactions        │   CDC Pipeline                 │ Iceberg Tables           │
│ Low latency         │ ────────────────────────────>  │ Columnar Parquet on COS  │
│ Source of truth     │  (every change captured)       │ Time travel / snapshots  │
│                     │                                │ Partitioned analytics    │
│                     │   Federation (on-ramp)         │                          │
│                     │ ·····························> │ CTAS materialization     │
│                     │  (batch snapshots to Iceberg)  │ (periodic refresh)       │
└─────────────────────┘                                └──────────────────────────┘
```

## Why This Architecture?

CockroachDB excels at OLTP -- fast transactions, strong consistency, multi-region resilience. But heavy analytical queries (aggregating millions of rows, trend analysis, ML training) compete with production traffic and degrade app performance.

This pipeline solves that by streaming every change to **Apache Iceberg tables** in watsonx.data, where:
- Data is stored as **columnar Parquet** (optimized for analytical scans)
- **Iceberg** provides time travel, snapshots, partition pruning, and schema evolution
- **Presto/Spark** handles heavy analytics without touching CockroachDB

## Three Data Access Patterns

| Pattern                      | How It Works                                                                                 | Best For                                                           | OLTP Impact               |
|------------------------------|----------------------------------------------------------------------------------------------|--------------------------------------------------------------------|---------------------------|
| **CDC to Iceberg** (primary) | Changefeed (webhook or Kafka/Debezium) streams row changes to Parquet on COS + Iceberg table | Streaming analytics, full change history, audit trail, compliance  | Zero load after capture   |
| **CTAS Materialization**     | `CREATE TABLE AS SELECT` from federated CockroachDB into Iceberg                             | Periodic snapshots, heavy batch analytics, ML training datasets    | One-time read per refresh |
| **Federation** (on-ramp)     | CockroachDB registered as PostgreSQL datasource; Presto queries live data                    | Building CTAS queries, data exploration, hybrid JOINs with Iceberg | Queries hit OLTP directly |

**Federation is the on-ramp, not the destination.** Querying CockroachDB live through Presto is no different from querying it directly -- the real value comes when data lands in Iceberg (via CDC or CTAS), where it's in columnar format, decoupled from OLTP, and queryable with Iceberg features like time travel.

**Use them together** for the production architecture:
- **CDC** continuously streams changes to Iceberg (audit trail, streaming analytics)
- **CTAS** periodically materializes snapshots into Iceberg (reporting, ML training)
- **Hybrid JOINs** combine live CockroachDB data with Iceberg history in a single query

## CDC Pipeline Paths

| Path        | How It Works                                           | Best For                         |
|-------------|--------------------------------------------------------|----------------------------------|
| **Webhook** | CockroachDB changefeed -> HTTP POST -> pipeline        | Quick demos, zero infrastructure |
| **Kafka**   | CockroachDB -> Debezium connector -> Kafka -> pipeline | Enterprise scale, existing Kafka |

Both CDC paths feed into the same processor that batches events, writes Parquet, uploads to COS, and inserts into the Iceberg table via Presto.

## Quick Start

### Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose (optional, for containerized setup)
- CockroachDB v25.4+ (included in docker-compose, run locally, or use CockroachDB Cloud)
- One of the following for the lakehouse:
  - **Local watsonx.data Developer Edition** running on `kind` (free, runs on your laptop) -- see [Local Developer Edition Setup](#local-watsonxdata-developer-edition-setup) below
  - **IBM Cloud watsonx.data** (Lite or Enterprise) + IBM Cloud Object Storage + an [IBM Cloud API key](https://cloud.ibm.com/iam/apikeys) -- see [IBM Cloud Setup Guide](#ibm-cloud-setup-guide) below

The pipeline auto-detects which target to use based on env vars: set `WATSONX_DATA_USERNAME`/`PASSWORD` for local DE, or `WATSONX_DATA_API_KEY` for IBM Cloud. Same code, same demo scripts, same SQL.

### Option 1: Webhook CDC (Zero Infrastructure)

```bash
# Start CockroachDB + webhook pipeline
docker compose up -d cockroachdb pipeline-webhook

# Wait for CockroachDB to be ready
docker compose exec cockroachdb cockroach sql --insecure -e "SELECT version();"

# Initialize the database
docker compose exec cockroachdb cockroach sql --insecure < sql/setup.sql

# Create the changefeed
docker compose exec cockroachdb cockroach sql --insecure < sql/create-changefeed-webhook.sql

# Watch pipeline logs
docker compose logs -f pipeline-webhook

# Check stats
curl http://localhost:5002/cdc/stats
```

### Option 2: Kafka + Debezium CDC

```bash
# Start everything including Kafka stack
docker compose --profile kafka up -d

# Wait for Kafka Connect to be ready
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do sleep 2; done

# Register the Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @sql/debezium-connector.json

# Watch pipeline logs
docker compose logs -f pipeline-kafka
```

The database is initialized automatically via `sql/setup.sql` (mounted as docker-entrypoint-initdb).

> **The [Debezium CockroachDB connector](https://github.com/debezium/debezium-connector-cockroachdb) is NOT bundled in the stock `quay.io/debezium/connect` image.** It targets Debezium `3.6.0-SNAPSHOT` and must be built from source and mounted into the Connect container as a plugin:
>
> ```bash
> # 1. Build the connector plugin (JDK 17+ and Maven 3.9.8+).
> #    The connector compiles to Java 17 bytecode (debezium PR #35), so the
> #    plugin loads in the stock Java-17 Debezium Connect image.
> git clone https://github.com/debezium/debezium-connector-cockroachdb
> cd debezium-connector-cockroachdb
> ./mvnw clean package -Passembly
> # produces target/plugin/ with the connector JARs
>
> # 2. Point the pipeline's connect service at that plugin dir.
> #    3.6.0.Final is not released yet -- until it is, pin DEBEZIUM_VERSION to a
> #    released Connect image tag (the Java-17 plugin loads fine in 3.5.x).
> export DEBEZIUM_PLUGIN_PATH=/abs/path/to/debezium-connector-cockroachdb/target/plugin
> export DEBEZIUM_VERSION=3.5.0.Final   # override until 3.6.0.Final ships
> docker compose --profile kafka up -d
> ```
>
> The `connect` service mounts `${DEBEZIUM_PLUGIN_PATH}` into `/kafka/connect` so Kafka Connect can load `io.debezium.connector.cockroachdb.CockroachDBConnector`. Without this, connector registration fails with "Failed to find any class that implements Connector".

The Debezium CockroachDB connector topic name follows the pattern `<topic.prefix>.<database>.<schema>.<table>`. With the default connector config (`"topic.prefix": "crdb"`), the topic is `crdb.defaultdb.public.expenses`.

### Option 3: Local Development (No Docker)

```bash
# Install
uv sync

# Set COS + watsonx.data credentials (see IBM Cloud Setup Guide below)
export COS_ENDPOINT="https://s3.us-south.cloud-object-storage.appdomain.cloud"
export COS_API_KEY="<your-cos-api-key>"
export COS_INSTANCE_ID="<your-cos-instance-crn>"
export COS_BUCKET="<your-bucket-name>"
export PRESTO_ENGINE_HOST="<your-presto-engine-host>"
export WATSONX_DATA_API_KEY="<your-ibm-cloud-api-key>"

# Start webhook receiver
uv run crdb-wxd-pipeline webhook

# In another terminal, send test events
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[{"after": {"expense_id": "abc-123", "user_id": "user-1", "description": "Coffee", "merchant": "Starbucks", "expense_amount": 5.50, "expense_date": "2025-03-01", "shopping_type": "Coffee", "payment_method": "Debit Card", "recurring": false}}]'

# Check stats
curl http://localhost:5002/cdc/stats

# View local output
uv run crdb-wxd-pipeline stats --local
```

For the Kafka path locally:

```bash
uv sync --extra kafka
uv run crdb-wxd-pipeline kafka --brokers localhost:29092 --topics crdb.defaultdb.public.expenses
```

## Configuration

All settings via environment variables:

### Core

| Variable                    | Description              | Default                                                        |
|-----------------------------|--------------------------|----------------------------------------------------------------|
| `CDC_MODE`                  | `webhook` or `kafka`     | `webhook`                                                      |
| `CDC_BATCH_SIZE`            | Events per Parquet file  | `1000`                                                         |
| `CDC_BATCH_TIMEOUT_SECONDS` | Max seconds before flush | `60`                                                           |
| `DATABASE_URL`              | CockroachDB connection   | `cockroachdb://root@localhost:26257/defaultdb?sslmode=disable` |

### Webhook

| Variable                 | Description             | Default   |
|--------------------------|-------------------------|-----------|
| `CDC_WEBHOOK_HOST`       | Bind address            | `0.0.0.0` |
| `CDC_WEBHOOK_PORT`       | Bind port               | `5002`    |
| `CDC_WEBHOOK_AUTH_TOKEN` | Bearer token (optional) | --        |

### Kafka

| Variable                  | Description            | Default                          |
|---------------------------|------------------------|----------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers          | `localhost:9092`                 |
| `KAFKA_CDC_TOPICS`        | Comma-separated topics | `crdb.defaultdb.public.expenses` |
| `KAFKA_GROUP_ID`          | Consumer group         | `crdb-wxd-pipeline`              |
| `KAFKA_AUTO_OFFSET_RESET` | Offset reset policy    | `earliest`                       |

### Object Storage -- IBM Cloud Object Storage (cloud watsonx.data)

| Variable          | Description                                | Default          |
|-------------------|--------------------------------------------|------------------|
| `COS_ENDPOINT`    | COS endpoint URL (must include `https://`) | --               |
| `COS_API_KEY`     | COS service credential API key             | --               |
| `COS_INSTANCE_ID` | COS service instance CRN                   | --               |
| `COS_BUCKET`      | Target bucket                              | `crdb-lakehouse` |
| `COS_PREFIX`      | Object key prefix                          | `cdc/expenses/`  |

### Object Storage -- S3-compatible (MinIO / local DE / any S3)

| Variable        | Description                                       | Default          |
|-----------------|---------------------------------------------------|------------------|
| `S3_ENDPOINT`   | S3 API endpoint (e.g. `http://localhost:9000`)    | --               |
| `S3_ACCESS_KEY` | Access key ID                                     | --               |
| `S3_SECRET_KEY` | Secret access key                                 | --               |
| `S3_REGION`     | AWS region (MinIO accepts any value)              | `us-east-1`      |
| `S3_BUCKET`     | Target bucket                                     | `iceberg-bucket` |
| `S3_PREFIX`     | Object key prefix                                 | `cdc/expenses/`  |

When neither set is configured, Parquet files are written to `./cdc-output/` (or `CDC_LOCAL_OUTPUT`). When both are set, S3 wins.

### watsonx.data / Presto (Iceberg Table Insert)

| Variable                  | Description                                                         | Default        |
|---------------------------|---------------------------------------------------------------------|----------------|
| `PRESTO_ENGINE_HOST`      | Presto hostname (cloud: from console; local DE: `localhost`)        | --             |
| `PRESTO_PORT`             | Presto HTTPS port (cloud: `443`; local DE: `8443`)                  | `443`          |
| `WATSONX_DATA_API_KEY`    | IBM Cloud platform API key (**cloud mode**)                         | --             |
| `WATSONX_DATA_USERNAME`   | Basic-auth username (**local DE mode**, e.g. `ibmlhadmin`)          | --             |
| `WATSONX_DATA_PASSWORD`   | Basic-auth password (**local DE mode**)                             | --             |
| `WATSONX_DATA_VERIFY_SSL` | Verify Presto TLS cert (set `false` for local DE self-signed)       | `true`         |
| `WATSONX_DATA_CATALOG`    | Iceberg catalog name in watsonx.data                                | `iceberg_data` |
| `WATSONX_DATA_NAMESPACE`  | Iceberg namespace (schema)                                          | `banko`        |

**Auth mode is auto-detected.** If `WATSONX_DATA_USERNAME`/`PASSWORD` are set, the pipeline uses HTTP Basic auth (local DE). Otherwise it uses an IAM bearer token (IBM Cloud) with `WATSONX_DATA_API_KEY`. When neither is set, the pipeline writes Parquet only and skips Iceberg INSERTs.

## Output Format

Parquet files are written with Hive-style partitioning:

```
cdc/expenses/
  year=2025/month=03/day=15/
    expenses_20250315T143022_000000.parquet
    expenses_20250315T144500_000001.parquet
  year=2025/month=03/day=16/
    expenses_20250316T091200_000002.parquet
```

Each file contains:

| Column           | Type    | Description                      |
|------------------|---------|----------------------------------|
| `expense_id`     | string  | UUID                             |
| `user_id`        | string  | UUID                             |
| `description`    | string  | Expense description              |
| `merchant`       | string  | Merchant name                    |
| `expense_amount` | float64 | Amount                           |
| `expense_date`   | string  | Date (YYYY-MM-DD)                |
| `shopping_type`  | string  | Category                         |
| `payment_method` | string  | Payment method                   |
| `recurring`      | bool    | Recurring flag                   |
| `cdc_table`      | string  | Source table name                |
| `cdc_operation`  | string  | insert, update, delete, snapshot |
| `cdc_timestamp`  | string  | CDC event timestamp              |

For non-expenses tables (e.g. TPC-C), all columns are stored as strings and the pipeline auto-detects the source table from the CDC event payload.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CockroachDB (OLTP)                           │
│                                                                     │
│  expenses table                                                     │
│                                                                     │
│  Path 1: PostgreSQL wire protocol ──> Federation (live queries)     │
│  Path 2: CREATE CHANGEFEED ──> Webhook ──> CDC Pipeline             │
│  Path 3: Debezium connector ──> Kafka ──> CDC Pipeline              │
│  Path 4: CREATE CHANGEFEED format=parquet ──> COS directly          │
└──────┬──────────────────┬──────────────────────┬────────────────────┘
       │                  │                      │
       │         ┌────────▼────────┐  ┌──────────▼──────────┐
       │         │ Webhook Receiver│  │   Kafka Consumer    │
       │         │ (Flask)         │  │  (confluent-kafka)  │
       │         └────────┬────────┘  └──────────┬──────────┘
       │                  │                      │
       │             ┌────▼──────────────────────▼────┐
       │             │        CDC Processor           │
       │             │  - Normalizes webhook/Debezium │
       │             │  - Batches events              │
       │             │  - Writes Snappy Parquet       │
       │             │  - Hive partitioning           │
       │             └──────┬────────────┬────────────┘
       │                    │            │
       │       ┌────────────▼──┐  ┌──────▼──────────────┐
       │       │  IBM COS      │  │  Presto REST API    │
       │       │  (Parquet     │  │  INSERT INTO        │
       │       │   archive)    │  │  Iceberg table      │
       │       └───────────────┘  └──────┬──────────────┘
       │                                 │
       │          ┌──────────────────────▼───────────────────┐
       │          │          watsonx.data (Presto)           │
       ▼          │                                          │
  ┌──────────┐    │  iceberg_data.banko.expenses      (CDC)  │
  │ cockroachdb   │  iceberg_data.banko.expenses_snapshot    │
  │ catalog  │───>│                  (CTAS)                  │
  │(federated│    │                                          │
  │ live)    │    │  Hybrid JOINs across all three tables    │
  └──────────┘    └──────────────────────────────────────────┘
```

## Local watsonx.data Developer Edition Setup

If you don't have an IBM Cloud account (or just want to develop offline), [watsonx.data Developer Edition](https://www.ibm.com/docs/en/watsonx/watsonxdata/2.0.x?topic=installing-watsonxdata-developer-edition) runs the full lakehouse stack on your laptop in a `kind` Kubernetes cluster -- Presto + Iceberg + MinIO + Hive metastore, all in one install. The pipeline supports it out of the box.

### Step 1: Confirm your DE install is running

After you complete the official Developer Edition installer, you should see these pods in the `wxd` namespace:

```bash
kubectl -n wxd get pods
# ibm-lh-presto-...     Running
# ibm-lh-minio-...      Running
# ibm-lh-mds-thrift-... Running
# lhconsole-ui-...      Running
# ...
```

The installer typically port-forwards the **console UI**, **MinIO UI** (port 9001), and **MDS Thrift** for you. The pipeline additionally needs **Presto** (port 8443) and **MinIO S3 API** (port 9000) reachable from your host.

### Step 2: Port-forward Presto and MinIO S3

```bash
./scripts/port-forward-wxd.sh
# Starts kubectl port-forwards for:
#   localhost:8443 -> ibm-lh-presto-svc:8443  (Presto REST API, basic auth)
#   localhost:9000 -> ibm-lh-minio-svc:9000   (MinIO S3 API)
```

The script is idempotent -- safe to re-run if a port-forward dies.

### Step 3: Source the local env file

```bash
source .env.local
```

This sets all the right values for the local stack:

| Variable                     | Value                  | Notes                                               |
|------------------------------|------------------------|-----------------------------------------------------|
| `S3_ENDPOINT`                | `http://localhost:9000` | MinIO S3 API (after port-forward)                  |
| `S3_ACCESS_KEY`              | `dummyvalue`           | DE default                                          |
| `S3_SECRET_KEY`              | `dummyvalue`           | DE default                                          |
| `S3_BUCKET`                  | `iceberg-bucket`       | Pre-created by DE installer                         |
| `PRESTO_ENGINE_HOST`         | `localhost`            | After port-forward                                  |
| `PRESTO_PORT`                | `8443`                 |                                                     |
| `WATSONX_DATA_USERNAME`      | `ibmlhadmin`           | DE default                                          |
| `WATSONX_DATA_PASSWORD`      | `password`             | DE default                                          |
| `WATSONX_DATA_VERIFY_SSL`    | `false`                | DE uses a self-signed cert                          |
| `WATSONX_DATA_CATALOG`       | `iceberg_data`         | Catalog auto-registered by DE                       |

### Step 4: Create the Iceberg schema and table

```bash
# Via Presto REST API
python3 - <<'EOF'
import requests, urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
AUTH=("ibmlhadmin","password"); URL="https://localhost:8443/v1/statement"
H={"X-Presto-User":"ibmlhadmin","Content-Type":"text/plain"}
def run(sql):
    r=requests.post(URL,headers=H,data=sql,auth=AUTH,verify=False,timeout=30); j=r.json()
    while j.get("nextUri"):
        time.sleep(0.3); j=requests.get(j["nextUri"],auth=AUTH,verify=False,timeout=30).json()
        if j.get("stats",{}).get("state") in ("FINISHED","FAILED"): break
    print(("✅" if j.get("stats",{}).get("state")=="FINISHED" else "❌"), sql[:60])
for stmt in open("sql/watsonx-data-local-setup.sql").read().split(";"):
    if stmt.strip(): run(stmt)
EOF
```

Or paste the contents of `sql/watsonx-data-local-setup.sql` into the watsonx.data console at `https://localhost:6443` (login: `ibmlhadmin` / `password`).

### Step 5: Run the pipeline

```bash
uv run crdb-wxd-pipeline webhook
```

You should see:

```
Sink: S3-compatible (http://localhost:9000 / bucket=iceberg-bucket)
Presto: localhost:8443 -- local DE (basic auth)
✅ Presto connection verified (query state: WAITING_FOR_PREREQUISITES)
Iceberg: iceberg_data.banko.expenses via Presto

CDC Webhook receiver starting on 0.0.0.0:5002
```

### Step 6: Send test events (or hook up a CockroachDB changefeed)

```bash
# Quick smoke test
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[{"after":{"expense_id":"test-1","user_id":"u1","description":"Coffee","merchant":"Starbucks","expense_amount":5.50,"expense_date":"2026-05-02","shopping_type":"Coffee","payment_method":"Debit Card","recurring":true}}]'

# Or run the full demo
uv run python scripts/demo.py
```

### Step 7: Verify

```bash
# Parquet files in MinIO
uv run crdb-wxd-pipeline stats --s3

# Rows in Iceberg
python3 -c "
import requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
r = requests.post('https://localhost:8443/v1/statement',
    headers={'X-Presto-User':'ibmlhadmin','Content-Type':'text/plain'},
    auth=('ibmlhadmin','password'), verify=False,
    data='SELECT cdc_operation, COUNT(*) FROM iceberg_data.banko.expenses GROUP BY cdc_operation')
print(r.json())
"
```

You can also browse the data in the watsonx.data console at `https://localhost:6443` (Query workspace > pick the `presto` engine > query `iceberg_data.banko.expenses`).

### Wiring the CockroachDB changefeed for the local pipeline

```bash
# In your local CockroachDB (the same one .env.local points at)
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" < sql/setup.sql
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" -e "
CREATE CHANGEFEED FOR TABLE expenses
INTO 'webhook-http://host.docker.internal:5002/cdc/events?insecure_tls_skip_verify=true'
WITH updated, diff, resolved = '30s';
"
# Substitute host.docker.internal -> localhost if CockroachDB is not in Docker
```

From here every demo script in `scripts/` and every SQL file in `sql/` works against the local stack with no changes -- they just read the same env vars.

---

## IBM Cloud Setup Guide

### Step 1: Create IBM Cloud Object Storage (COS)

1. Go to [IBM Cloud Catalog > Object Storage](https://cloud.ibm.com/objectstorage/create)
2. Choose **Standard** plan, name it (e.g. `crdb-lakehouse-cos`), and click **Create**
3. In your COS instance, click **Create bucket** > **Custom bucket**:
   - **Bucket name**: e.g. `crdb-lakehouse-virag` (must be globally unique)
   - **Resiliency**: Regional
   - **Location**: `us-south` (or your preferred region)
   - **Storage class**: Smart Tier
4. Go to **Service credentials** (left sidebar) > **New credential**:
   - **Name**: `pipeline-writer`
   - **Role**: Writer
   - **Include HMAC**: Yes
   - Click **Add**
5. Expand the new credential to view the JSON. Note these values:
   - `apikey` -- your `COS_API_KEY`
   - `resource_instance_id` -- your `COS_INSTANCE_ID`
6. Go to **Endpoints** (left sidebar) and note the **public** endpoint for your region:
   - e.g. `https://s3.us-south.cloud-object-storage.appdomain.cloud`

### Step 2: Test the Pipeline with COS

```bash
export COS_ENDPOINT="https://s3.us-south.cloud-object-storage.appdomain.cloud"
export COS_API_KEY="<your-cos-api-key>"
export COS_INSTANCE_ID="<your-cos-instance-crn>"
export COS_BUCKET="<your-bucket-name>"

# Start webhook receiver
uv run crdb-wxd-pipeline webhook

# In another terminal, send a test CDC event
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[{"after": {"expense_id": "test-123", "user_id": "user-1", "description": "Coffee at Starbucks", "merchant": "Starbucks", "expense_amount": 5.50, "expense_date": "2025-03-01", "shopping_type": "Coffee", "payment_method": "Debit Card", "recurring": false}}]'
```

After the batch timeout (default 60s), you should see a Parquet file in your COS bucket under `cdc/expenses/year=YYYY/month=MM/day=DD/`.

### Step 3: Provision watsonx.data

1. Go to [IBM Cloud Catalog > watsonx.data](https://cloud.ibm.com/watsonxdata) and choose a plan:
   - **Lite** -- free, 500 resource units, 30-day trial (good for demos)
   - **Enterprise** -- pay-as-you-go (production use)
2. Set **Region** to the same region as your COS bucket (e.g. `us-south`)
3. Name it (e.g. `banko-watsonx-data`) and click **Create**
4. Wait for provisioning (a few minutes), then click **Open watsonx.data console**

### Step 4: Connect COS Bucket to watsonx.data

In the watsonx.data console:

1. Go to **Infrastructure Manager** > **Add component** > **Add storage**
2. Select **IBM Cloud Object Storage** and fill in:
   - **Bucket**: your bucket name (e.g. `crdb-lakehouse-virag`)
   - **Endpoint**: your COS public endpoint
   - **API key**: your `COS_API_KEY`
   - **Service instance CRN**: your `COS_INSTANCE_ID`
   - **Catalog type**: Apache Iceberg
   - **Catalog name**: `iceberg_data`
3. Click **Register**
4. The catalog is automatically associated with your Presto engine. If not, associate it manually and **restart the Presto engine** (pause then resume).

### Step 5: Create the Iceberg Schema and Table

In the watsonx.data **Query workspace** (left nav), select your Presto engine and run these SQL statements (also available in `sql/watsonx-data-setup.sql`):

```sql
-- Create the banko namespace (schema)
-- Replace <your-bucket-name> with your actual bucket name
CREATE SCHEMA IF NOT EXISTS iceberg_data.banko
WITH (location = 's3a://<your-bucket-name>/banko');

-- Create the expenses Iceberg table
CREATE TABLE iceberg_data.banko.expenses (
    expense_id VARCHAR,
    user_id VARCHAR,
    description VARCHAR,
    merchant VARCHAR,
    expense_amount DOUBLE,
    expense_date VARCHAR,
    shopping_type VARCHAR,
    payment_method VARCHAR,
    recurring BOOLEAN,
    cdc_table VARCHAR,
    cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['shopping_type']
);

-- Verify
SHOW TABLES IN iceberg_data.banko;
```

If `SHOW CATALOGS` does not list `iceberg_data`, restart the Presto engine (pause + resume) and try again.

### Step 6: Get the Presto Engine Host

1. In the watsonx.data console, go to **Infrastructure Manager**
2. Click on your **Presto engine**
3. Click **Connection information** and find `engine_host` in the JSON, e.g.:
   ```
   a5c9e678-xxxx-xxxx-xxxx-xxxxxxxxxxxx.wxd.xxxxx.lakehouse.ibmappdomain.cloud
   ```

### Step 7: Create an IBM Cloud API Key

1. Go to [IBM Cloud IAM > API Keys](https://cloud.ibm.com/iam/apikeys)
2. Click **Create an IBM Cloud API key**
3. Name it `wxd-pipeline` and click **Create**
4. Copy the key value immediately (it cannot be retrieved later)

### Step 8: Run the Full Pipeline (COS + Iceberg)

```bash
# COS credentials
export COS_ENDPOINT="https://s3.us-south.cloud-object-storage.appdomain.cloud"
export COS_API_KEY="<your-cos-api-key>"
export COS_INSTANCE_ID="<your-cos-instance-crn>"
export COS_BUCKET="<your-bucket-name>"

# watsonx.data / Presto credentials
export PRESTO_ENGINE_HOST="<your-presto-engine-host>"
export WATSONX_DATA_API_KEY="<your-ibm-cloud-api-key>"
export WATSONX_DATA_CATALOG="iceberg_data"
export WATSONX_DATA_NAMESPACE="banko"

# Optional: reduce batch timeout for testing
export CDC_BATCH_TIMEOUT_SECONDS=10

# Start the pipeline
uv run crdb-wxd-pipeline webhook
```

On startup you should see:
```
Sink: IBM COS (crdb-lakehouse-virag)
🔑 IAM token acquired (expires in 3600s)
✅ Presto connection verified (query state: WAITING_FOR_PREREQUISITES)
Iceberg: iceberg_data.banko.expenses via Presto
```

Send test events:
```bash
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[
    {"after": {"expense_id": "e2e-001", "user_id": "user-1", "description": "Coffee at Starbucks", "merchant": "Starbucks", "expense_amount": 5.50, "expense_date": "2025-03-15", "shopping_type": "Coffee", "payment_method": "Debit Card", "recurring": true}},
    {"after": {"expense_id": "e2e-002", "user_id": "user-2", "description": "Groceries at Whole Foods", "merchant": "Whole Foods", "expense_amount": 87.23, "expense_date": "2025-03-15", "shopping_type": "Groceries", "payment_method": "Credit Card", "recurring": true}},
    {"after": {"expense_id": "e2e-003", "user_id": "user-1", "description": "Gas at Shell", "merchant": "Shell", "expense_amount": 45.00, "expense_date": "2025-03-15", "shopping_type": "Transport", "payment_method": "Debit Card", "recurring": false}}
  ]'
```

After the batch flush you should see:
```
✅ Flushed 3 events -> cdc/expenses/year=2025/month=03/day=15/expenses_... (3684 bytes)
✅ Presto INSERT: 3 rows into iceberg_data.banko.expenses (13.2s)
```

### Step 9: Test the Full CDC Lifecycle

Send inserts, updates, and deletes to demonstrate the complete CDC audit trail:

```bash
# 1. INSERT: multiple expenses
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[
    {"after": {"expense_id": "demo-001", "user_id": "user-1", "description": "Coffee at Starbucks", "merchant": "Starbucks", "expense_amount": 5.50, "expense_date": "2025-04-01", "shopping_type": "Coffee", "payment_method": "Debit Card", "recurring": true}},
    {"after": {"expense_id": "demo-002", "user_id": "user-1", "description": "Groceries at Whole Foods", "merchant": "Whole Foods", "expense_amount": 142.87, "expense_date": "2025-04-01", "shopping_type": "Groceries", "payment_method": "Credit Card", "recurring": true}},
    {"after": {"expense_id": "demo-003", "user_id": "user-2", "description": "Uber ride to airport", "merchant": "Uber", "expense_amount": 48.30, "expense_date": "2025-04-01", "shopping_type": "Transport", "payment_method": "Apple Pay", "recurring": false}}
  ]'

# Wait for flush, then...

# 2. UPDATE: amount changed, description updated
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[
    {"before": {"expense_id": "demo-001", "expense_amount": 5.50}, "after": {"expense_id": "demo-001", "user_id": "user-1", "description": "Coffee at Starbucks (added pastry)", "merchant": "Starbucks", "expense_amount": 12.75, "expense_date": "2025-04-01", "shopping_type": "Coffee", "payment_method": "Credit Card", "recurring": true}}
  ]'

# 3. DELETE: expense removed (after is null, before has the expense_id)
curl -X POST http://localhost:5002/cdc/events \
  -H "Content-Type: application/json" \
  -d '[
    {"before": {"expense_id": "demo-003", "user_id": "user-2", "description": "Uber ride to airport", "merchant": "Uber", "expense_amount": 48.30, "expense_date": "2025-04-01", "shopping_type": "Transport", "payment_method": "Apple Pay", "recurring": false}, "after": null}
  ]'
```

### Step 10: Query CDC Data and Iceberg Features

In the watsonx.data **Query workspace** (also available in `sql/watsonx-data-setup.sql`):

```sql
-- View all CDC events (inserts, updates, and deletes)
SELECT expense_id, merchant, expense_amount, cdc_operation, cdc_timestamp
FROM iceberg_data.banko.expenses
ORDER BY cdc_timestamp DESC;

-- CDC audit trail for a single expense (insert -> update -> delete)
SELECT expense_id, description, expense_amount, payment_method, cdc_operation, cdc_timestamp
FROM iceberg_data.banko.expenses
WHERE expense_id = 'demo-001'
ORDER BY cdc_timestamp;

-- CDC operation counts
SELECT cdc_operation, COUNT(*) AS event_count
FROM iceberg_data.banko.expenses
GROUP BY cdc_operation;

-- Spending by category (exclude deleted records)
SELECT shopping_type, SUM(expense_amount) AS total, COUNT(*) AS txn_count
FROM iceberg_data.banko.expenses
WHERE cdc_operation != 'delete'
GROUP BY shopping_type
ORDER BY total DESC;

-- Current state: latest version of each expense (dedup updates, exclude deletes)
SELECT e.*
FROM iceberg_data.banko.expenses e
INNER JOIN (
    SELECT expense_id, MAX(cdc_timestamp) AS latest
    FROM iceberg_data.banko.expenses
    GROUP BY expense_id
) latest ON e.expense_id = latest.expense_id AND e.cdc_timestamp = latest.latest
WHERE e.cdc_operation != 'delete';
```

### Step 11: Iceberg Table Features

```sql
-- Time travel: view table snapshots (each batch flush creates a new snapshot)
SELECT * FROM iceberg_data.banko."expenses$snapshots"
ORDER BY committed_at DESC;

-- Query table as it was at a previous snapshot
-- (replace <snapshot-id> with an actual snapshot ID from the query above)
SELECT * FROM iceberg_data.banko.expenses
FOR VERSION AS OF <snapshot-id>;

-- Table history
SELECT * FROM iceberg_data.banko."expenses$history";

-- Partition statistics (data is partitioned by shopping_type)
SELECT * FROM iceberg_data.banko."expenses$partitions";

-- Underlying data files
SELECT * FROM iceberg_data.banko."expenses$manifests";
SELECT * FROM iceberg_data.banko."expenses$files";
```

## Running the Demo

### Automated Demo Script

The demo script generates randomized CDC events (inserts, updates, deletes) with current timestamps. Each run produces unique data.

```bash
# 1. Clean up (run in watsonx.data Query workspace, also in sql/cleanup.sql)
DROP TABLE IF EXISTS iceberg_data.banko.expenses;
CREATE TABLE iceberg_data.banko.expenses (
    expense_id VARCHAR, user_id VARCHAR, description VARCHAR, merchant VARCHAR,
    expense_amount DOUBLE, expense_date VARCHAR, shopping_type VARCHAR,
    payment_method VARCHAR, recurring BOOLEAN, cdc_table VARCHAR,
    cdc_operation VARCHAR, cdc_timestamp VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['shopping_type']);

# 2. Start the pipeline (with all env vars set)
uv run crdb-wxd-pipeline webhook

# 3. In another terminal, run the demo
uv run python scripts/demo.py
```

The demo script:
- Sends 10 random inserts (diverse merchants and categories)
- Waits for flush, then sends 4 updates (amount corrections)
- Waits for flush, then sends 2 deletes (expense removals)
- Prints suggested Presto queries with the actual expense IDs

Options:
```bash
# Custom pipeline URL and batch pause
uv run python scripts/demo.py --url http://localhost:5002 --batch-pause 20
```

## Analytics Dashboard (Grafana)

Visualize CDC data with a pre-built Grafana dashboard. The pipeline exposes dashboard API endpoints that query Presto/Iceberg and return JSON. Grafana's Infinity datasource polls these endpoints.

### Start Grafana

```bash
docker compose --profile dashboard up -d grafana
```

Open http://localhost:3000 (admin/admin) and navigate to the **CockroachDB CDC Analytics** dashboard.

### Dashboard Panels

- **Total CDC Events** -- count of all insert/update/delete events
- **Total Spend** -- sum of expense amounts (excludes deletes)
- **Unique Expenses** -- distinct expense IDs
- **Unique Merchants** -- distinct merchants
- **Spending by Category** -- donut chart by shopping_type
- **CDC Operations** -- donut chart (insert/update/delete split)
- **Top Merchants by Spend** -- horizontal bar gauge
- **Recent CDC Events** -- table with latest 20 events

The dashboard auto-refreshes every 30 seconds.

### Dashboard API Endpoints

The pipeline exposes these endpoints when Presto is configured:

| Endpoint                                  | Description                                    |
|-------------------------------------------|------------------------------------------------|
| `GET /cdc/dashboard/totals`               | Total events, spend, unique expenses/merchants |
| `GET /cdc/dashboard/spending-by-category` | Spend grouped by shopping_type                 |
| `GET /cdc/dashboard/top-merchants`        | Top 10 merchants by spend                      |
| `GET /cdc/dashboard/cdc-operations`       | Count by CDC operation type                    |
| `GET /cdc/dashboard/recent-events`        | Latest 20 CDC events                           |
| `GET /cdc/dashboard/snapshots`            | Iceberg table snapshots                        |

### Cleanup (Reset for Fresh Demo)

```bash
# Automated: cancels changefeeds, resets TPC-C, prints Iceberg SQL
./scripts/cleanup.sh                             # Clean all
./scripts/cleanup.sh --workload expenses         # Expenses only
./scripts/cleanup.sh --workload tpcc             # TPC-C only
./scripts/cleanup.sh --crdb-url "postgresql://..." # Custom CockroachDB

# Then run the printed SQL in the watsonx.data Query workspace
# (or use sql/cleanup.sql for expenses, sql/setup-tpcc.sql for TPC-C)
```

### Full Demo Workflow (Webhook Path)

```bash
# 1. Clean up: ./scripts/cleanup.sh --workload expenses
#    + run sql/cleanup.sql in watsonx.data Query workspace
# 2. Start pipeline:  uv run crdb-wxd-pipeline webhook
# 3. Start Grafana:   docker compose --profile dashboard up -d grafana
# 4. Run demo:        uv run python scripts/demo.py
# 5. Open dashboard:  http://localhost:3000/d/crdb-cdc/ (admin/admin)
# 6. Watch CDC events flow into charts in real-time
```

### Full Demo Workflow (Kafka + Debezium Path)

```bash
# 1. Clean up: ./scripts/cleanup.sh --workload expenses
#    + run sql/cleanup.sql in watsonx.data Query workspace
# 2. Start everything: docker compose --profile kafka --profile dashboard up -d
# 3. Wait for Connect: until curl -s http://localhost:8083/ > /dev/null 2>&1; do sleep 2; done
# 4. Register connector: curl -X POST http://localhost:8083/connectors \
#      -H "Content-Type: application/json" -d @sql/debezium-connector.json
# 5. Insert data into CockroachDB:
#      docker exec crdb-source cockroach sql --insecure \
#        --execute "INSERT INTO expenses (...) VALUES (...);"
# 6. Watch pipeline: docker compose logs -f pipeline-kafka
# 7. Open dashboard: http://localhost:3000/d/crdb-cdc/ (admin/admin)
```

## Data Federation (CockroachDB as PostgreSQL Datasource)

CockroachDB is wire-compatible with PostgreSQL. Register it as a PostgreSQL datasource in watsonx.data to enable CTAS materialization (batch snapshots into Iceberg) and hybrid JOINs (live OLTP + Iceberg history in one query).

**Federation is the on-ramp to Iceberg, not a replacement for it.** Querying CockroachDB through Presto is no different from querying it directly. The value comes when you use federation to materialize data INTO Iceberg or JOIN live data with Iceberg CDC history.

### Setup

1. In the watsonx.data console, go to **Infrastructure Manager** > **Add component** > **Add database**
2. Select **PostgreSQL** as the database type
3. Fill in your CockroachDB connection details:
   - **Host**: `<your-cluster>.cockroachlabs.cloud` (or self-hosted host)
   - **Port**: `26257`
   - **Database**: `defaultdb`
   - **Username / Password**: your CockroachDB credentials
   - **SSL**: enabled (upload the CockroachDB CA certificate)
4. Set the **catalog name** to `cockroachdb`
5. Associate the catalog with your Presto engine and **restart the engine**

### CTAS Materialization (OLTP to Iceberg)

Pull live CockroachDB data into Iceberg for heavy analytics. Run periodically (hourly/daily) to refresh.

```sql
-- Materialize current CockroachDB state into Iceberg (columnar Parquet, partitioned)
CREATE TABLE iceberg_data.banko.expenses_snapshot
WITH (format = 'PARQUET', partitioning = ARRAY['shopping_type'])
AS
SELECT *, CAST(CURRENT_TIMESTAMP AS VARCHAR) AS snapshot_timestamp
FROM cockroachdb.public.expenses;

-- Now run heavy analytics on the Iceberg snapshot (zero load on CockroachDB)
SELECT shopping_type, SUM(expense_amount) AS total_spend, COUNT(*) AS txn_count
FROM iceberg_data.banko.expenses_snapshot
GROUP BY shopping_type ORDER BY total_spend DESC;
```

### Hybrid JOINs (Live OLTP + CDC History in Iceberg)

The killer query: combine live CockroachDB data with CDC change history stored in Iceberg.

```sql
-- Join live OLTP state with full CDC audit trail
SELECT
    live.expense_id, live.merchant,
    live.expense_amount AS current_amount,
    cdc.expense_amount AS historical_amount,
    cdc.cdc_operation, cdc.cdc_timestamp
FROM cockroachdb.public.expenses live
JOIN iceberg_data.banko.expenses cdc
  ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
ORDER BY cdc.cdc_timestamp DESC LIMIT 20;

-- Compare all three data sources side-by-side
SELECT 'Live (CockroachDB)' AS source, COUNT(*) AS rows, SUM(expense_amount) AS total
FROM cockroachdb.public.expenses
UNION ALL
SELECT 'CDC History (Iceberg)', COUNT(*), SUM(expense_amount)
FROM iceberg_data.banko.expenses
UNION ALL
SELECT 'Snapshot (Iceberg)', COUNT(*), SUM(expense_amount)
FROM iceberg_data.banko.expenses_snapshot;
```

See `sql/federation-setup.sql` for complete setup instructions and `sql/demo-federation.sql` for a guided walkthrough of all three patterns.

## Direct Parquet to COS (Path 4)

CockroachDB changefeeds support `format=parquet` natively with cloud storage sinks. Since IBM COS is S3-compatible, you can write Parquet files directly from CockroachDB to COS -- no pipeline needed for the file write.

```sql
CREATE CHANGEFEED FOR expenses
INTO 's3://{BUCKET}/cdc/expenses/?AWS_ACCESS_KEY_ID={KEY}&AWS_SECRET_ACCESS_KEY={SECRET}&AWS_ENDPOINT=https://{COS_ENDPOINT}&AWS_REGION=us-south'
WITH format = parquet, updated, diff, resolved = '10s', partition_format = 'daily';
```

This eliminates the webhook/Kafka pipeline for the Parquet conversion step. However, the Parquet files land as raw files on COS -- they are **not** in Iceberg format. To get Iceberg features (time travel, snapshots, partition pruning), you still need a Spark or Presto job to load the Parquet files into Iceberg tables.

> **Note**: CockroachDB's Parquet writer does not support `VECTOR` columns. Use a CDC query (`AS SELECT ...`) to exclude them. Tables without VECTOR columns (e.g., TPC-C) work with standard `CREATE CHANGEFEED FOR table`.

| Path                   | Parquet Conversion | Iceberg             | Best For                                   |
|------------------------|--------------------|---------------------|--------------------------------------------|
| Webhook/Kafka pipeline | Pipeline           | Yes (Presto INSERT) | Full Iceberg features, real-time dashboard |
| Direct Parquet to COS  | CockroachDB native | No (raw files)      | Simple archive, Spark batch load           |

CockroachDB auto-adds metadata columns: `__crdb__event_type` (c/u/d), `__crdb__updated`, `__crdb__before` (diff).

See `sql/create-changefeed-cos-parquet.sql` for the full SQL with IBM COS HMAC credentials.

## TPC-C Workload Demo

The pipeline supports any CockroachDB table, not just the Banko expenses schema. The TPC-C demo uses CockroachDB's [built-in TPC-C workload](https://www.cockroachlabs.com/docs/stable/cockroach-workload) to generate realistic OLTP traffic (orders, payments, deliveries, stock updates) and streams all changes to Iceberg.

### Quick Start

```bash
# 1. Start the CDC pipeline (in one terminal)
uv run crdb-wxd-pipeline webhook

# 2. Run the automated TPC-C demo (in another terminal)
./scripts/demo-watsonx.sh --workload tpcc --crdb-url "postgresql://root@localhost:26257/tpcc?sslmode=disable"
```

The demo script automatically:
1. Initializes TPC-C with `cockroach workload init tpcc` (1 warehouse, ~600K rows)
2. Creates a changefeed on 7 high-activity tables (order, order_line, new_order, customer, district, stock, history)
3. Runs `cockroach workload run tpcc --tolerate-errors` to generate transactions
4. Shows pipeline stats as CDC events flow to Parquet/Iceberg
5. Prints analytics queries for the watsonx.data Query workspace

### Options

```bash
# More warehouses and longer workload
./scripts/demo-watsonx.sh --workload tpcc --tpcc-warehouses 5 --tpcc-duration 120s

# Run specific act only
./scripts/demo-watsonx.sh --workload tpcc --act 3    # Just run the workload
```

### Iceberg Tables

Create the TPC-C Iceberg tables in the watsonx.data Query workspace (also in `sql/setup-tpcc.sql`):

```sql
CREATE SCHEMA IF NOT EXISTS iceberg_data.tpcc
WITH (location = 's3a://<your-bucket-name>/tpcc');

-- 9 tables: warehouse, district, customer, order, order_line,
-- new_order, item, stock, history
-- See sql/setup-tpcc.sql for full CREATE TABLE statements
```

### Analytics Queries

See `sql/demo-tpcc.sql` for the full set, including:
- CDC events by table and operation type
- Order revenue by district
- Customer balance change tracking
- Low stock alerts
- New order lifecycle (insert -> delete on delivery)
- OLTP vs OLAP comparison (same query on CockroachDB vs Iceberg)
- Iceberg time travel on TPC-C data

### Cleanup

```bash
# Reset everything for a fresh TPC-C demo
./scripts/cleanup.sh --workload tpcc
# Then run sql/setup-tpcc.sql in watsonx.data Query workspace
```

## Related Projects

- [Banko AI Assistant](https://github.com/cockroachlabs-field/banko-ai-assistant) -- The OLTP application (RAG, agents, fraud detection)
- [Debezium CockroachDB Connector](https://github.com/debezium/debezium-connector-cockroachdb) -- CDC connector for Kafka
- [Debezium CockroachDB Examples](https://github.com/viragtripathi/debezium-cockroachdb-examples) -- runnable Debezium connector examples (Kafka and sinkless)

## License

MIT
