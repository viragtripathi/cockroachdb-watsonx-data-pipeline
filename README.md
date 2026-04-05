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
- CockroachDB v25.4+ (included in docker-compose or use CockroachDB Cloud)
- IBM Cloud account with:
  - IBM Cloud Object Storage instance + bucket
  - watsonx.data instance (Lite or Enterprise plan)
  - IBM Cloud API key ([create one here](https://cloud.ibm.com/iam/apikeys))

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

The [Debezium CockroachDB connector](https://github.com/debezium/debezium-connector-cockroachdb) is included in the official Debezium Connect image (`quay.io/debezium/connect:3.5.0.Final`) -- no need to build or copy JARs.

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

### IBM Cloud Object Storage

| Variable          | Description                                | Default          |
|-------------------|--------------------------------------------|------------------|
| `COS_ENDPOINT`    | COS endpoint URL (must include `https://`) | --               |
| `COS_API_KEY`     | COS service credential API key             | --               |
| `COS_INSTANCE_ID` | COS service instance CRN                   | --               |
| `COS_BUCKET`      | Target bucket                              | `crdb-lakehouse` |
| `COS_PREFIX`      | Object key prefix                          | `cdc/expenses/`  |

When COS is not configured, Parquet files are written to `./cdc-output/` (or `CDC_LOCAL_OUTPUT`).

### watsonx.data / Presto (Iceberg Table Insert)

| Variable                 | Description                                        | Default        |
|--------------------------|----------------------------------------------------|----------------|
| `PRESTO_ENGINE_HOST`     | Presto engine hostname (from watsonx.data console) | --             |
| `WATSONX_DATA_API_KEY`   | IBM Cloud platform API key                         | --             |
| `WATSONX_DATA_CATALOG`   | Iceberg catalog name in watsonx.data               | `iceberg_data` |
| `WATSONX_DATA_NAMESPACE` | Iceberg namespace (schema)                         | `banko`        |

When `PRESTO_ENGINE_HOST` and `WATSONX_DATA_API_KEY` are set, the pipeline inserts each batch into the Iceberg table via the Presto REST API after writing Parquet to COS. When not set, only COS/local writes occur.

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
| `cdc_operation`  | string  | insert, update, delete, snapshot |
| `cdc_timestamp`  | string  | CDC event timestamp              |

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
       │             └──────┬─────────────┬───────────┘
       │                    │             │
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
    payment_method VARCHAR, recurring BOOLEAN, cdc_operation VARCHAR,
    cdc_timestamp VARCHAR
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

### Full Demo Workflow (Webhook Path)

```bash
# 1. Clean up Iceberg table (watsonx.data Query workspace, or sql/cleanup.sql)
# 2. Start pipeline:  uv run crdb-wxd-pipeline webhook
# 3. Start Grafana:   docker compose --profile dashboard up -d grafana
# 4. Run demo:        uv run python scripts/demo.py
# 5. Open dashboard:  http://localhost:3000/d/crdb-cdc/ (admin/admin)
# 6. Watch CDC events flow into charts in real-time
```

### Full Demo Workflow (Kafka + Debezium Path)

```bash
# 1. Clean up Iceberg table (watsonx.data Query workspace, or sql/cleanup.sql)
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

## Related Projects

- [Banko AI Assistant](https://github.com/cockroachlabs-field/banko-ai-assistant-rag-demo) -- The OLTP application (RAG, agents, fraud detection)
- [Debezium CockroachDB Connector](https://github.com/debezium/debezium-connector-cockroachdb) -- CDC connector for Kafka
- [Debezium CockroachDB Demo](https://github.com/cockroachlabs-field/debezium-cockroachdb-demo) -- Debezium connector demo with Kafka

## License

MIT
