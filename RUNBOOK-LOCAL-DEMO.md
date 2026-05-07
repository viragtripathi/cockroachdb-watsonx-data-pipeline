# Local Demo Runbook (IBM Think)

A condensed, copy-paste-ready guide for running the full
CockroachDB → watsonx.data Developer Edition demo on your laptop.

---

## What you have

| Component | Where | How to verify |
|---|---|---|
| watsonx.data DE | `kind` cluster, namespace `wxd` | `kubectl -n wxd get pods` (all `Running`) |
| Console UI | `https://localhost:6443` (`ibmlhadmin` / `password`) | Browser |
| MinIO API | `http://localhost:9000` (`dummyvalue` / `dummyvalue`) | Port-forwarded |
| MinIO UI | `http://localhost:9001` | Port-forwarded |
| Presto | `https://localhost:8443` (basic auth) | Port-forwarded |
| MDS Thrift | `localhost:8381` | Port-forwarded |
| CockroachDB | `localhost:26257` (insecure, in Docker) | `cockroach sql --insecure --url ...` |
| Iceberg catalog | `iceberg_data` (preconfigured by DE) | `SHOW CATALOGS` in Presto |
| CRDB federation catalog | `cockroachdb` (registered via UI, persistent) | `SHOW CATALOGS` includes `cockroachdb` |

---

## Restart from scratch

If anything goes sideways, run these in order. None of these wipe your DE
install or the Banko data — they only restart processes and the CDC table.

### 1. Re-establish port-forwards

```bash
./scripts/port-forward-wxd.sh
```

This (re)starts:
- `localhost:8443 → ibm-lh-presto-svc:8443`
- `localhost:9000 → ibm-lh-minio-svc:9000`

It's idempotent — kills any existing forwards on those ports first.

If the install-time port-forwards (`6443`, `9001`, `8381`) are also dead,
re-run them from the DE install notes:

```bash
nohup kubectl port-forward -n wxd service/lhconsole-ui-svc        6443:443  --address 0.0.0.0 > /dev/null 2>&1 &
nohup kubectl port-forward -n wxd service/ibm-lh-minio-svc        9001:9001 --address 0.0.0.0 > /dev/null 2>&1 &
nohup kubectl port-forward -n wxd service/ibm-lh-mds-thrift-svc   8381:8381 --address 0.0.0.0 > /dev/null 2>&1 &
```

### 2. Confirm Presto + MinIO + CRDB are all reachable

```bash
# Presto
curl -sk -u ibmlhadmin:password https://localhost:8443/v1/info | head -c 200; echo

# MinIO
curl -s http://localhost:9000/minio/health/live -o /dev/null -w "MinIO: %{http_code}\n"

# CockroachDB
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
    -e "SELECT version();" | head -2
```

### 3. Cancel any old changefeeds

```bash
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" -e "
  CANCEL JOBS (
    SELECT job_id FROM [SHOW JOBS]
    WHERE job_type='CHANGEFEED' AND status='running'
  );
"
```

### 4. Reset the CDC table (clean slate)

```bash
python3 - <<'EOF'
import requests, urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
AUTH=("ibmlhadmin","password"); URL="https://localhost:8443/v1/statement"
H={"X-Presto-User":"ibmlhadmin","Content-Type":"text/plain"}
def run(sql):
    r=requests.post(URL,headers=H,data=sql,auth=AUTH,verify=False,timeout=60); j=r.json()
    while j.get("nextUri"):
        time.sleep(0.3); j=requests.get(j["nextUri"],auth=AUTH,verify=False).json()
        if j.get("stats",{}).get("state") in ("FINISHED","FAILED"): break
    print(("✅" if j.get("stats",{}).get("state")=="FINISHED" else "❌"), sql[:60])
run("DROP TABLE IF EXISTS iceberg_data.banko.expenses")
run("DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot")
run("CREATE SCHEMA IF NOT EXISTS iceberg_data.banko WITH (location='s3a://iceberg-bucket/banko')")
run("""CREATE TABLE iceberg_data.banko.expenses (
    expense_id VARCHAR, user_id VARCHAR, description VARCHAR, merchant VARCHAR,
    expense_amount DOUBLE, expense_date VARCHAR, shopping_type VARCHAR,
    payment_method VARCHAR, recurring BOOLEAN, cdc_table VARCHAR,
    cdc_operation VARCHAR, cdc_timestamp VARCHAR
) WITH (format='PARQUET', partitioning=ARRAY['shopping_type'])""")
EOF
```

### 5. Start the pipeline (Terminal A)

```bash
cd /Users/viragtripathi/idea_workspace/cockroachdb-watsonx-data-pipeline
source .env.local
uv run crdb-wxd-pipeline webhook
```

You should see:
```
Sink: S3-compatible (http://localhost:9000 / bucket=iceberg-bucket)
Presto: localhost:8443 -- local DE (basic auth)
✅ Presto connection verified
Iceberg: iceberg_data.banko.expenses via Presto
CDC Webhook receiver starting on https://0.0.0.0:5002
```

### 6. Start the changefeed (Terminal B)

CRDB is in Docker, so it must reach the pipeline via `host.docker.internal`,
NOT `localhost` or `127.0.0.1`:

```bash
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" -e "
SET CLUSTER SETTING kv.rangefeed.enabled = true;
CREATE CHANGEFEED FOR TABLE expenses
INTO 'webhook-https://host.docker.internal:5002/cdc/events?insecure_tls_skip_verify=true'
WITH updated, diff, resolved = '15s', min_checkpoint_frequency = '5s';
"
```

The 5000-row backfill takes ~60 seconds. Watch progress:

```bash
watch -n 5 'curl -sk https://localhost:5002/cdc/stats'
```

---

## Demo flow (recommended order for the IBM Think session)

Open the watsonx.data Query workspace at **https://localhost:6443/** and run
each section from the existing SQL files. Everything below is **already
written** in your repo.

### Act 1 — Verify both data sources are visible

> File: `sql/demo-federation.sql` (STEP 1)

```sql
SHOW CATALOGS;
-- Expect: cockroachdb, iceberg_data, hive_data, system, ...

SELECT 'CockroachDB (Live)' AS source, COUNT(*) AS rows
FROM cockroachdb.public.expenses
UNION ALL
SELECT 'Iceberg (CDC History)', COUNT(*)
FROM iceberg_data.banko.expenses;
```

**Talking point:** "One Presto endpoint. Two data sources. Federation lets us
query the live OLTP database and the historical lakehouse from the same SQL
shell."

### Act 2 — Federated live query (the on-ramp)

> File: `sql/demo-federation.sql` (STEP 2)

```sql
SELECT shopping_type,
       COUNT(*) AS transactions,
       ROUND(SUM(expense_amount), 2) AS total_spend
FROM cockroachdb.public.expenses
GROUP BY shopping_type
ORDER BY total_spend DESC;
```

**Talking point:** "This query is hitting CockroachDB live, through
PostgreSQL wire protocol, with no copy. Great for exploration, not for heavy
analytics — that would slow down the OLTP."

### Act 3 — CDC history in Iceberg

> File: `sql/demo-federation.sql` (STEP 3)

```sql
SELECT cdc_operation, COUNT(*) AS event_count,
       ROUND(SUM(expense_amount), 2) AS total_amount
FROM iceberg_data.banko.expenses
GROUP BY cdc_operation
ORDER BY event_count DESC;

-- Iceberg time travel
SELECT snapshot_id, committed_at, summary
FROM iceberg_data.banko."expenses$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
```

**Talking point:** "Every change CockroachDB has seen is here, captured by
the CDC pipeline as columnar Parquet. Each batch is a new Iceberg snapshot
— time travel is free."

### Act 4 — CTAS materialization (federation → Iceberg)

> File: `sql/demo-federation.sql` (STEP 4)

```sql
DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot;

CREATE TABLE iceberg_data.banko.expenses_snapshot
WITH (format='PARQUET', partitioning=ARRAY['shopping_type'])
AS
SELECT *, CAST(CURRENT_TIMESTAMP AS VARCHAR) AS snapshot_timestamp
FROM cockroachdb.public.expenses;

SELECT shopping_type, COUNT(*), ROUND(SUM(expense_amount),2) AS total
FROM iceberg_data.banko.expenses_snapshot
GROUP BY shopping_type ORDER BY total DESC;
```

**Talking point:** "One CTAS, one query, OLTP → columnar Parquet on object
storage, partitioned and ready for analytical scans. No ETL job. Run on a
schedule."

### Act 5 — Hybrid JOIN (the killer query)

> File: `sql/demo-federation.sql` (STEP 5)

```sql
-- Cross-source comparison
SELECT 'Live (CockroachDB)' AS source, COUNT(*) AS rows,
       ROUND(SUM(expense_amount),2) AS total_spend
FROM cockroachdb.public.expenses
UNION ALL
SELECT 'CDC History (Iceberg)', COUNT(*), ROUND(SUM(expense_amount),2)
FROM iceberg_data.banko.expenses
UNION ALL
SELECT 'Snapshot (Iceberg)', COUNT(*), ROUND(SUM(expense_amount),2)
FROM iceberg_data.banko.expenses_snapshot;

-- The hybrid JOIN — only meaningful if you've done some UPDATEs (see below)
SELECT live.expense_id, live.merchant,
       live.expense_amount AS current_amount,
       cdc.expense_amount AS previous_amount,
       ROUND(live.expense_amount - cdc.expense_amount, 2) AS difference,
       cdc.cdc_operation, cdc.cdc_timestamp
FROM cockroachdb.public.expenses live
JOIN iceberg_data.banko.expenses cdc
  ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id
WHERE cdc.cdc_operation = 'update'
ORDER BY ABS(live.expense_amount - cdc.expense_amount) DESC
LIMIT 10;
```

**Talking point:** "Live state from CockroachDB. Historical state from
Iceberg. One JOIN. This is the query that takes weeks of plumbing to build
in most enterprise stacks."

### Act 5b — The price-drift query (the real audience moment)

The query above JOINs on the latest CDC version. To highlight how prices
have *changed*, JOIN against the **first-seen** insert event:

```sql
WITH original AS (
  SELECT expense_id, expense_amount AS original_amount
  FROM iceberg_data.banko.expenses
  WHERE cdc_operation = 'insert'
)
SELECT live.expense_id, live.merchant,
       ROUND(live.expense_amount, 2)       AS current_amount,
       ROUND(original.original_amount, 2)  AS original_amount,
       ROUND(live.expense_amount - original.original_amount, 2) AS price_change
FROM cockroachdb.public.expenses live
JOIN original ON CAST(live.expense_id AS VARCHAR) = original.expense_id
WHERE live.expense_amount <> original.original_amount
ORDER BY ABS(live.expense_amount - original.original_amount) DESC
LIMIT 10;
```

Output (verified live against the local stack):

| expense_id | merchant | current | original | change |
|---|---|---|---|---|
| 3bdba940... | Delta Airlines | $2419.89 | $2199.90 | +$219.99 |
| dc7dd35c... | Delta Airlines | $2419.58 | $2199.62 | +$219.96 |
| 8b789ccb... | Hilton Hotels  | $2417.50 | $2197.73 | +$219.77 |

**Talking point:** "These three rows had price corrections applied. Live
shows the latest. Iceberg preserves the original. The difference is the
audit trail. No instrumentation, no triggers — just CDC into a lakehouse."

---

## Making the hybrid JOIN visually compelling (live during the demo)

The Banko app keeps inserting rows in the background, so manual UPDATEs get
diluted by noise. To make the hybrid JOIN return meaningful matches *during
the demo*, do this **after Acts 1-4 but before Act 5**:

```bash
# In a third terminal — generate visible price corrections
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" -e "
-- Pick the 5 highest-amount expenses and bump them by 15%
UPDATE expenses
SET expense_amount = expense_amount * 1.15,
    description = description || ' (price correction at IBM Think demo)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses ORDER BY expense_amount DESC LIMIT 5
);

-- Delete the 3 smallest expenses (ghost record demo)
DELETE FROM expenses
WHERE expense_id IN (
  SELECT expense_id FROM expenses ORDER BY expense_amount ASC LIMIT 3
);
"
```

Wait ~20 seconds for the pipeline to flush the events to Iceberg, then run
the hybrid JOIN in Act 5 — you'll see the 5 price corrections with the live
amount, the prior amount, and the timestamp of the change.

---

## Troubleshooting

| Symptom | Diagnosis | Fix |
|---|---|---|
| `SHOW CATALOGS` doesn't include `cockroachdb` | Catalog not registered in DE, or catalog registry hasn't synced | Re-register via UI; wait 30s; rerun |
| Pipeline shows 0 events after CHANGEFEED runs | Changefeed sink URL uses `localhost`/`127.0.0.1` and CRDB is in Docker | Use `webhook-https://host.docker.internal:5002/...` |
| `transient error: connection refused` in CHANGEFEED status | Pipeline not running, port-forward dropped, or wrong host | Check `curl -sk https://localhost:5002/cdc/stats`; restart pipeline |
| `this sink requires webhook-https` | CRDB requires HTTPS for webhook sinks | Pipeline must serve HTTPS — `CDC_WEBHOOK_HTTPS=true` (already in `.env.local`) |
| All CDC events show `cdc_operation=insert` even after UPDATEs | CRDB wraps the diff differently when no `before` field is sent | Make sure changefeed has `WITH updated, diff` |
| Presto query fails with TLS error after pod restart | Port-forward died with the pod | `./scripts/port-forward-wxd.sh` |
| HTTP 401 from Presto | Wrong basic-auth creds | Check `.env.local` has `WATSONX_DATA_USERNAME=ibmlhadmin` and `PASSWORD=password` |
| `No active changefeed` after CRDB restart | Changefeeds need to be recreated | Re-run Step 6 of the restart sequence |

---

## Quick state check (anytime)

```bash
echo "=== Port-forwards ==="
lsof -nP -iTCP -sTCP:LISTEN 2>/dev/null | grep -E ':(6443|8443|8381|9000|9001|26257|5002) '

echo "=== CRDB row count ==="
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
  --format=table -e "SELECT COUNT(*) FROM expenses;"

echo "=== Pipeline stats ==="
curl -sk https://localhost:5002/cdc/stats; echo

echo "=== Active changefeeds ==="
cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
  --format=table -e "SELECT job_id, status FROM [SHOW JOBS] WHERE job_type='CHANGEFEED' AND status='running';"

echo "=== CDC events in Iceberg ==="
python3 -c "
import requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
r=requests.post('https://localhost:8443/v1/statement',
    headers={'X-Presto-User':'ibmlhadmin','Content-Type':'text/plain'},
    auth=('ibmlhadmin','password'), verify=False,
    data='SELECT cdc_operation, COUNT(*) FROM iceberg_data.banko.expenses GROUP BY cdc_operation')
print(r.json())
"
```
