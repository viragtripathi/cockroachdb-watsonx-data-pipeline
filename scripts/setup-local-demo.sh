#!/usr/bin/env bash
# End-to-end runbook for the local watsonx.data Developer Edition demo.
#
# Prerequisites (already in place from earlier sessions):
#   - kind cluster with watsonx.data DE in namespace `wxd`
#   - Local CockroachDB at localhost:26257
#   - .env.local present at repo root
#
# This script:
#   1. Refreshes port-forwards (Presto:8443, MinIO:9000)
#   2. Verifies CockroachDB is reachable and has the banko expenses table
#   3. Creates the Iceberg schema/table for CDC
#   4. Reminds you to register the cockroachdb catalog in the wxd UI (one-time)
#   5. Prints the next commands to run

set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Step 1: Port-forwards ==="
./scripts/port-forward-wxd.sh 2>&1 | tail -8

echo ""
echo "=== Step 2: CockroachDB reachable? ==="
if cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
        -e "SELECT version();" 2>&1 | head -2 | grep -q CockroachDB; then
    echo "  ✅ CockroachDB up"
else
    echo "  ❌ CockroachDB not reachable at localhost:26257 -- start it first"
    exit 1
fi

ROW_COUNT=$(cockroach sql --insecure --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
    --format=csv -e "SELECT COUNT(*) FROM expenses" 2>/dev/null | tail -1 || echo 0)
if [[ "${ROW_COUNT:-0}" -gt 0 ]]; then
    echo "  ✅ banko.expenses populated (${ROW_COUNT} rows)"
else
    echo "  ⚠️  expenses table is empty or missing."
    echo "     Run the Banko AI Assistant to populate it:"
    echo "       git clone https://github.com/cockroachlabs-field/banko-ai-assistant"
    echo "       cd banko-ai-assistant && follow README to load expenses"
fi

echo ""
echo "=== Step 3: Create Iceberg schema + table for CDC ==="
python3 - <<'PYEOF'
import requests, urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
AUTH=("ibmlhadmin","password"); URL="https://localhost:8443/v1/statement"
H={"X-Presto-User":"ibmlhadmin","Content-Type":"text/plain"}
def run(sql):
    r=requests.post(URL,headers=H,data=sql,auth=AUTH,verify=False,timeout=30); j=r.json()
    while j.get("nextUri"):
        time.sleep(0.3)
        j=requests.get(j["nextUri"],auth=AUTH,verify=False,timeout=30).json()
        if j.get("stats",{}).get("state") in ("FINISHED","FAILED"): break
    state=j.get("stats",{}).get("state","?")
    sym = "✅" if state == "FINISHED" else "❌"
    err = ""
    if state == "FAILED":
        err = " -- " + j.get("error",{}).get("message","unknown")
    print(f"  {sym} {sql[:60]}{err}")
for stmt in [
    "CREATE SCHEMA IF NOT EXISTS iceberg_data.banko WITH (location = 's3a://iceberg-bucket/banko')",
    """CREATE TABLE IF NOT EXISTS iceberg_data.banko.expenses (
        expense_id VARCHAR, user_id VARCHAR, description VARCHAR, merchant VARCHAR,
        expense_amount DOUBLE, expense_date VARCHAR, shopping_type VARCHAR,
        payment_method VARCHAR, recurring BOOLEAN, cdc_table VARCHAR,
        cdc_operation VARCHAR, cdc_timestamp VARCHAR
    ) WITH (format = 'PARQUET', partitioning = ARRAY['shopping_type'])""",
]:
    run(stmt)
PYEOF

echo ""
echo "=== Step 4: Register the cockroachdb federation catalog (ONE-TIME) ==="
echo ""
echo "  The Presto file-based registration gets reconciled away by DE's catalog"
echo "  controller, so use the watsonx.data console UI:"
echo ""
echo "    1. Open https://localhost:6443/  (login: ibmlhadmin / password)"
echo "    2. Infrastructure manager > Add component > Add database"
echo "    3. Select 'PostgreSQL'"
echo "    4. Fill in:"
echo "         Display name:      cockroachdb"
echo "         Hostname:          host.docker.internal"
echo "         Port:              26257"
echo "         Database name:     defaultdb"
echo "         Username:          root"
echo "         Password:          (leave blank, --insecure CRDB)"
echo "         SSL:               OFF (insecure local)"
echo "    5. Associate with engine 'presto-01'"
echo "    6. Wait ~30s for the engine to reload, then SHOW CATALOGS should list 'cockroachdb'"
echo ""
echo "  After this is registered ONCE, it persists across pod restarts."
echo ""
echo "=== Step 5: Run the demo ==="
echo ""
echo "  Terminal A:  source .env.local && uv run crdb-wxd-pipeline webhook"
echo "  Terminal B (CRDB changefeed -- only needs to be done once per session):"
echo "    cockroach sql --insecure --url 'postgresql://root@localhost:26257/defaultdb?sslmode=disable' \\"
echo "      -e \"SET CLUSTER SETTING kv.rangefeed.enabled = true;\""
echo "    cockroach sql --insecure --url 'postgresql://root@localhost:26257/defaultdb?sslmode=disable' \\"
echo "      < sql/create-changefeed-webhook.sql"
echo ""
echo "  Then in the wxd Query workspace (https://localhost:6443/), run sections from:"
echo "    sql/demo-federation.sql      -- live + federation + hybrid JOINs (the headline demo)"
echo "    sql/federation-setup.sql     -- detailed CTAS + audit-trail queries"
echo "    sql/demo-tpcc.sql            -- (only if you also load TPC-C)"
echo ""
