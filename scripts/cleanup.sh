#!/usr/bin/env bash
# Full demo cleanup — wipes the demo state without touching the wxd install,
# the podman machine, or your Banko source data in CockroachDB.
#
# Default behavior (no args): clean everything safely.
#   - Stop the pipeline process
#   - Cancel all running CockroachDB changefeeds
#   - Drop & recreate the Iceberg expenses + snapshot tables (via Presto)
#   - Delete CDC Parquet files from the MinIO bucket
#
# What this DOES NOT touch (intentionally):
#   - The wxd install / podman machine / kind cluster
#   - The Banko expenses data in CockroachDB (5000+ rows)
#   - The cockroachdb federation catalog registration in wxd
#   - kubectl port-forwards (use ./scripts/port-forward-wxd.sh to refresh)
#
# Usage:
#   ./scripts/cleanup.sh                       # clean everything (default)
#   ./scripts/cleanup.sh --keep-iceberg        # leave Iceberg tables intact
#   ./scripts/cleanup.sh --keep-pipeline       # leave pipeline running
#   ./scripts/cleanup.sh --keep-minio          # leave Parquet files in MinIO
#   ./scripts/cleanup.sh --workload tpcc       # also reset TPC-C tables
#   ./scripts/cleanup.sh --crdb-url "..."      # custom CRDB connection
#   ./scripts/cleanup.sh --dry-run             # show what would happen, do nothing

set -uo pipefail
cd "$(dirname "$0")/.."

CRDB_URL="${CRDB_URL:-postgresql://root@localhost:26257/defaultdb?sslmode=disable}"
PIPELINE_URL="${PIPELINE_URL:-https://localhost:5002}"
PRESTO_URL="${PRESTO_URL:-https://localhost:8443}"
MINIO_BUCKET="${S3_BUCKET:-iceberg-bucket}"
MINIO_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
MINIO_KEY="${S3_ACCESS_KEY:-dummyvalue}"
MINIO_SECRET="${S3_SECRET_KEY:-dummyvalue}"

KEEP_ICEBERG=0
KEEP_PIPELINE=0
KEEP_MINIO=0
WORKLOAD=expenses
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-iceberg)  KEEP_ICEBERG=1; shift ;;
        --keep-pipeline) KEEP_PIPELINE=1; shift ;;
        --keep-minio)    KEEP_MINIO=1; shift ;;
        --workload)      WORKLOAD="$2"; shift 2 ;;
        --crdb-url)      CRDB_URL="$2"; shift 2 ;;
        --dry-run)       DRY_RUN=1; shift ;;
        -h|--help)       sed -n '2,21p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1 (use --help)"; exit 2 ;;
    esac
done

ok()   { echo "  ✅ $*"; }
info() { echo "  ℹ  $*"; }
warn() { echo "  ⚠️  $*"; }
fail() { echo "  ❌ $*"; }
hr()   { printf "\n=== %s ===\n" "$*"; }
do_or_print() {
    if [[ "$DRY_RUN" -eq 1 ]]; then
        echo "  [dry-run] $*"
    else
        eval "$*"
    fi
}

echo "Cleanup mode: workload=${WORKLOAD}  iceberg=$([[ $KEEP_ICEBERG -eq 0 ]] && echo wipe || echo keep)  pipeline=$([[ $KEEP_PIPELINE -eq 0 ]] && echo stop || echo keep)  minio=$([[ $KEEP_MINIO -eq 0 ]] && echo wipe || echo keep)"
[[ "$DRY_RUN" -eq 1 ]] && echo "(dry-run — no changes will be made)"

# ----------------------------------------------------------------------
hr "1. Stop the pipeline"
# ----------------------------------------------------------------------
if [[ "$KEEP_PIPELINE" -eq 1 ]]; then
    info "skipping (--keep-pipeline)"
else
    PIDS=$(pgrep -f "crdb-wxd-pipeline webhook" 2>/dev/null || true)
    if [[ -n "$PIDS" ]]; then
        if [[ "$DRY_RUN" -eq 1 ]]; then
            echo "  [dry-run] would kill pipeline PIDs: $PIDS"
        else
            kill $PIDS 2>/dev/null
            sleep 2
            pkill -9 -f "crdb-wxd-pipeline webhook" 2>/dev/null || true
            ok "pipeline stopped (was PIDs: $PIDS)"
        fi
    else
        info "pipeline not running"
    fi
fi

# ----------------------------------------------------------------------
hr "2. Cancel CockroachDB changefeeds"
# ----------------------------------------------------------------------
if ! cockroach sql --insecure --url "$CRDB_URL" -e "SELECT 1;" >/dev/null 2>&1; then
    warn "CockroachDB not reachable at $CRDB_URL — skipping changefeed cleanup"
else
    JOBS=$(cockroach sql --insecure --url "$CRDB_URL" --format=csv -e "
        SELECT job_id FROM [SHOW JOBS]
        WHERE job_type='CHANGEFEED' AND status='running';
    " 2>/dev/null | tail -n +2)
    if [[ -z "$JOBS" ]]; then
        info "no active changefeeds"
    else
        for j in $JOBS; do
            do_or_print "cockroach sql --insecure --url '$CRDB_URL' -e 'CANCEL JOB $j;' >/dev/null 2>&1 || true"
            ok "cancelled changefeed $j"
        done
    fi
fi

# ----------------------------------------------------------------------
hr "3. Drop & recreate Iceberg tables (via Presto)"
# ----------------------------------------------------------------------
if [[ "$KEEP_ICEBERG" -eq 1 ]]; then
    info "skipping (--keep-iceberg)"
else
    if ! curl -sk -u ibmlhadmin:password --max-time 5 "${PRESTO_URL}/v1/info" >/dev/null 2>&1; then
        warn "Presto not reachable at ${PRESTO_URL} — skipping Iceberg cleanup"
    elif [[ "$DRY_RUN" -eq 1 ]]; then
        echo "  [dry-run] would DROP and recreate iceberg_data.banko.expenses + expenses_snapshot"
        if [[ "$WORKLOAD" == "tpcc" || "$WORKLOAD" == "all" ]]; then
            echo "  [dry-run] would DROP all 9 iceberg_data.tpcc.* tables"
        fi
    else
        python3 - <<PYEOF
import requests, urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
AUTH=("ibmlhadmin","password"); URL="${PRESTO_URL}/v1/statement"
H={"X-Presto-User":"ibmlhadmin","Content-Type":"text/plain"}
def run(sql):
    r=requests.post(URL,headers=H,data=sql,auth=AUTH,verify=False,timeout=60); j=r.json()
    while True:
        nxt=j.get("nextUri"); state=j.get("stats",{}).get("state")
        if not nxt or state in ("FINISHED","FAILED"): break
        time.sleep(0.3); j=requests.get(nxt,auth=AUTH,verify=False,timeout=60).json()
    return j.get("stats",{}).get("state","?"), j.get("error",{}).get("message","")

stmts = []
if "${WORKLOAD}" in ("expenses","all"):
    stmts += [
        ("DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot", "drop snapshot"),
        ("DROP TABLE IF EXISTS iceberg_data.banko.expenses",          "drop expenses CDC table"),
        ("CREATE SCHEMA IF NOT EXISTS iceberg_data.banko WITH (location='s3a://${MINIO_BUCKET}/banko')", "ensure schema"),
        ("""CREATE TABLE iceberg_data.banko.expenses (
            expense_id VARCHAR, user_id VARCHAR, description VARCHAR, merchant VARCHAR,
            expense_amount DOUBLE, expense_date VARCHAR, shopping_type VARCHAR,
            payment_method VARCHAR, recurring BOOLEAN, cdc_table VARCHAR,
            cdc_operation VARCHAR, cdc_timestamp VARCHAR
        ) WITH (format='PARQUET', partitioning=ARRAY['shopping_type'])""", "recreate empty expenses"),
    ]
if "${WORKLOAD}" in ("tpcc","all"):
    for t in ['warehouse','district','customer','"order"','order_line','new_order','item','stock','history']:
        stmts.append((f"DROP TABLE IF EXISTS iceberg_data.tpcc.{t}", f"drop tpcc.{t}"))

for sql, label in stmts:
    state, err = run(sql)
    print(("  ✅" if state=="FINISHED" else "  ⚠️ "), label, f"({state})", err[:80])
PYEOF
    fi
fi

# ----------------------------------------------------------------------
hr "4. Wipe MinIO Parquet files"
# ----------------------------------------------------------------------
if [[ "$KEEP_MINIO" -eq 1 ]]; then
    info "skipping (--keep-minio)"
elif [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  [dry-run] would delete s3://${MINIO_BUCKET}/cdc/  and  s3://${MINIO_BUCKET}/banko/"
else
    if ! curl -s --max-time 3 "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        warn "MinIO not reachable at ${MINIO_ENDPOINT} — skipping"
    else
        python3 - <<PYEOF
import boto3, botocore
s3 = boto3.client("s3", endpoint_url="${MINIO_ENDPOINT}",
                  aws_access_key_id="${MINIO_KEY}", aws_secret_access_key="${MINIO_SECRET}",
                  region_name="us-east-1",
                  config=botocore.client.Config(signature_version="s3v4", s3={"addressing_style":"path"}))
for prefix in ("cdc/", "banko/"):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket="${MINIO_BUCKET}", Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append({"Key": obj["Key"]})
    if not keys:
        print(f"  ℹ  s3://${MINIO_BUCKET}/{prefix}  empty")
        continue
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket="${MINIO_BUCKET}", Delete={"Objects": keys[i:i+1000]})
    print(f"  ✅ deleted {len(keys)} objects from s3://${MINIO_BUCKET}/{prefix}")
PYEOF
    fi
fi

# ----------------------------------------------------------------------
hr "Summary"
# ----------------------------------------------------------------------
if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  (dry-run complete — re-run without --dry-run to apply)"
else
    echo "  🟢 Cleanup complete"
    echo
    echo "  What's still in place:"
    echo "    - wxd install, podman machine, kind cluster (untouched)"
    echo "    - cockroachdb federation catalog in wxd (untouched)"
    echo "    - Banko data in CockroachDB (untouched)"
    echo "    - Port-forwards (untouched)"
    echo
    echo "  To rebuild the demo state:"
    echo "    ./scripts/preflight-demo.sh"
fi
