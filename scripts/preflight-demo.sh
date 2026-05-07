#!/usr/bin/env bash
# Pre-demo health check WITH AUTO-FIX (the default).
#
# Walks the stack from container runtime -> kubernetes -> network -> data plane
# -> pipeline. At each level: detect, then fix anything safely fixable. Print a
# clear per-level summary at the end.
#
# Usage:
#   ./scripts/preflight-demo.sh             # auto-fix mode (default)
#   ./scripts/preflight-demo.sh --check-only # read-only, never modifies state
#   ./scripts/preflight-demo.sh --quiet      # only print warnings/errors/summary
#
# Exit codes:
#   0  = ready to demo (everything green or auto-fixed)
#   1  = warnings present but usable
#   2  = unrecoverable failure (needs human action; instructions printed)

set -uo pipefail
cd "$(dirname "$0")/.."

# ---------------- Config ----------------
NS="${WXD_NAMESPACE:-wxd}"
PODMAN_MACHINE="${PODMAN_MACHINE:-podman-wxd}"
CRDB_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable"
PIPELINE_URL="https://localhost:5002"
PRESTO_URL="https://localhost:8443"
MINIO_URL="http://localhost:9000"
ENV_FILE=".env.local"
PIPELINE_LOG="/tmp/crdb-wxd-pipeline.log"

CHECK_ONLY=0; QUIET=0
for arg in "$@"; do
    case "$arg" in
        --check-only) CHECK_ONLY=1 ;;
        --quiet)      QUIET=1 ;;
        --help|-h)
            sed -n '2,16p' "$0"; exit 0 ;;
        *) echo "unknown arg: $arg"; exit 2 ;;
    esac
done

# ---------------- Output helpers ----------------
EXIT=0
declare -a SUMMARY_LINES

LEVEL=""
hr() { LEVEL="$*"; [[ "$QUIET" -eq 0 ]] && printf "\n=== %s ===\n" "$LEVEL"; }

ok()    { [[ "$QUIET" -eq 0 ]] && echo "  ✅ $*"; }
warn()  { echo "  ⚠️  $*"; EXIT=$(( EXIT < 1 ? 1 : EXIT )); SUMMARY_LINES+=("⚠️  ${LEVEL}: $*"); }
fail()  { echo "  ❌ $*"; EXIT=2; SUMMARY_LINES+=("❌ ${LEVEL}: $*"); }
fix()   { [[ "$QUIET" -eq 0 ]] && echo "  🔧 $*"; SUMMARY_LINES+=("🔧 ${LEVEL}: $*"); }
info()  { [[ "$QUIET" -eq 0 ]] && echo "     $*"; }

# ---------------- LEVEL 1: Container runtime ----------------
hr "L1. Container runtime (podman)"

if ! command -v podman >/dev/null 2>&1; then
    fail "podman not installed"
    info "Install: https://podman.io/getting-started/installation"
    echo
    printf "Summary:\n"; for l in "${SUMMARY_LINES[@]}"; do echo "  $l"; done
    exit 2
fi
ok "podman $(podman version --format '{{.Client.Version}}' 2>/dev/null || echo '?')"

MACHINE_STATE=$(podman machine list --format '{{.Name}} {{.Running}}' 2>/dev/null | awk -v m="${PODMAN_MACHINE}" '$1 == m || $1 == m"*" {print $2}')
if [[ -z "${MACHINE_STATE}" ]]; then
    fail "podman machine '${PODMAN_MACHINE}' does not exist"
    info "Likely cause: 'podman machine reset' or major upgrade wiped it."
    info "Recovery: recreate the machine and re-run the IBM watsonx.data DE installer."
    info "          (re-creating the machine alone won't bring wxd back -- the install is gone too)"
    echo
    printf "Summary:\n"; for l in "${SUMMARY_LINES[@]}"; do echo "  $l"; done
    exit 2
fi

if [[ "${MACHINE_STATE}" != "true" ]]; then
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        fail "podman machine '${PODMAN_MACHINE}' is stopped (use 'podman machine start ${PODMAN_MACHINE}')"
    else
        fix "starting podman machine '${PODMAN_MACHINE}' (this can take 30-60s)..."
        if podman machine start "${PODMAN_MACHINE}" >/dev/null 2>&1; then
            ok "podman machine started"
        else
            fail "podman machine start failed"
            info "Try: podman machine start ${PODMAN_MACHINE}"
            echo
            printf "Summary:\n"; for l in "${SUMMARY_LINES[@]}"; do echo "  $l"; done
            exit 2
        fi
    fi
else
    ok "podman machine '${PODMAN_MACHINE}' running"
fi

# ---------------- LEVEL 2: Kubernetes / wxd install ----------------
hr "L2. Kubernetes (kind cluster + wxd namespace)"

# Wait briefly for kubectl to reach the cluster API (it can take a moment after machine start)
KUBE_OK=0
for i in 1 2 3 4 5 6; do
    if kubectl get ns >/dev/null 2>&1; then KUBE_OK=1; break; fi
    [[ $i -gt 1 ]] && info "waiting for cluster API (${i}x5s)..."
    sleep 5
done
if [[ "${KUBE_OK}" -eq 0 ]]; then
    fail "Cannot reach the Kubernetes API"
    info "Check: kubectl config current-context  -- should point at your wxd cluster"
    echo
    printf "Summary:\n"; for l in "${SUMMARY_LINES[@]}"; do echo "  $l"; done
    exit 2
fi
ok "Cluster API reachable (context: $(kubectl config current-context 2>/dev/null))"

if ! kubectl get ns "${NS}" >/dev/null 2>&1; then
    fail "namespace '${NS}' does not exist"
    info "Likely cause: wxd helm release was uninstalled, or the kind cluster was deleted."
    info "Recovery: re-run the IBM watsonx.data Developer Edition installer (not auto-recoverable)."
    echo
    printf "Summary:\n"; for l in "${SUMMARY_LINES[@]}"; do echo "  $l"; done
    exit 2
fi
ok "namespace '${NS}' exists"

# Check pods
NOT_READY=$(kubectl -n "${NS}" get pods --no-headers 2>/dev/null \
    | grep -vE "Completed|^$" \
    | awk '$2 !~ /^[1-9]+\/[1-9]+$/ || $3 != "Running"')
if [[ -n "${NOT_READY}" ]]; then
    NOT_READY_NAMES=$(echo "${NOT_READY}" | awk '{print $1}' | tr '\n' ' ')
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "Some pods not Ready: ${NOT_READY_NAMES}"
    else
        fix "rolling-restarting affected deployments..."
        # Map pod -> deployment (strip the -<hash>-<random> suffix)
        for pod in ${NOT_READY_NAMES}; do
            DEPLOY=$(echo "$pod" | sed -E 's/-[a-f0-9]+-[a-z0-9]+$//')
            if kubectl -n "${NS}" get deployment "${DEPLOY}" >/dev/null 2>&1; then
                kubectl -n "${NS}" rollout restart "deployment/${DEPLOY}" >/dev/null 2>&1 \
                    && info "  restarted deployment/${DEPLOY}" \
                    || warn "  failed to restart deployment/${DEPLOY}"
            fi
        done
        info "waiting up to 3 minutes for pods to be Ready..."
        if kubectl -n "${NS}" wait --for=condition=Ready pod --all --timeout=180s >/dev/null 2>&1; then
            ok "all pods Ready after restart"
        else
            warn "some pods still not Ready after 3 minutes (continuing)"
        fi
    fi
else
    ok "all wxd pods Running and Ready"
fi

# Highlight Presto restart count (OOMKilled is the most common headache)
PRESTO_INFO=$(kubectl -n "${NS}" get pods -l app=ibm-lh-presto --no-headers 2>/dev/null | head -1)
if [[ -n "${PRESTO_INFO}" ]]; then
    PRESTO_RESTARTS=$(echo "${PRESTO_INFO}" | awk '{print $4}' | sed -E 's/\(.*\)//')
    if [[ "${PRESTO_RESTARTS:-0}" =~ ^[0-9]+$ ]] && [[ "${PRESTO_RESTARTS}" -gt 2 ]]; then
        warn "Presto pod has restarted ${PRESTO_RESTARTS} times (likely OOMKilled — bump podman machine memory)"
        info "  Current podman machine memory: $(podman machine inspect ${PODMAN_MACHINE} --format '{{.Resources.Memory}}' 2>/dev/null) MB"
        info "  Recommended: 20480 MB or higher. Stop machine, podman machine set ${PODMAN_MACHINE} --memory 20480, start."
    else
        ok "Presto pod stable (restarts: ${PRESTO_RESTARTS})"
    fi
fi

# ---------------- LEVEL 3: Port-forwards ----------------
hr "L3. Port-forwards"

declare -a PORT_PAIRS=(
    "lhconsole-ui-svc 6443 443"
    "ibm-lh-presto-svc 8443 8443"
    "ibm-lh-minio-svc 9000 9000"
    "ibm-lh-minio-svc 9001 9001"
    "ibm-lh-mds-thrift-svc 8381 8381"
)

forward_port() {
    local svc="$1" lport="$2" rport="$3"
    local pids
    pids=$(pgrep -f "kubectl port-forward.*${svc}.*${lport}:" || true)
    [[ -n "${pids}" ]] && kill ${pids} 2>/dev/null && sleep 1
    nohup kubectl port-forward -n "${NS}" "service/${svc}" "${lport}:${rport}" \
        --address 0.0.0.0 >/dev/null 2>&1 &
    disown
}

if [[ "$CHECK_ONLY" -eq 1 ]]; then
    for pair in "${PORT_PAIRS[@]}"; do
        read -r svc lport rport <<<"$pair"
        if lsof -nP -iTCP:${lport} -sTCP:LISTEN >/dev/null 2>&1; then
            ok "localhost:${lport} listening (${svc})"
        else
            warn "localhost:${lport} not listening (${svc})"
        fi
    done
else
    fix "refreshing all port-forwards..."
    for pair in "${PORT_PAIRS[@]}"; do
        read -r svc lport rport <<<"$pair"
        forward_port "${svc}" "${lport}" "${rport}"
    done
    sleep 3
    for pair in "${PORT_PAIRS[@]}"; do
        read -r svc lport rport <<<"$pair"
        if lsof -nP -iTCP:${lport} -sTCP:LISTEN >/dev/null 2>&1; then
            ok "localhost:${lport} (${svc})"
        else
            warn "localhost:${lport} (${svc}) -- not listening yet, port-forward may still be starting"
        fi
    done
fi

# ---------------- LEVEL 4: Host -> service reachability ----------------
hr "L4. Host -> service reachability"

# Presto with retry (port-forward may still be settling)
PRESTO_REACHABLE=0
for i in 1 2 3 4; do
    if INFO=$(curl -sk -u ibmlhadmin:password --max-time 8 ${PRESTO_URL}/v1/info 2>/dev/null); then
        if echo "${INFO}" | grep -q nodeVersion; then
            PRESTO_REACHABLE=1
            UPTIME=$(echo "${INFO}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uptime','?'))" 2>/dev/null || echo "?")
            ok "Presto: uptime=${UPTIME}"
            break
        fi
    fi
    [[ $i -lt 4 ]] && info "  Presto not yet reachable (${i}/4), retrying..."
    sleep 4
done
[[ "${PRESTO_REACHABLE}" -eq 0 ]] && fail "Presto NOT reachable at ${PRESTO_URL} after retries"

# MinIO
if [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 ${MINIO_URL}/minio/health/live 2>/dev/null)" == "200" ]]; then
    ok "MinIO health endpoint responsive"
else
    fail "MinIO NOT reachable at ${MINIO_URL}"
fi

# CockroachDB
if cockroach sql --insecure --url "${CRDB_URL}" -e "SELECT 1;" >/dev/null 2>&1; then
    ok "CockroachDB responsive at localhost:26257"
else
    fail "CockroachDB NOT reachable -- start it (your local CRDB script or docker compose up -d cockroachdb)"
fi

# ---------------- LEVEL 5: In-cluster connectivity ----------------
hr "L5. In-cluster connectivity (the path Query workspace uses)"

IN_CLUSTER=$(kubectl -n "${NS}" exec deployment/lhconsole-ui -- \
    curl -sk --max-time 5 -o /dev/null -w "%{http_code}" \
    https://ibm-lh-presto-svc:8443/v1/info 2>/dev/null || echo "fail")
if [[ "${IN_CLUSTER}" == "401" || "${IN_CLUSTER}" == "200" ]]; then
    ok "lhconsole-ui -> Presto reachable (HTTP ${IN_CLUSTER})"
elif [[ "${IN_CLUSTER}" == "fail" || "${IN_CLUSTER}" == "000" ]]; then
    warn "lhconsole-ui CANNOT reach Presto in-cluster -- queries from the wxd UI will get ECONNREFUSED"
    info "  Probably Presto is restarting. Wait 60s and re-run preflight."
fi

# ---------------- LEVEL 6: Data plane (catalog + schema + source data) ----------------
hr "L6. Data plane (catalog + schema + source data)"

# Combined Presto probe with retries
PRESTO_PROBE=$(python3 - <<'PYEOF' 2>/dev/null
import requests, urllib3, time, json
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
AUTH=('ibmlhadmin','password'); URL='https://localhost:8443/v1/statement'
H={'X-Presto-User':'ibmlhadmin','Content-Type':'text/plain'}
def q(sql, attempts=4):
    for i in range(attempts):
        try:
            r = requests.post(URL,headers=H,data=sql,auth=AUTH,verify=False,timeout=20)
            j = r.json(); rows=[]
            while True:
                if j.get('data'): rows.extend(j['data'])
                nxt=j.get('nextUri'); state=j.get('stats',{}).get('state')
                if not nxt or state in ('FINISHED','FAILED'): break
                time.sleep(0.3)
                j = requests.get(nxt,auth=AUTH,verify=False,timeout=20).json()
            if state == 'FINISHED': return rows
        except Exception:
            time.sleep(2 + i*2)
    return None
catalogs = q('SHOW CATALOGS')
schemas  = q("SELECT schema_name FROM iceberg_data.information_schema.schemata WHERE schema_name='banko'")
table    = q("SELECT table_name FROM iceberg_data.banko.information_schema.tables WHERE table_name='expenses'") if schemas else None
count    = q('SELECT COUNT(*) FROM iceberg_data.banko.expenses') if table else None
print(json.dumps({
    'catalogs_ok':       catalogs is not None,
    'has_cockroachdb':   catalogs is not None and any(r[0]=='cockroachdb' for r in catalogs),
    'has_banko_schema':  schemas is not None and len(schemas) > 0,
    'has_expenses_table': table is not None and len(table) > 0,
    'iceberg_rows':      count[0][0] if count else None,
}))
PYEOF
)

HAS_CRDB_CATALOG=$(echo "${PRESTO_PROBE}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['has_cockroachdb'])" 2>/dev/null || echo "False")
HAS_SCHEMA=$(echo "${PRESTO_PROBE}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['has_banko_schema'])" 2>/dev/null || echo "False")
HAS_TABLE=$(echo "${PRESTO_PROBE}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['has_expenses_table'])" 2>/dev/null || echo "False")
ICEBERG_ROWS=$(echo "${PRESTO_PROBE}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['iceberg_rows'] if d['iceberg_rows'] is not None else '?')" 2>/dev/null || echo "?")

if [[ "${HAS_CRDB_CATALOG}" == "True" ]]; then
    ok "Federation catalog 'cockroachdb' visible in Presto"
else
    warn "Federation catalog 'cockroachdb' NOT in SHOW CATALOGS"
    info "  This is a one-time UI step (cannot be auto-fixed reliably)."
    info "  Open https://localhost:6443/ -> Infrastructure manager -> Add database (PostgreSQL):"
    info "    Hostname: host.docker.internal | Port: 26257 | DB: defaultdb | User: root | (no password) | SSL off"
    info "  Associate with engine 'presto-01'."
fi

# Schema + table — auto-create if missing
if [[ "${HAS_SCHEMA}" != "True" || "${HAS_TABLE}" != "True" ]]; then
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "Iceberg schema/table missing (banko.expenses)"
    else
        fix "creating Iceberg schema + table (banko.expenses)..."
        python3 - <<'PYEOF' 2>&1 | sed 's/^/     /'
import requests, urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
AUTH=('ibmlhadmin','password'); URL='https://localhost:8443/v1/statement'
H={'X-Presto-User':'ibmlhadmin','Content-Type':'text/plain'}
def run(sql):
    r=requests.post(URL,headers=H,data=sql,auth=AUTH,verify=False,timeout=60); j=r.json()
    while True:
        nxt=j.get('nextUri'); state=j.get('stats',{}).get('state')
        if not nxt or state in ('FINISHED','FAILED'): break
        time.sleep(0.3); j=requests.get(nxt,auth=AUTH,verify=False,timeout=60).json()
    return j.get('stats',{}).get('state','?')
for stmt in [
    "CREATE SCHEMA IF NOT EXISTS iceberg_data.banko WITH (location='s3a://iceberg-bucket/banko')",
    """CREATE TABLE IF NOT EXISTS iceberg_data.banko.expenses (
        expense_id VARCHAR, user_id VARCHAR, description VARCHAR, merchant VARCHAR,
        expense_amount DOUBLE, expense_date VARCHAR, shopping_type VARCHAR,
        payment_method VARCHAR, recurring BOOLEAN, cdc_table VARCHAR,
        cdc_operation VARCHAR, cdc_timestamp VARCHAR
    ) WITH (format='PARQUET', partitioning=ARRAY['shopping_type'])""",
]:
    print(("✅" if run(stmt) == "FINISHED" else "❌"), stmt[:55])
PYEOF
    fi
else
    ok "Iceberg schema + table present (banko.expenses, ${ICEBERG_ROWS} CDC rows)"
fi

# Source data
CRDB_ROWS=$(cockroach sql --insecure --url "${CRDB_URL}" --format=csv \
    -e "SELECT COUNT(*) FROM expenses;" 2>/dev/null | tail -1)
if [[ "${CRDB_ROWS:-0}" -gt 0 ]]; then
    ok "CockroachDB.expenses: ${CRDB_ROWS} rows (live OLTP source)"
else
    warn "CockroachDB.expenses is empty or missing"
    info "  Load it via the Banko AI Assistant: https://github.com/cockroachlabs-field/banko-ai-assistant"
fi

# ---------------- LEVEL 7: Pipeline ----------------
hr "L7. Pipeline"

PIPELINE_OK=0
if STATS=$(curl -sk --max-time 5 ${PIPELINE_URL}/cdc/stats 2>/dev/null); then
    if echo "${STATS}" | grep -q total_events; then
        PIPELINE_OK=1
        ok "Pipeline alive: ${STATS}"
    fi
fi

if [[ "${PIPELINE_OK}" -eq 0 ]]; then
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "Pipeline NOT running (start: source ${ENV_FILE} && uv run crdb-wxd-pipeline webhook)"
    elif [[ ! -f "${ENV_FILE}" ]]; then
        fail "${ENV_FILE} not found at $(pwd) -- can't auto-start pipeline"
    else
        fix "starting pipeline in background (logs: ${PIPELINE_LOG})..."
        # Run via nohup so it survives this script exiting
        (
            set -a
            # shellcheck disable=SC1090
            source "${ENV_FILE}"
            set +a
            nohup uv run crdb-wxd-pipeline webhook > "${PIPELINE_LOG}" 2>&1 &
            disown
        )
        # Wait for the pipeline to come up
        for i in 1 2 3 4 5 6; do
            sleep 3
            if curl -sk --max-time 3 ${PIPELINE_URL}/cdc/stats 2>/dev/null | grep -q total_events; then
                ok "pipeline started"; PIPELINE_OK=1; break
            fi
            info "  waiting for pipeline (${i}/6)..."
        done
        [[ "${PIPELINE_OK}" -eq 0 ]] && warn "pipeline did not respond in 18s -- check ${PIPELINE_LOG}"
    fi
fi

# ---------------- LEVEL 8: Changefeed ----------------
hr "L8. Changefeed"

CF_RUNNING=$(cockroach sql --insecure --url "${CRDB_URL}" --format=csv -e "
    SELECT COUNT(*) FROM [SHOW JOBS]
    WHERE job_type='CHANGEFEED' AND status='running';
" 2>/dev/null | tail -1)
CF_RUNNING=${CF_RUNNING:-0}

if [[ "${CF_RUNNING}" -ge 1 ]]; then
    ok "Changefeed running (${CF_RUNNING} active)"
else
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "No active changefeed (recreate via: see runbook)"
    elif [[ "${PIPELINE_OK}" -eq 0 ]]; then
        warn "skipping changefeed creation -- pipeline isn't reachable"
    else
        fix "creating changefeed (sink: webhook-https://host.docker.internal:5002/cdc/events)..."
        cockroach sql --insecure --url "${CRDB_URL}" -e "
            SET CLUSTER SETTING kv.rangefeed.enabled = true;
            CREATE CHANGEFEED FOR TABLE expenses
            INTO 'webhook-https://host.docker.internal:5002/cdc/events?insecure_tls_skip_verify=true'
            WITH updated, diff, resolved = '15s', min_checkpoint_frequency = '5s';
        " 2>&1 | tail -3 | sed 's/^/     /'
        # Verify
        sleep 2
        CF_NEW=$(cockroach sql --insecure --url "${CRDB_URL}" --format=csv -e "
            SELECT COUNT(*) FROM [SHOW JOBS] WHERE job_type='CHANGEFEED' AND status='running';
        " 2>/dev/null | tail -1)
        [[ "${CF_NEW:-0}" -ge 1 ]] && ok "changefeed created" || warn "changefeed creation may have failed"
    fi
fi

# ---------------- Summary ----------------
hr "SUMMARY"
if [[ "${EXIT}" -eq 0 ]]; then
    echo "  🟢 READY TO DEMO"
    if [[ ${#SUMMARY_LINES[@]} -gt 0 ]]; then
        echo
        echo "  Auto-fixes applied:"
        for l in "${SUMMARY_LINES[@]}"; do echo "    $l"; done
    fi
    echo
    echo "  Next: open https://localhost:6443/  ->  Query workspace  ->  presto-01"
    echo "         then run sections from sql/demo-federation.sql"
elif [[ "${EXIT}" -eq 1 ]]; then
    echo "  🟡 USABLE WITH WARNINGS"
    echo
    for l in "${SUMMARY_LINES[@]}"; do echo "    $l"; done
else
    echo "  🔴 NOT READY -- manual action required"
    echo
    for l in "${SUMMARY_LINES[@]}"; do echo "    $l"; done
fi
exit "${EXIT}"
