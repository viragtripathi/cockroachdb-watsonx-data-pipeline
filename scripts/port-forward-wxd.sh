#!/usr/bin/env bash
# Port-forward local watsonx.data Developer Edition services to localhost.
#
# What this exposes (in addition to the install-time UI/MinIO-UI/MDS forwards):
#   - Presto REST API:    https://localhost:8443  (basic auth: ibmlhadmin / password)
#   - MinIO S3 API:       http://localhost:9000   (creds: dummyvalue / dummyvalue)
#
# The pipeline writes Parquet files to MinIO and INSERTs them into Iceberg
# tables via Presto, so both port-forwards are required when running the
# pipeline on the host against a kind-based watsonx.data cluster.
#
# Idempotent: kills any existing port-forwards on the same ports first.

set -euo pipefail

NAMESPACE="${WXD_NAMESPACE:-wxd}"

forward() {
    local svc="$1"
    local port="$2"

    # Kill any existing kubectl port-forward bound to this port
    local pids
    pids="$(pgrep -f "kubectl port-forward.*${svc}.*${port}:" || true)"
    if [[ -n "${pids}" ]]; then
        echo "  Stopping existing port-forward for ${svc}:${port}..."
        # shellcheck disable=SC2086
        kill ${pids} 2>/dev/null || true
        sleep 1
    fi

    echo "  Starting port-forward: localhost:${port} -> ${svc}:${port}"
    nohup kubectl port-forward -n "${NAMESPACE}" "service/${svc}" \
        "${port}:${port}" --address 0.0.0.0 > /dev/null 2>&1 &
    disown
}

if ! command -v kubectl >/dev/null 2>&1; then
    echo "❌ kubectl not found in PATH"; exit 1
fi
if ! kubectl -n "${NAMESPACE}" get svc ibm-lh-presto-svc >/dev/null 2>&1; then
    echo "❌ namespace '${NAMESPACE}' does not contain ibm-lh-presto-svc."
    echo "   Set WXD_NAMESPACE if your install uses a different namespace."
    exit 1
fi

echo "Port-forwarding watsonx.data Developer Edition services (namespace=${NAMESPACE})..."
forward ibm-lh-presto-svc 8443
forward ibm-lh-minio-svc  9000

# Give port-forwards a moment to bind, then verify
sleep 2
echo ""
echo "Verifying:"
for port in 8443 9000; do
    if lsof -nP -iTCP:${port} -sTCP:LISTEN >/dev/null 2>&1; then
        echo "  ✅ localhost:${port} listening"
    else
        echo "  ⚠️  localhost:${port} not yet listening (may still be starting)"
    fi
done
echo ""
echo "Ready. Now run:  source .env.local && uv run crdb-wxd-pipeline webhook"
