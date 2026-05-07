#!/usr/bin/env bash
# Seed real CDC events (insert/update/delete) into the running pipeline.
# Single-command alternative to copy-pasting sql/demo-cdc-changes.sql on stage.
#
# Usage:
#   ./scripts/seed-cdc-changes.sh             # run all blocks (updates + deletes + insert)
#   ./scripts/seed-cdc-changes.sh updates     # 5 price corrections only
#   ./scripts/seed-cdc-changes.sh deletes     # 3 deletions only
#   ./scripts/seed-cdc-changes.sh insert      # 1 fresh insert with memorable merchant
#   ./scripts/seed-cdc-changes.sh --no-wait   # skip waiting for flush (default waits ~20s)

set -euo pipefail
cd "$(dirname "$0")/.."

CRDB_URL="${CRDB_URL:-postgresql://root@localhost:26257/defaultdb?sslmode=disable}"
PIPELINE_URL="${PIPELINE_URL:-https://localhost:5002}"
WAIT=1
ACTION="all"

for arg in "$@"; do
    case "$arg" in
        all|updates|deletes|insert) ACTION="$arg" ;;
        --no-wait) WAIT=0 ;;
        --help|-h)
            sed -n '2,12p' "$0"; exit 0 ;;
        *) echo "unknown arg: $arg"; exit 2 ;;
    esac
done

# Sanity: pipeline alive?
if ! curl -sk --max-time 3 "${PIPELINE_URL}/cdc/stats" >/dev/null 2>&1; then
    echo "❌ Pipeline not reachable at ${PIPELINE_URL}"
    echo "   Start it with: ./scripts/preflight-demo.sh"
    exit 2
fi

START_STATS=$(curl -sk "${PIPELINE_URL}/cdc/stats" 2>/dev/null)
START_EVENTS=$(echo "${START_STATS}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_events'])" 2>/dev/null || echo 0)

run_sql() {
    local label="$1" sql="$2"
    echo
    echo "▶ ${label}"
    if OUT=$(cockroach sql --insecure --url "${CRDB_URL}" --format=table -e "${sql}" 2>&1); then
        # Show the affected-row count line(s)
        echo "${OUT}" | grep -E "^(UPDATE|DELETE|INSERT) [0-9]+" | sed 's/^/    /'
        return 0
    else
        echo "    ❌ failed:"
        echo "${OUT}" | tail -5 | sed 's/^/      /'
        return 1
    fi
}

if [[ "${ACTION}" == "all" || "${ACTION}" == "updates" ]]; then
    run_sql "5 price corrections (cdc_operation='update')" "
        UPDATE expenses
        SET expense_amount = expense_amount * 1.15,
            description = description || ' (price correction for IBM Think demo)'
        WHERE expense_id IN (
            SELECT expense_id FROM expenses ORDER BY expense_amount DESC LIMIT 5
        );"
fi

if [[ "${ACTION}" == "all" || "${ACTION}" == "deletes" ]]; then
    run_sql "3 deletions (cdc_operation='delete', will become ghost records)" "
        DELETE FROM expenses
        WHERE expense_id IN (
            SELECT expense_id FROM expenses ORDER BY expense_amount ASC LIMIT 3
        );"
fi

if [[ "${ACTION}" == "all" || "${ACTION}" == "insert" ]]; then
    run_sql "1 fresh insert (merchant='IBM Think Demo Merchant')" "
        INSERT INTO expenses (expense_id, user_id, expense_date, expense_amount,
                              shopping_type, description, merchant, payment_method, recurring)
        VALUES (
            gen_random_uuid(),
            (SELECT user_id FROM expenses LIMIT 1),
            current_date,
            99.99,
            'Travel',
            'Demo INSERT triggered live at IBM Think',
            'IBM Think Demo Merchant',
            'Credit Card',
            false
        );"
fi

if [[ "${WAIT}" -eq 1 ]]; then
    echo
    echo "⏳ Waiting for pipeline to flush events to Iceberg..."
    for i in 1 2 3 4 5 6 7 8; do
        sleep 3
        NOW_STATS=$(curl -sk "${PIPELINE_URL}/cdc/stats" 2>/dev/null)
        NOW_EVENTS=$(echo "${NOW_STATS}" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_events'])" 2>/dev/null || echo 0)
        DELTA=$((NOW_EVENTS - START_EVENTS))
        if [[ "${DELTA}" -ge 1 ]]; then
            echo "  ✅ pipeline received ${DELTA} new event(s) after ${i}x3s"
            break
        fi
    done
fi

echo
echo "🎬 Now run your demo queries in the watsonx.data Query workspace:"
echo "    https://localhost:6443/  ->  Query workspace  ->  presto-01"
echo
echo "  -- Operation breakdown (will show insert + update + delete now)"
echo "  SELECT cdc_operation, COUNT(*) FROM iceberg_data.banko.expenses"
echo "  GROUP BY cdc_operation ORDER BY 2 DESC;"
echo
echo "  -- Hybrid JOIN: live vs original (price drift query)"
echo "  See sql/demo-federation.sql Act 5b"
