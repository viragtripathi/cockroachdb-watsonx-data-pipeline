#!/usr/bin/env bash
#
# Full demo cleanup -- reset everything to a clean state.
#
# Usage:
#   ./scripts/cleanup.sh                             # Clean all (expenses + TPC-C)
#   ./scripts/cleanup.sh --workload expenses         # Clean expenses only
#   ./scripts/cleanup.sh --workload tpcc             # Clean TPC-C only
#   ./scripts/cleanup.sh --crdb-url "postgresql://..." # Custom CockroachDB URL
#
# What this does:
#   CockroachDB side:
#     - Cancels active changefeeds
#     - Drops and reinitializes TPC-C database (if --workload tpcc or all)
#
#   watsonx.data side (manual -- prints SQL to run):
#     - Prints DROP TABLE statements for Iceberg tables
#     - Prints CREATE TABLE statements to recreate them
#
set -euo pipefail

CRDB_DOCKER="${CRDB_DOCKER:-crdb-source}"
CRDB_URL="${CRDB_URL:-}"
WORKLOAD="${WORKLOAD:-all}"
TPCC_WAREHOUSES="${TPCC_WAREHOUSES:-1}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

show_help() {
    cat <<'EOF'
Demo Cleanup -- reset everything to a clean state.

USAGE:
  ./scripts/cleanup.sh [OPTIONS]

EXAMPLES:
  ./scripts/cleanup.sh                             # Clean all (expenses + TPC-C)
  ./scripts/cleanup.sh --workload expenses         # Clean expenses only
  ./scripts/cleanup.sh --workload tpcc             # Clean TPC-C only
  ./scripts/cleanup.sh --crdb-url "postgresql://..." # Custom CockroachDB

OPTIONS:
  --workload TYPE       all (default), expenses, or tpcc
  --crdb-url URL        CockroachDB connection URL
  --crdb-docker NAME    Docker container name (default: crdb-source)
  --tpcc-warehouses N   Warehouses for TPC-C reinit (default: 1)
  -h, --help            Show this help
EOF
    exit 0
}

if [[ $# -eq 0 ]]; then
    show_help
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help) show_help ;;
        --workload) WORKLOAD="$2"; shift 2 ;;
        --crdb-url) CRDB_URL="$2"; shift 2 ;;
        --crdb-docker) CRDB_DOCKER="$2"; shift 2 ;;
        --tpcc-warehouses) TPCC_WAREHOUSES="$2"; shift 2 ;;
        *) echo "Unknown option: $1 (use --help for usage)"; exit 1 ;;
    esac
done

run_crdb_sql() {
    local db=${1:-defaultdb}
    local sql=$2
    if [ -n "$CRDB_URL" ]; then
        local url
        url=$(echo "$CRDB_URL" | sed "s|/[^?]*|/${db}|")
        cockroach sql --url "$url" --execute "$sql" 2>&1 || true
    else
        docker exec "$CRDB_DOCKER" cockroach sql --insecure -d "$db" --execute "$sql" 2>&1 || true
    fi
}

tpcc_url() {
    if [ -n "$CRDB_URL" ]; then
        echo "$CRDB_URL" | sed "s|/[^?]*|/tpcc|"
    else
        echo "postgresql://root@localhost:26257/tpcc?sslmode=disable"
    fi
}

echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║${NC}  ${BOLD}Demo Cleanup (workload: ${WORKLOAD})${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════╝${NC}"
echo ""

# ============================================================
# CockroachDB: Cancel changefeeds
# ============================================================
echo -e "${BOLD}--- CockroachDB: Cancel active changefeeds ---${NC}"
echo ""

if [ "$WORKLOAD" = "all" ] || [ "$WORKLOAD" = "expenses" ]; then
    echo -e "  ${GREEN}>>>${NC} Cancelling changefeeds in defaultdb..."
    JOBS=$(run_crdb_sql defaultdb "SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running';" 2>&1 | grep -E '^[0-9]+' || true)
    if [ -n "$JOBS" ]; then
        while IFS= read -r job_id; do
            echo -e "  Cancelling job ${job_id}..."
            run_crdb_sql defaultdb "CANCEL JOB ${job_id};" > /dev/null
        done <<< "$JOBS"
        echo -e "  ${GREEN}Done.${NC}"
    else
        echo -e "  ${YELLOW}No active changefeeds in defaultdb.${NC}"
    fi
fi

if [ "$WORKLOAD" = "all" ] || [ "$WORKLOAD" = "tpcc" ]; then
    echo -e "  ${GREEN}>>>${NC} Cancelling changefeeds in tpcc..."
    JOBS=$(run_crdb_sql tpcc "SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running';" 2>&1 | grep -E '^[0-9]+' || true)
    if [ -n "$JOBS" ]; then
        while IFS= read -r job_id; do
            echo -e "  Cancelling job ${job_id}..."
            run_crdb_sql tpcc "CANCEL JOB ${job_id};" > /dev/null
        done <<< "$JOBS"
        echo -e "  ${GREEN}Done.${NC}"
    else
        echo -e "  ${YELLOW}No active changefeeds in tpcc.${NC}"
    fi
fi

# ============================================================
# CockroachDB: Reset TPC-C
# ============================================================
if [ "$WORKLOAD" = "all" ] || [ "$WORKLOAD" = "tpcc" ]; then
    echo ""
    echo -e "${BOLD}--- CockroachDB: Reset TPC-C database ---${NC}"
    echo ""
    echo -e "  ${GREEN}>>>${NC} Dropping and reinitializing TPC-C (${TPCC_WAREHOUSES} warehouse(s))..."
    local_url=$(tpcc_url)
    if cockroach workload init tpcc "$local_url" --warehouses="${TPCC_WAREHOUSES}" --drop 2>&1 | tail -3; then
        echo -e "  ${GREEN}TPC-C reinitialized.${NC}"
    else
        echo -e "  ${YELLOW}TPC-C init had issues (may be OK if DB doesn't exist yet).${NC}"
    fi
fi

# ============================================================
# watsonx.data: Print Iceberg cleanup SQL
# ============================================================
echo ""
echo -e "${BOLD}--- watsonx.data: Iceberg table cleanup ---${NC}"
echo ""
echo -e "  Run these in the ${CYAN}watsonx.data Query workspace${NC}:"
echo ""

if [ "$WORKLOAD" = "all" ] || [ "$WORKLOAD" = "expenses" ]; then
    echo -e "  ${YELLOW}-- Expenses cleanup:${NC}"
    echo "  DROP TABLE IF EXISTS iceberg_data.banko.expenses;"
    echo "  DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot;"
    echo ""
    echo "  CREATE TABLE iceberg_data.banko.expenses ("
    echo "      expense_id VARCHAR, user_id VARCHAR, description VARCHAR,"
    echo "      merchant VARCHAR, expense_amount DOUBLE, expense_date VARCHAR,"
    echo "      shopping_type VARCHAR, payment_method VARCHAR, recurring BOOLEAN,"
    echo "      cdc_table VARCHAR, cdc_operation VARCHAR, cdc_timestamp VARCHAR"
    echo "  ) WITH (format = 'PARQUET', partitioning = ARRAY['shopping_type']);"
    echo ""
fi

if [ "$WORKLOAD" = "all" ] || [ "$WORKLOAD" = "tpcc" ]; then
    echo -e "  ${YELLOW}-- TPC-C cleanup:${NC}"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.warehouse;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.district;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.customer;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.\"order\";"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.order_line;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.new_order;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.item;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.stock;"
    echo "  DROP TABLE IF EXISTS iceberg_data.tpcc.history;"
    echo ""
    echo "  -- Then recreate by running sql/setup-tpcc.sql"
    echo ""
fi

echo -e "  ${CYAN}NOTE:${NC} Or copy-paste from sql/cleanup.sql (expenses)"
echo -e "  ${CYAN}NOTE:${NC} and sql/setup-tpcc.sql (TPC-C tables)"

# ============================================================
# Summary
# ============================================================
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  ${GREEN}CockroachDB:${NC} Changefeeds cancelled, TPC-C reset"
echo -e "  ${YELLOW}watsonx.data:${NC} Run the SQL above to reset Iceberg tables"
echo -e "  ${GREEN}Ready:${NC} Re-run the demo with:"
echo ""
if [ "$WORKLOAD" = "tpcc" ]; then
    echo "    ./scripts/demo-watsonx.sh --workload tpcc"
elif [ "$WORKLOAD" = "expenses" ]; then
    echo "    ./scripts/demo-watsonx.sh"
else
    echo "    ./scripts/demo-watsonx.sh                 # expenses"
    echo "    ./scripts/demo-watsonx.sh --workload tpcc # TPC-C"
fi
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
