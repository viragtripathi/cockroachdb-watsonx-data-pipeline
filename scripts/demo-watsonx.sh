#!/usr/bin/env bash
#
# watsonx.data Demo Script
# ========================
# End-to-end demo: CockroachDB -> watsonx.data via Federation + CDC + Hybrid JOINs
#
# Run this AFTER the Banko AI demo when CockroachDB already has expense data.
#
# Prerequisites:
#   - CockroachDB running (local Docker or CockroachDB Cloud)
#   - watsonx.data instance with Presto engine
#   - CockroachDB registered as PostgreSQL datasource (catalog: cockroachdb)
#   - Iceberg catalog (iceberg_data) with banko schema configured
#   - COS bucket connected to watsonx.data
#
# Usage:
#   ./scripts/demo-watsonx.sh                         # Full demo with expenses (all 5 acts)
#   ./scripts/demo-watsonx.sh --workload tpcc         # TPC-C workload demo
#   ./scripts/demo-watsonx.sh --act 1                 # Run specific act
#   ./scripts/demo-watsonx.sh --skip-cdc              # Skip CDC pipeline (federation only)
#   ./scripts/demo-watsonx.sh --crdb-url "..."        # Custom CockroachDB URL
#
set -euo pipefail

# --- Configuration ---
CRDB_DOCKER="${CRDB_DOCKER:-crdb-source}"
CRDB_URL="${CRDB_URL:-}"
PIPELINE_URL="${PIPELINE_URL:-http://localhost:5002}"
BATCH_PAUSE="${BATCH_PAUSE:-15}"
ACT="${ACT:-all}"
SKIP_CDC="${SKIP_CDC:-false}"
WORKLOAD="${WORKLOAD:-expenses}"
TPCC_WAREHOUSES="${TPCC_WAREHOUSES:-1}"
TPCC_DURATION="${TPCC_DURATION:-60s}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

print_banner() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC}  ${BOLD}CockroachDB + IBM watsonx.data Integration Demo ${NC}            ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}  Federation | CDC to Iceberg | Hybrid JOINs                  ${CYAN}║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_act() {
    local act_num=$1
    local title=$2
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  ACT ${act_num}: ${title}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

print_step() {
    echo -e "  ${GREEN}>>>${NC} $1"
}

print_query() {
    echo -e "  ${YELLOW}SQL>${NC} $1"
}

print_note() {
    echo -e "  ${CYAN}NOTE:${NC} $1"
}

wait_for_user() {
    echo ""
    echo -e "  ${BOLD}Press ENTER to continue...${NC}"
    read -r
}

run_crdb_sql() {
    if [ -n "$CRDB_URL" ]; then
        cockroach sql --url "$CRDB_URL" --execute "$1" 2>&1 || true
    else
        docker exec "$CRDB_DOCKER" cockroach sql --insecure --execute "$1" 2>&1 || true
    fi
}

run_crdb_sql_db() {
    local db=$1
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

show_help() {
    cat <<'EOF'
CockroachDB + IBM watsonx.data Integration Demo

USAGE:
  ./scripts/demo-watsonx.sh [OPTIONS]

WORKLOADS:
  (default)          Banko expenses demo (requires Banko AI app data)
  --workload tpcc    TPC-C industry-standard OLTP benchmark

EXAMPLES:
  # Expenses demo (all acts)
  ./scripts/demo-watsonx.sh --crdb-url "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

  # Expenses demo, skip CDC (federation + CTAS only)
  ./scripts/demo-watsonx.sh --crdb-url "..." --skip-cdc

  # TPC-C demo (all acts, fully automated)
  ./scripts/demo-watsonx.sh --workload tpcc --crdb-url "postgresql://root@localhost:26257/tpcc?sslmode=disable"

  # TPC-C with 5 warehouses and 2-minute workload
  ./scripts/demo-watsonx.sh --workload tpcc --tpcc-warehouses 5 --tpcc-duration 120s

  # Run a specific act only
  ./scripts/demo-watsonx.sh --act 3
  ./scripts/demo-watsonx.sh --workload tpcc --act 3

  # Clean up before re-running
  ./scripts/cleanup.sh                       # Reset all
  ./scripts/cleanup.sh --workload tpcc       # Reset TPC-C only

OPTIONS:
  --workload TYPE       Workload type: expenses (default) or tpcc
  --act N               Run a specific act (1-5) instead of all
  --crdb-url URL        CockroachDB connection URL
  --pipeline-url URL    CDC pipeline URL (default: http://localhost:5002)
  --skip-cdc            Skip the CDC pipeline act (expenses only)
  --batch-pause SECS    Seconds to wait for batch flush (default: 15)
  --tpcc-warehouses N   TPC-C warehouse count (default: 1)
  --tpcc-duration DUR   TPC-C workload duration (default: 60s)
  -h, --help            Show this help

ACTS (expenses):
  1  Show existing OLTP data from Banko AI demo
  2  Federation: verify live connection (on-ramp to Iceberg)
  3  CTAS: materialize OLTP data into Iceberg
  4  CDC: stream inserts/updates/deletes to Iceberg
  5  Hybrid JOINs: live + CDC + snapshot in one query

ACTS (tpcc):
  1  Initialize TPC-C workload (cockroach workload init)
  2  Start CDC pipeline + create changefeed on 7 tables
  3  Run TPC-C workload (generate transactions)
  4  Query TPC-C CDC data in watsonx.data
  5  Summary + Iceberg features
EOF
    exit 0
}

# Show help if no arguments
if [[ $# -eq 0 ]]; then
    show_help
fi

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help) show_help ;;
        --act) ACT="$2"; shift 2 ;;
        --skip-cdc) SKIP_CDC=true; shift ;;
        --crdb-url) CRDB_URL="$2"; shift 2 ;;
        --pipeline-url) PIPELINE_URL="$2"; shift 2 ;;
        --batch-pause) BATCH_PAUSE="$2"; shift 2 ;;
        --workload) WORKLOAD="$2"; shift 2 ;;
        --tpcc-warehouses) TPCC_WAREHOUSES="$2"; shift 2 ;;
        --tpcc-duration) TPCC_DURATION="$2"; shift 2 ;;
        *) echo "Unknown option: $1 (use --help for usage)"; exit 1 ;;
    esac
done

# ================================================================
# ACT 1: Show existing OLTP data (from Banko demo)
# ================================================================
act1() {
    print_act 1 "CockroachDB OLTP Data (from Banko AI Demo)"

    print_step "Checking CockroachDB expenses table..."
    echo ""
    run_crdb_sql "SELECT count(*) AS total_expenses FROM expenses;"

    echo ""
    print_step "Expense breakdown by category:"
    echo ""
    run_crdb_sql "
        SELECT shopping_type,
               count(*) AS txn_count,
               sum(expense_amount)::DECIMAL(12,2) AS total_spend
        FROM expenses
        GROUP BY shopping_type
        ORDER BY total_spend DESC;
    "

    echo ""
    print_step "Top 5 merchants:"
    echo ""
    run_crdb_sql "
        SELECT merchant,
               count(*) AS txn_count,
               sum(expense_amount)::DECIMAL(12,2) AS total_spend
        FROM expenses
        GROUP BY merchant
        ORDER BY total_spend DESC
        LIMIT 5;
    "

    echo ""
    print_note "This data was created during the Banko AI assistant demo."
    print_note "Now let's connect this to watsonx.data for analytics..."
    wait_for_user
}

# ================================================================
# ACT 2: Federation - Live queries via watsonx.data
# ================================================================
act2() {
    print_act 2 "Federation: The On-Ramp to Iceberg"

    print_note "CockroachDB is registered as a PostgreSQL datasource in watsonx.data."
    print_note "Federation lets Presto see live CockroachDB data -- but that's just"
    print_note "the on-ramp. The real value is materializing data INTO Iceberg."
    echo ""

    print_step "Run in watsonx.data Query workspace:"
    echo ""

    print_query "-- Verify federation (quick sanity check)"
    echo "    SHOW CATALOGS;"
    echo "    SELECT COUNT(*) AS total_expenses FROM cockroachdb.public.expenses;"
    echo ""

    print_note "This queries CockroachDB directly through Presto -- same as any SQL client."
    print_note "For heavy analytics, we need the data in Iceberg (columnar Parquet)."
    print_note "Two ways to get it there: CTAS (next) and CDC pipeline (Act 4)."
    wait_for_user
}

# ================================================================
# ACT 3: CTAS - Materialize snapshot into Iceberg
# ================================================================
act3() {
    print_act 3 "CTAS: OLTP Data into Iceberg for Analytics"

    print_note "This is the primary use case for federation: pull CockroachDB data"
    print_note "INTO Iceberg where it becomes columnar Parquet, partitioned, and"
    print_note "optimized for heavy analytical queries -- all decoupled from OLTP."
    echo ""

    print_step "Run in watsonx.data Query workspace:"
    echo ""

    print_query "-- Drop previous snapshot if exists"
    echo "    DROP TABLE IF EXISTS iceberg_data.banko.expenses_snapshot;"
    echo ""

    print_query "-- Materialize OLTP data into Iceberg (columnar Parquet, partitioned)"
    echo "    CREATE TABLE iceberg_data.banko.expenses_snapshot"
    echo "    WITH (format = 'PARQUET', partitioning = ARRAY['shopping_type'])"
    echo "    AS"
    echo "    SELECT expense_id, user_id, description, merchant,"
    echo "           expense_amount, CAST(expense_date AS VARCHAR) AS expense_date,"
    echo "           shopping_type, payment_method, recurring,"
    echo "           CAST(CURRENT_TIMESTAMP AS VARCHAR) AS snapshot_timestamp"
    echo "    FROM cockroachdb.public.expenses;"
    echo ""

    print_query "-- Now run heavy analytics on Iceberg (zero load on CockroachDB)"
    echo "    SELECT shopping_type,"
    echo "           COUNT(*) AS transactions,"
    echo "           ROUND(SUM(expense_amount), 2) AS total_spend"
    echo "    FROM iceberg_data.banko.expenses_snapshot"
    echo "    GROUP BY shopping_type"
    echo "    ORDER BY total_spend DESC;"
    echo ""

    print_note "Data is now columnar Parquet on COS, partitioned by category."
    print_note "Analytical queries scan only relevant partitions -- fast and cheap."
    print_note "In production, schedule this hourly/daily for reporting + ML training."
    print_note ""
    print_note "But CTAS is a point-in-time snapshot. For continuous streaming"
    print_note "with full change history, we need the CDC pipeline..."
    wait_for_user
}

# ================================================================
# ACT 4: CDC Pipeline - Stream changes to Iceberg
# ================================================================
act4() {
    print_act 4 "CDC Pipeline: Stream Changes to Iceberg"

    if [ "$SKIP_CDC" = true ]; then
        print_note "Skipping CDC demo (--skip-cdc flag set)."
        print_note "To run CDC, start the pipeline and re-run without --skip-cdc."
        wait_for_user
        return
    fi

    print_note "The CDC pipeline captures every INSERT, UPDATE, and DELETE"
    print_note "as an immutable event in Iceberg -- full audit trail."
    echo ""

    # Check if pipeline is running
    print_step "Checking CDC pipeline..."
    if curl -s "$PIPELINE_URL/cdc/stats" > /dev/null 2>&1; then
        local stats
        stats=$(curl -s "$PIPELINE_URL/cdc/stats")
        echo -e "  ${GREEN}Pipeline running:${NC} $stats"
    else
        echo -e "  ${RED}Pipeline not running.${NC}"
        echo ""
        print_step "Start the pipeline in another terminal:"
        echo "    export COS_ENDPOINT=... COS_API_KEY=... COS_INSTANCE_ID=... COS_BUCKET=..."
        echo "    export PRESTO_ENGINE_HOST=... WATSONX_DATA_API_KEY=..."
        echo "    uv run crdb-wxd-pipeline webhook"
        echo ""
        print_note "Or use the Kafka/Debezium path:"
        echo "    docker compose --profile kafka --profile dashboard up -d"
        echo "    curl -X POST http://localhost:8083/connectors \\"
        echo "      -H 'Content-Type: application/json' -d @sql/debezium-connector.json"
        wait_for_user
        return
    fi

    echo ""
    print_step "Phase 1: INSERT 5 new expenses..."
    local IDS=()
    local EVENTS='['
    for i in $(seq 1 5); do
        local ID="demo-$(date +%s)-$i"
        IDS+=("$ID")
        local MERCHANTS=("Starbucks" "Whole Foods" "Uber" "Target" "Netflix")
        local CATEGORIES=("Coffee" "Groceries" "Transport" "Shopping" "Entertainment")
        local AMOUNTS=("5.50" "87.23" "25.00" "42.99" "15.99")
        local PAYMENTS=("Debit Card" "Credit Card" "Apple Pay" "Google Pay" "Debit Card")
        local IDX=$((i - 1))
        [ $i -gt 1 ] && EVENTS+=','
        EVENTS+=$(cat <<EOF
{"after": {"expense_id": "$ID", "user_id": "demo-user-1", "description": "${CATEGORIES[$IDX]} at ${MERCHANTS[$IDX]}", "merchant": "${MERCHANTS[$IDX]}", "expense_amount": ${AMOUNTS[$IDX]}, "expense_date": "$(date +%Y-%m-%d)", "shopping_type": "${CATEGORIES[$IDX]}", "payment_method": "${PAYMENTS[$IDX]}", "recurring": false}}
EOF
)
    done
    EVENTS+=']'

    local RESP
    RESP=$(curl -s -X POST "$PIPELINE_URL/cdc/events" \
        -H "Content-Type: application/json" \
        -d "$EVENTS")
    echo -e "  ${GREEN}Sent 5 inserts:${NC} $RESP"

    echo ""
    print_step "Waiting ${BATCH_PAUSE}s for batch flush..."
    sleep "$BATCH_PAUSE"

    # Updates
    print_step "Phase 2: UPDATE 2 expenses (amount correction)..."
    local UPDATE_EVENTS='['
    UPDATE_EVENTS+=$(cat <<EOF
{"before": {"expense_id": "${IDS[0]}", "expense_amount": 5.50}, "after": {"expense_id": "${IDS[0]}", "user_id": "demo-user-1", "description": "Coffee at Starbucks (added pastry)", "merchant": "Starbucks", "expense_amount": 12.75, "expense_date": "$(date +%Y-%m-%d)", "shopping_type": "Coffee", "payment_method": "Credit Card", "recurring": true}}
EOF
)
    UPDATE_EVENTS+=','
    UPDATE_EVENTS+=$(cat <<EOF
{"before": {"expense_id": "${IDS[2]}", "expense_amount": 25.00}, "after": {"expense_id": "${IDS[2]}", "user_id": "demo-user-1", "description": "Uber ride (tip added)", "merchant": "Uber", "expense_amount": 32.50, "expense_date": "$(date +%Y-%m-%d)", "shopping_type": "Transport", "payment_method": "Apple Pay", "recurring": false}}
EOF
)
    UPDATE_EVENTS+=']'

    RESP=$(curl -s -X POST "$PIPELINE_URL/cdc/events" \
        -H "Content-Type: application/json" \
        -d "$UPDATE_EVENTS")
    echo -e "  ${GREEN}Sent 2 updates:${NC} $RESP"

    echo ""
    print_step "Waiting ${BATCH_PAUSE}s for batch flush..."
    sleep "$BATCH_PAUSE"

    # Deletes
    print_step "Phase 3: DELETE 1 expense..."
    local DELETE_EVENTS
    DELETE_EVENTS=$(cat <<EOF
[{"before": {"expense_id": "${IDS[4]}", "user_id": "demo-user-1", "description": "Entertainment at Netflix", "merchant": "Netflix", "expense_amount": 15.99, "expense_date": "$(date +%Y-%m-%d)", "shopping_type": "Entertainment", "payment_method": "Debit Card", "recurring": false}, "after": null}]
EOF
)

    RESP=$(curl -s -X POST "$PIPELINE_URL/cdc/events" \
        -H "Content-Type: application/json" \
        -d "$DELETE_EVENTS")
    echo -e "  ${GREEN}Sent 1 delete:${NC} $RESP"

    echo ""
    print_step "Waiting ${BATCH_PAUSE}s for batch flush..."
    sleep "$BATCH_PAUSE"

    local FINAL_STATS
    FINAL_STATS=$(curl -s "$PIPELINE_URL/cdc/stats")
    echo -e "  ${GREEN}Pipeline stats:${NC} $FINAL_STATS"

    echo ""
    print_step "Now query the CDC history in watsonx.data:"
    echo ""

    print_query "-- Full CDC audit trail"
    echo "    SELECT expense_id, merchant, expense_amount,"
    echo "           cdc_operation, cdc_timestamp"
    echo "    FROM iceberg_data.banko.expenses"
    echo "    ORDER BY cdc_timestamp DESC;"
    echo ""

    print_query "-- CDC operation counts"
    echo "    SELECT cdc_operation, COUNT(*) AS event_count"
    echo "    FROM iceberg_data.banko.expenses"
    echo "    GROUP BY cdc_operation;"
    echo ""

    print_query "-- Audit trail for a single expense (insert -> update)"
    echo "    SELECT expense_id, description, expense_amount,"
    echo "           cdc_operation, cdc_timestamp"
    echo "    FROM iceberg_data.banko.expenses"
    echo "    WHERE expense_id = '${IDS[0]}'"
    echo "    ORDER BY cdc_timestamp;"
    echo ""

    print_query "-- Time travel: Iceberg snapshots"
    echo "    SELECT * FROM iceberg_data.banko.\"expenses\\\$snapshots\""
    echo "    ORDER BY committed_at DESC;"
    echo ""

    print_note "Each batch flush = one Iceberg snapshot. You can query any"
    print_note "previous state with: SELECT * FROM ... FOR VERSION AS OF <snapshot_id>"
    wait_for_user
}

# ================================================================
# ACT 5: Hybrid JOINs - The Killer Query
# ================================================================
act5() {
    print_act 5 "Hybrid JOINs: Live + CDC History + Snapshot"

    print_note "This is the killer demo: JOIN data across all three sources"
    print_note "in a single Presto query."
    echo ""

    print_step "Run in watsonx.data Query workspace:"
    echo ""

    print_query "-- Compare all three sources side-by-side"
    echo "    SELECT 'Live (CockroachDB)' AS source,"
    echo "           COUNT(*) AS rows,"
    echo "           ROUND(SUM(expense_amount), 2) AS total_spend"
    echo "    FROM cockroachdb.public.expenses"
    echo "    UNION ALL"
    echo "    SELECT 'CDC History (Iceberg)', COUNT(*), ROUND(SUM(expense_amount), 2)"
    echo "    FROM iceberg_data.banko.expenses"
    echo "    UNION ALL"
    echo "    SELECT 'Snapshot (Iceberg)', COUNT(*), ROUND(SUM(expense_amount), 2)"
    echo "    FROM iceberg_data.banko.expenses_snapshot;"
    echo ""

    print_query "-- JOIN live OLTP with CDC history: find amount changes"
    echo "    SELECT live.expense_id, live.merchant,"
    echo "           live.expense_amount AS current_amount,"
    echo "           cdc.expense_amount AS previous_amount,"
    echo "           ROUND(live.expense_amount - cdc.expense_amount, 2) AS difference,"
    echo "           cdc.cdc_operation, cdc.cdc_timestamp"
    echo "    FROM cockroachdb.public.expenses live"
    echo "    JOIN iceberg_data.banko.expenses cdc"
    echo "      ON CAST(live.expense_id AS VARCHAR) = cdc.expense_id"
    echo "    WHERE cdc.cdc_operation = 'update'"
    echo "    ORDER BY ABS(live.expense_amount - cdc.expense_amount) DESC"
    echo "    LIMIT 10;"
    echo ""

    print_query "-- Ghost records: deleted from OLTP but preserved in CDC"
    echo "    SELECT cdc.expense_id, cdc.merchant, cdc.expense_amount,"
    echo "           cdc.cdc_timestamp AS deleted_at"
    echo "    FROM iceberg_data.banko.expenses cdc"
    echo "    LEFT JOIN cockroachdb.public.expenses live"
    echo "      ON cdc.expense_id = CAST(live.expense_id AS VARCHAR)"
    echo "    WHERE live.expense_id IS NULL"
    echo "      AND cdc.cdc_operation = 'delete'"
    echo "    ORDER BY cdc.cdc_timestamp DESC;"
    echo ""

    print_query "-- Full lifecycle: live + CDC + snapshot in one query"
    echo "    SELECT"
    echo "        COALESCE(CAST(live.expense_id AS VARCHAR), cdc.expense_id) AS id,"
    echo "        CASE WHEN live.expense_id IS NOT NULL THEN 'Active'"
    echo "             ELSE 'Deleted' END AS status,"
    echo "        COALESCE(live.merchant, cdc.merchant) AS merchant,"
    echo "        live.expense_amount AS live_amount,"
    echo "        snap.expense_amount AS snapshot_amount,"
    echo "        COUNT(cdc.cdc_operation) AS cdc_events"
    echo "    FROM iceberg_data.banko.expenses cdc"
    echo "    LEFT JOIN cockroachdb.public.expenses live"
    echo "      ON cdc.expense_id = CAST(live.expense_id AS VARCHAR)"
    echo "    LEFT JOIN iceberg_data.banko.expenses_snapshot snap"
    echo "      ON cdc.expense_id = snap.expense_id"
    echo "    GROUP BY live.expense_id, cdc.expense_id,"
    echo "             live.merchant, cdc.merchant,"
    echo "             live.expense_amount, snap.expense_amount"
    echo "    ORDER BY cdc_events DESC"
    echo "    LIMIT 20;"
    echo ""

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${BOLD}Summary: CockroachDB (OLTP) + watsonx.data (OLAP)${NC}"
    echo ""
    echo -e "  ${GREEN}CDC to Iceberg${NC}  -- Stream every change for audit trails + streaming analytics"
    echo -e "  ${GREEN}CTAS${NC}            -- Periodic snapshots for reporting + ML training"
    echo -e "  ${GREEN}Federation${NC}      -- On-ramp: exploration, CTAS source, hybrid JOINs"
    echo -e "  ${GREEN}Hybrid JOINs${NC}   -- Combine live + historical + snapshots in one query"
    echo ""
    echo -e "  ${BOLD}Production patterns:${NC}"
    echo -e "  - CDC pipeline always running (audit trail, compliance, streaming analytics)"
    echo -e "  - CTAS scheduled hourly/daily (reporting dashboards, ML feature stores)"
    echo -e "  - Hybrid JOINs for reconciliation and cross-source analytics"
    echo -e "  - All analytics run on Iceberg -- zero impact on CockroachDB OLTP"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# ================================================================
# TPC-C WORKLOAD ACTS
# ================================================================

tpcc_act1() {
    print_act 1 "Initialize TPC-C Workload"

    print_note "TPC-C is the industry-standard OLTP benchmark."
    print_note "Simulates warehouse/order processing with 9 tables."
    echo ""

    print_step "Initializing TPC-C with ${TPCC_WAREHOUSES} warehouse(s)..."
    echo ""

    local url
    url=$(tpcc_url)
    if cockroach workload init tpcc "$url" --warehouses="${TPCC_WAREHOUSES}" --drop 2>&1 | tail -3; then
        echo ""
        echo -e "  ${GREEN}TPC-C initialized successfully.${NC}"
    else
        echo -e "  ${YELLOW}Init returned an error (may already exist, continuing).${NC}"
    fi

    echo ""
    print_step "Tables loaded:"
    echo ""
    run_crdb_sql_db tpcc "SELECT table_name, estimated_row_count FROM [SHOW TABLES FROM tpcc] ORDER BY estimated_row_count DESC;"
    echo ""
    print_note "With ${TPCC_WAREHOUSES} warehouse(s): ~600K rows of realistic OLTP data."
    wait_for_user
}

tpcc_act2() {
    print_act 2 "Start CDC Pipeline + Changefeed"

    # Check pipeline
    print_step "Checking CDC pipeline at ${PIPELINE_URL}..."
    if curl -s "${PIPELINE_URL}/cdc/stats" > /dev/null 2>&1; then
        local stats
        stats=$(curl -s "${PIPELINE_URL}/cdc/stats")
        echo -e "  ${GREEN}Pipeline running:${NC} ${stats}"
    else
        echo -e "  ${RED}Pipeline not running at ${PIPELINE_URL}.${NC}"
        echo -e "  ${YELLOW}Start it in another terminal:${NC}  uv run crdb-wxd-pipeline webhook"
        echo ""
        echo -e "  ${BOLD}Press ENTER after the pipeline is running...${NC}"
        read -r
        # Recheck
        if ! curl -s "${PIPELINE_URL}/cdc/stats" > /dev/null 2>&1; then
            echo -e "  ${RED}Still not reachable. Continuing anyway (changefeed will retry).${NC}"
        fi
    fi

    echo ""
    print_step "Enabling rangefeed and creating changefeed on 7 TPC-C tables..."
    echo ""

    run_crdb_sql_db tpcc "SET CLUSTER SETTING kv.rangefeed.enabled = true;"

    local CHANGEFEED_SQL="CREATE CHANGEFEED FOR \"order\", order_line, new_order, customer, district, stock, history INTO 'webhook-https://localhost:5002/cdc/events?insecure_tls_skip_verify=true' WITH updated, diff, resolved = '10s', min_checkpoint_frequency = '10s';"

    local result
    result=$(run_crdb_sql_db tpcc "$CHANGEFEED_SQL" 2>&1)
    if echo "$result" | grep -qi "error\|already exists"; then
        echo -e "  ${YELLOW}Changefeed may already exist or had an error:${NC}"
        echo "  ${result}" | head -3
        echo -e "  ${YELLOW}Continuing -- the pipeline will receive events if changefeed is active.${NC}"
    else
        echo -e "  ${GREEN}Changefeed created. CDC events flowing to pipeline.${NC}"
    fi

    echo ""
    print_note "7 tables streaming: order, order_line, new_order, customer,"
    print_note "district, stock, history. Pipeline auto-detects each table."
    wait_for_user
}

tpcc_act3() {
    print_act 3 "Run TPC-C Workload (Generate Transactions)"

    print_note "Running cockroach workload run tpcc for ${TPCC_DURATION}."
    print_note "This generates new orders, payments, deliveries, stock checks."
    echo ""

    print_step "Starting TPC-C workload..."
    echo ""

    local url
    url=$(tpcc_url)

    # Show pipeline stats before
    if curl -s "${PIPELINE_URL}/cdc/stats" > /dev/null 2>&1; then
        local before
        before=$(curl -s "${PIPELINE_URL}/cdc/stats")
        echo -e "  Pipeline before: ${before}"
        echo ""
    fi

    # Run workload -- tolerate errors (e.g. connection issues)
    if cockroach workload run tpcc "$url" \
        --warehouses="${TPCC_WAREHOUSES}" \
        --duration="${TPCC_DURATION}" \
        --tolerate-errors 2>&1 | tail -15; then
        echo ""
        echo -e "  ${GREEN}TPC-C workload completed.${NC}"
    else
        echo ""
        echo -e "  ${YELLOW}Workload exited with errors (this is OK -- partial data captured).${NC}"
    fi

    # Show pipeline stats after
    echo ""
    if curl -s "${PIPELINE_URL}/cdc/stats" > /dev/null 2>&1; then
        local after
        after=$(curl -s "${PIPELINE_URL}/cdc/stats")
        echo -e "  Pipeline after: ${after}"
    fi

    echo ""
    print_step "Waiting ${BATCH_PAUSE}s for final batch flush..."
    sleep "${BATCH_PAUSE}"

    if curl -s "${PIPELINE_URL}/cdc/stats" > /dev/null 2>&1; then
        local final
        final=$(curl -s "${PIPELINE_URL}/cdc/stats")
        echo -e "  Pipeline final: ${final}"
    fi

    echo ""
    print_note "CDC events have been captured. Query them in watsonx.data next."
    wait_for_user
}

tpcc_act4() {
    print_act 4 "Query TPC-C CDC Data in watsonx.data"

    print_note "All TPC-C changes are now in Iceberg. Run these in the"
    print_note "watsonx.data Query workspace (also in sql/demo-tpcc.sql)."
    echo ""

    print_query "-- CDC events by table and operation"
    echo "    SELECT cdc_table, cdc_operation, COUNT(*) AS events"
    echo "    FROM ("
    echo "      SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.\"order\""
    echo "      UNION ALL"
    echo "      SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.order_line"
    echo "      UNION ALL"
    echo "      SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.customer"
    echo "      UNION ALL"
    echo "      SELECT cdc_table, cdc_operation FROM iceberg_data.tpcc.stock"
    echo "    )"
    echo "    GROUP BY cdc_table, cdc_operation"
    echo "    ORDER BY cdc_table, events DESC;"
    echo ""

    print_query "-- Order revenue by district"
    echo "    SELECT ol_d_id AS district,"
    echo "           COUNT(*) AS line_items,"
    echo "           ROUND(SUM(CAST(ol_amount AS DOUBLE)), 2) AS revenue"
    echo "    FROM iceberg_data.tpcc.order_line"
    echo "    WHERE cdc_operation IN ('insert', 'snapshot')"
    echo "    GROUP BY ol_d_id ORDER BY revenue DESC;"
    echo ""

    print_query "-- Customer balance changes (update events from payments)"
    echo "    SELECT c_id, c_first, c_last, c_balance,"
    echo "           cdc_operation, cdc_timestamp"
    echo "    FROM iceberg_data.tpcc.customer"
    echo "    WHERE cdc_operation = 'update'"
    echo "    ORDER BY cdc_timestamp DESC LIMIT 10;"
    echo ""

    print_query "-- Low stock alerts"
    echo "    SELECT s_i_id AS item, s_quantity AS qty, s_order_cnt AS orders"
    echo "    FROM iceberg_data.tpcc.stock"
    echo "    WHERE CAST(s_quantity AS BIGINT) < 15"
    echo "    ORDER BY CAST(s_quantity AS BIGINT) LIMIT 10;"
    echo ""

    print_note "Full query set in sql/demo-tpcc.sql"
    wait_for_user
}

tpcc_act5() {
    print_act 5 "TPC-C: Summary"

    print_step "Iceberg snapshots (run in watsonx.data):"
    echo ""
    print_query "SELECT * FROM iceberg_data.tpcc.\"order\\\$snapshots\" ORDER BY committed_at DESC;"
    echo ""

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${BOLD}TPC-C CDC Pipeline Summary${NC}"
    echo ""
    echo -e "  ${GREEN}Workload${NC}    -- Industry-standard TPC-C (orders, payments, deliveries)"
    echo -e "  ${GREEN}CDC${NC}         -- 7 tables streamed to Iceberg via changefeed"
    echo -e "  ${GREEN}Analytics${NC}   -- Revenue, inventory, customer behavior on Iceberg"
    echo -e "  ${GREEN}Zero impact${NC} -- All analytics decoupled from CockroachDB OLTP"
    echo ""
    echo -e "  ${BOLD}Production value:${NC}"
    echo -e "  - Full audit trail of every order, payment, and stock change"
    echo -e "  - Time travel: query data as it was at any point in time"
    echo -e "  - Heavy analytics on Iceberg -- zero load on CockroachDB"
    echo -e "  - Works with any CockroachDB table or workload"
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# --- Main ---
print_banner

if [ "$WORKLOAD" = "tpcc" ]; then
    echo -e "  ${BOLD}Workload: TPC-C${NC}"
    echo ""
    case "$ACT" in
        1) tpcc_act1 ;;
        2) tpcc_act2 ;;
        3) tpcc_act3 ;;
        4) tpcc_act4 ;;
        5) tpcc_act5 ;;
        all)
            tpcc_act1
            tpcc_act2
            tpcc_act3
            tpcc_act4
            tpcc_act5
            ;;
        *) echo "Unknown act: $ACT (use 1-5 or all)"; exit 1 ;;
    esac
else
    case "$ACT" in
        1) act1 ;;
        2) act2 ;;
        3) act3 ;;
        4) act4 ;;
        5) act5 ;;
        all)
            act1
            act2
            act3
            [ "$SKIP_CDC" = false ] && act4
            act5
            ;;
        *) echo "Unknown act: $ACT (use 1-5 or all)"; exit 1 ;;
    esac
fi
