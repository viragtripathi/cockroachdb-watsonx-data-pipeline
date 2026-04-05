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
#   ./scripts/demo-watsonx.sh                    # Full demo (all 5 acts)
#   ./scripts/demo-watsonx.sh --act 1            # Run specific act
#   ./scripts/demo-watsonx.sh --skip-cdc         # Skip CDC pipeline (federation only)
#   ./scripts/demo-watsonx.sh --crdb-url "..."   # Custom CockroachDB URL
#
set -euo pipefail

# --- Configuration ---
CRDB_DOCKER="${CRDB_DOCKER:-crdb-source}"
CRDB_URL="${CRDB_URL:-}"
PIPELINE_URL="${PIPELINE_URL:-http://localhost:5002}"
BATCH_PAUSE="${BATCH_PAUSE:-15}"
ACT="${ACT:-all}"
SKIP_CDC="${SKIP_CDC:-false}"

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
    echo -e "${CYAN}║${NC}  ${BOLD}CockroachDB + IBM watsonx.data Integration Demo${NC}            ${CYAN}║${NC}"
    echo -e "${CYAN}║${NC}  Federation | CDC to Iceberg | Hybrid JOINs               ${CYAN}║${NC}"
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
        cockroach sql --url "$CRDB_URL" --execute "$1" 2>/dev/null
    else
        docker exec "$CRDB_DOCKER" cockroach sql --insecure --execute "$1" 2>/dev/null
    fi
}

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --act) ACT="$2"; shift 2 ;;
        --skip-cdc) SKIP_CDC=true; shift ;;
        --crdb-url) CRDB_URL="$2"; shift 2 ;;
        --pipeline-url) PIPELINE_URL="$2"; shift 2 ;;
        --batch-pause) BATCH_PAUSE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
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

# --- Main ---
print_banner

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
