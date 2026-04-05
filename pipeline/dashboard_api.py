"""Dashboard API endpoints for Grafana Infinity datasource.

Queries Presto/Iceberg via the PrestoWriter and returns JSON
that Grafana can consume directly.
"""

from flask import Blueprint, jsonify

dashboard_blueprint = Blueprint("dashboard", __name__, url_prefix="/cdc/dashboard")

_presto_writer = None


def init_dashboard(presto_writer) -> Blueprint:
    global _presto_writer
    _presto_writer = presto_writer
    return dashboard_blueprint


def _query_presto(sql: str) -> list[dict]:
    if _presto_writer is None:
        return []
    try:
        return _presto_writer.query(sql)
    except Exception as e:
        print(f"Dashboard query error: {e}")
        return []


@dashboard_blueprint.route("/spending-by-category", methods=["GET"])
def spending_by_category():
    rows = _query_presto("""
        SELECT shopping_type AS category,
               SUM(expense_amount) AS total,
               COUNT(*) AS txn_count
        FROM iceberg_data.banko.expenses
        WHERE cdc_operation != 'delete'
        GROUP BY shopping_type
        ORDER BY total DESC
    """)
    return jsonify(rows)


@dashboard_blueprint.route("/top-merchants", methods=["GET"])
def top_merchants():
    rows = _query_presto("""
        SELECT merchant,
               SUM(expense_amount) AS total,
               COUNT(*) AS txn_count
        FROM iceberg_data.banko.expenses
        WHERE cdc_operation != 'delete'
        GROUP BY merchant
        ORDER BY total DESC
        LIMIT 10
    """)
    return jsonify(rows)


@dashboard_blueprint.route("/cdc-operations", methods=["GET"])
def cdc_operations():
    rows = _query_presto("""
        SELECT cdc_operation AS operation,
               COUNT(*) AS event_count
        FROM iceberg_data.banko.expenses
        GROUP BY cdc_operation
    """)
    return jsonify(rows)


@dashboard_blueprint.route("/totals", methods=["GET"])
def totals():
    rows = _query_presto("""
        SELECT COUNT(*) AS total_events,
               SUM(CASE WHEN cdc_operation != 'delete'
                        THEN expense_amount ELSE 0 END) AS total_spend,
               COUNT(DISTINCT expense_id) AS unique_expenses,
               COUNT(DISTINCT merchant) AS unique_merchants
        FROM iceberg_data.banko.expenses
    """)
    return jsonify(rows if rows else [{}])


@dashboard_blueprint.route("/recent-events", methods=["GET"])
def recent_events():
    rows = _query_presto("""
        SELECT expense_id, merchant, expense_amount,
               cdc_operation, cdc_timestamp, shopping_type
        FROM iceberg_data.banko.expenses
        ORDER BY cdc_timestamp DESC
        LIMIT 20
    """)
    return jsonify(rows)


@dashboard_blueprint.route("/snapshots", methods=["GET"])
def snapshots():
    rows = _query_presto("""
        SELECT snapshot_id, parent_id, operation,
               committed_at, summary
        FROM iceberg_data.banko."expenses$snapshots"
        ORDER BY committed_at DESC
    """)
    return jsonify(rows)
