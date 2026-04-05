"""
CockroachDB CDC Webhook Receiver.

Receives changefeed events via HTTP POST from CockroachDB webhook sink:
  CREATE CHANGEFEED FOR expenses
  INTO 'webhook-https://<host>:<port>/cdc/events?insecure_tls_skip_verify=true'
  WITH updated, diff;
"""

from flask import Blueprint, jsonify, request

from .processor import CDCProcessor

cdc_blueprint = Blueprint("cdc", __name__, url_prefix="/cdc")

_processor: CDCProcessor | None = None


def init_webhook(processor: CDCProcessor) -> Blueprint:
    """Initialize the webhook blueprint with a processor."""
    global _processor
    _processor = processor
    return cdc_blueprint


@cdc_blueprint.route("/events", methods=["POST"])
def receive_events():
    """Receive CDC events from CockroachDB webhook changefeed."""
    if _processor is None:
        return jsonify({"error": "Pipeline not initialized"}), 503

    auth_token = _processor.config.webhook_auth_token
    if auth_token:
        header = request.headers.get("Authorization", "")
        if header != f"Bearer {auth_token}":
            return jsonify({"error": "Unauthorized"}), 401

    try:
        data = request.get_json(force=True)

        if isinstance(data, list):
            for event in data:
                _processor.process_event(event)
        elif isinstance(data, dict):
            if "payload" in data and isinstance(data["payload"], list):
                for payload_item in data["payload"]:
                    _processor.process_event(payload_item)
            else:
                _processor.process_event(data)
        else:
            return jsonify({"error": "Invalid payload format"}), 400

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"❌ Webhook error: {e}")
        return jsonify({"error": str(e)}), 500


@cdc_blueprint.route("/resolved", methods=["POST"])
def receive_resolved():
    """Handle resolved timestamp messages from CockroachDB."""
    if _processor:
        _processor.flush()
    return jsonify({"status": "ok"}), 200


@cdc_blueprint.route("/stats", methods=["GET"])
def pipeline_stats():
    """Return pipeline statistics."""
    if _processor is None:
        return jsonify({"error": "Pipeline not initialized"}), 503
    return jsonify(_processor.stats), 200
