"""
CDC event processor -- common logic for both webhook and Kafka paths.

Batches CDC events, converts to Parquet, and writes to IBM COS.
"""

import io
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig

EXPENSES_SCHEMA = pa.schema([
    ("expense_id", pa.string()),
    ("user_id", pa.string()),
    ("description", pa.string()),
    ("merchant", pa.string()),
    ("expense_amount", pa.float64()),
    ("expense_date", pa.string()),
    ("shopping_type", pa.string()),
    ("payment_method", pa.string()),
    ("recurring", pa.bool_()),
    ("cdc_operation", pa.string()),
    ("cdc_timestamp", pa.string()),
])


class CDCProcessor:
    """Batches CDC events and flushes them as Parquet to a sink."""

    def __init__(self, config: PipelineConfig, sink_fn: Callable[[bytes, str], None], presto_writer=None):
        self.config = config
        self.sink_fn = sink_fn
        self.presto_writer = presto_writer
        self._batch: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()
        self._total_events = 0
        self._total_flushes = 0

        self._timer_running = True
        self._timer = threading.Thread(target=self._flush_timer, daemon=True)
        self._timer.start()

    def process_event(self, event: dict) -> None:
        """Process a single CDC event (from webhook or Kafka)."""
        row = self._normalize_event(event)
        if not row:
            return

        with self._lock:
            self._batch.append(row)
            self._total_events += 1
            if len(self._batch) >= self.config.batch_size:
                self._flush_batch()

    def _normalize_event(self, event: dict) -> dict | None:
        """Normalize a CDC event into a flat row dict. Handles both webhook and Debezium formats."""
        try:
            # CockroachDB webhook changefeed: batched payload
            if "payload" in event and isinstance(event["payload"], list):
                for payload in event["payload"]:
                    after = payload.get("after")
                    if after:
                        return self._extract_row(after, "insert", payload.get("updated", ""))
                return None

            # CockroachDB webhook: single event with before/after
            if "after" in event:
                op = "delete" if event["after"] is None else ("update" if event.get("before") else "insert")
                if event["after"]:
                    return self._extract_row(event["after"], op, event.get("updated", ""))
                elif event.get("before") and op == "delete":
                    return self._extract_row(event["before"], "delete", event.get("updated", ""))
                return None

            # Debezium envelope format
            if "payload" in event and isinstance(event["payload"], dict):
                payload = event["payload"]
                op_map = {"c": "insert", "u": "update", "d": "delete", "r": "snapshot"}
                op = op_map.get(payload.get("op", ""), "unknown")
                after = payload.get("after")
                ts = payload.get("ts_ms", "")
                if after:
                    return self._extract_row(after, op, str(ts))
                return None

            # Direct row (initial scan / snapshot)
            if "expense_id" in event:
                return self._extract_row(event, "snapshot", "")

            return None
        except Exception as e:
            print(f"⚠️  Failed to normalize CDC event: {e}")
            return None

    @staticmethod
    def _extract_row(row: dict, operation: str, timestamp: str) -> dict:
        """Extract expense fields from a row dict."""
        return {
            "expense_id": str(row.get("expense_id", "")),
            "user_id": str(row.get("user_id", "")),
            "description": row.get("description", ""),
            "merchant": row.get("merchant", ""),
            "expense_amount": float(row.get("expense_amount", 0)),
            "expense_date": str(row.get("expense_date", "")),
            "shopping_type": row.get("shopping_type", ""),
            "payment_method": row.get("payment_method", ""),
            "recurring": bool(row.get("recurring", False)),
            "cdc_operation": operation,
            "cdc_timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        }

    def _flush_batch(self) -> None:
        """Flush current batch as Parquet to the sink. Caller must hold self._lock."""
        if not self._batch:
            return

        batch = self._batch.copy()
        self._batch.clear()
        self._last_flush = time.monotonic()

        try:
            table = pa.Table.from_pylist(batch, schema=EXPENSES_SCHEMA)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            parquet_bytes = buf.getvalue()

            now = datetime.now(timezone.utc)
            object_key = (
                f"{self.config.cos_prefix}"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"expenses_{now.strftime('%Y%m%dT%H%M%S')}_{self._total_flushes:06d}.parquet"
            )

            self.sink_fn(parquet_bytes, object_key)
            self._total_flushes += 1
            print(f"✅ Flushed {len(batch)} events -> {object_key} ({len(parquet_bytes)} bytes)")

            if self.presto_writer:
                self.presto_writer.insert_batch(batch)

        except Exception as e:
            print(f"❌ Flush failed ({len(batch)} events lost): {e}")

    def _flush_timer(self) -> None:
        """Background thread that flushes on timeout."""
        while self._timer_running:
            time.sleep(5)
            with self._lock:
                elapsed = time.monotonic() - self._last_flush
                if self._batch and elapsed >= self.config.batch_timeout_seconds:
                    print(f"⏱️  Batch timeout ({elapsed:.0f}s), flushing {len(self._batch)} events")
                    self._flush_batch()

    def flush(self) -> None:
        """Force flush any pending events."""
        with self._lock:
            self._flush_batch()

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "total_events": self._total_events,
                "total_flushes": self._total_flushes,
                "pending_batch": len(self._batch),
            }

    def stop(self) -> None:
        """Stop the processor and flush remaining events."""
        self._timer_running = False
        self.flush()
