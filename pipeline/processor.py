"""
CDC event processor -- common logic for both webhook and Kafka paths.

Batches CDC events, converts to Parquet, and writes to IBM COS.
Supports arbitrary table schemas (expenses, TPC-C, or any CockroachDB table).
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
    ("cdc_table", pa.string()),
    ("cdc_operation", pa.string()),
    ("cdc_timestamp", pa.string()),
])

# Columns appended to every CDC row regardless of source table
CDC_META_FIELDS = ["cdc_table", "cdc_operation", "cdc_timestamp"]


def _infer_arrow_type(value) -> pa.DataType:
    """Map a Python value to a PyArrow type."""
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    return pa.string()


class CDCProcessor:
    """Batches CDC events and flushes them as Parquet to a sink.

    Supports two modes:
      - Legacy (default): hardcoded expenses schema, single table
      - Generic: dynamic schema inferred from row data, multi-table
    """

    def __init__(self, config: PipelineConfig, sink_fn: Callable[[bytes, str], None], presto_writer=None):
        self.config = config
        self.sink_fn = sink_fn
        self.presto_writer = presto_writer
        # Per-table batches: {"expenses": [...], "order": [...], ...}
        self._batches: dict[str, list[dict]] = {}
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

        table_name = row.get("cdc_table", "expenses")
        with self._lock:
            self._batches.setdefault(table_name, []).append(row)
            self._total_events += 1
            if len(self._batches[table_name]) >= self.config.batch_size:
                self._flush_table(table_name)

    def _normalize_event(self, event: dict) -> dict | None:
        """Normalize a CDC event into a flat row dict. Handles both webhook and Debezium formats."""
        try:
            # CockroachDB webhook changefeed: batched payload
            if "payload" in event and isinstance(event["payload"], list):
                for payload in event["payload"]:
                    after = payload.get("after")
                    if after:
                        table = event.get("__table__", self._detect_table(after))
                        return self._extract_row(after, "insert", payload.get("updated", ""), table)
                return None

            # CockroachDB webhook: single event with before/after
            if "after" in event:
                op = "delete" if event["after"] is None else ("update" if event.get("before") else "insert")
                data = event["after"] if event["after"] else event.get("before")
                table = event.get("__table__", self._detect_table(data) if data else "unknown")
                if event["after"]:
                    return self._extract_row(event["after"], op, event.get("updated", ""), table)
                elif event.get("before") and op == "delete":
                    return self._extract_row(event["before"], "delete", event.get("updated", ""), table)
                return None

            # Debezium envelope format
            if "payload" in event and isinstance(event["payload"], dict):
                payload = event["payload"]
                op_map = {"c": "insert", "u": "update", "d": "delete", "r": "snapshot"}
                op = op_map.get(payload.get("op", ""), "unknown")
                after = payload.get("after")
                before = payload.get("before")
                ts = payload.get("ts_ms", "")
                source = payload.get("source", {})
                table = source.get("table", self._detect_table(after or before or {}))
                data = after or before
                if data:
                    return self._extract_row(data, op, str(ts), table)
                return None

            # Direct row (initial scan / snapshot)
            table = event.pop("__table__", self._detect_table(event))
            return self._extract_row(event, "snapshot", "", table)

        except Exception as e:
            print(f"⚠️  Failed to normalize CDC event: {e}")
            return None

    @staticmethod
    def _detect_table(row: dict) -> str:
        """Detect table name from row keys."""
        if "expense_id" in row:
            return "expenses"
        if "o_id" in row and "o_c_id" in row:
            return "order"
        if "ol_o_id" in row:
            return "order_line"
        if "no_o_id" in row:
            return "new_order"
        if "c_id" in row and "c_w_id" in row:
            return "customer"
        if "d_id" in row and "d_w_id" in row and "d_name" in row:
            return "district"
        if "w_id" in row and "w_name" in row:
            return "warehouse"
        if "s_i_id" in row and "s_w_id" in row:
            return "stock"
        if "i_id" in row and "i_name" in row:
            return "item"
        if "h_c_id" in row:
            return "history"
        return "unknown"

    @staticmethod
    def _extract_row(row: dict, operation: str, timestamp: str, table: str = "expenses") -> dict:
        """Extract all fields from a row, casting values to strings/floats as needed,
        and append CDC metadata columns."""
        ts = timestamp or datetime.now(timezone.utc).isoformat()

        if table == "expenses":
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
                "cdc_table": table,
                "cdc_operation": operation,
                "cdc_timestamp": ts,
            }

        # Generic: preserve all fields, cast to safe types
        result = {}
        for k, v in row.items():
            if v is None:
                result[k] = ""
            elif isinstance(v, bool):
                result[k] = v
            elif isinstance(v, (int, float)):
                result[k] = v
            else:
                result[k] = str(v)
        result["cdc_table"] = table
        result["cdc_operation"] = operation
        result["cdc_timestamp"] = ts
        return result

    def _flush_table(self, table_name: str) -> None:
        """Flush a single table's batch as Parquet. Caller must hold self._lock."""
        batch = self._batches.get(table_name)
        if not batch:
            return

        rows = batch.copy()
        batch.clear()
        self._last_flush = time.monotonic()

        try:
            if table_name == "expenses":
                arrow_table = pa.Table.from_pylist(rows, schema=EXPENSES_SCHEMA)
            else:
                arrow_table = self._build_arrow_table(rows)

            buf = io.BytesIO()
            pq.write_table(arrow_table, buf, compression="snappy")
            parquet_bytes = buf.getvalue()

            now = datetime.now(timezone.utc)
            prefix = self.config.cos_prefix.rstrip("/")
            # Use table-specific prefix: cdc/expenses/... or cdc/tpcc/order/...
            if table_name == "expenses":
                obj_prefix = prefix
            else:
                obj_prefix = prefix.replace("expenses", f"tpcc/{table_name}")
            object_key = (
                f"{obj_prefix}/"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
                f"{table_name}_{now.strftime('%Y%m%dT%H%M%S')}_{self._total_flushes:06d}.parquet"
            )

            self.sink_fn(parquet_bytes, object_key)
            self._total_flushes += 1
            print(f"✅ Flushed {len(rows)} {table_name} events -> {object_key} ({len(parquet_bytes)} bytes)")

            if self.presto_writer:
                self.presto_writer.insert_batch(rows, table_name)

        except Exception as e:
            print(f"❌ Flush failed for {table_name} ({len(rows)} events lost): {e}")

    @staticmethod
    def _build_arrow_table(rows: list[dict]) -> pa.Table:
        """Build a PyArrow table from rows with dynamically inferred schema."""
        if not rows:
            return pa.table({})
        # Use first row to infer types, then cast all values
        sample = rows[0]
        fields = []
        for k, v in sample.items():
            fields.append(pa.field(k, _infer_arrow_type(v)))
        schema = pa.schema(fields)
        # Cast all values to strings for safety (numeric columns stay as-is)
        columns = {f.name: [] for f in schema}
        for row in rows:
            for f in schema:
                val = row.get(f.name)
                if val is None:
                    val = "" if f.type == pa.string() else (0 if f.type in (pa.int64(), pa.float64()) else False)
                columns[f.name].append(val)
        arrays = [pa.array(columns[f.name], type=f.type) for f in schema]
        return pa.Table.from_arrays(arrays, schema=schema)

    def _flush_timer(self) -> None:
        """Background thread that flushes on timeout."""
        while self._timer_running:
            time.sleep(5)
            with self._lock:
                elapsed = time.monotonic() - self._last_flush
                if elapsed >= self.config.batch_timeout_seconds:
                    total_pending = sum(len(b) for b in self._batches.values())
                    if total_pending:
                        print(f"⏱️  Batch timeout ({elapsed:.0f}s), flushing {total_pending} events")
                        for tbl in list(self._batches.keys()):
                            self._flush_table(tbl)

    def flush(self) -> None:
        """Force flush any pending events."""
        with self._lock:
            for tbl in list(self._batches.keys()):
                self._flush_table(tbl)

    @property
    def stats(self) -> dict:
        with self._lock:
            pending = sum(len(b) for b in self._batches.values())
            return {
                "total_events": self._total_events,
                "total_flushes": self._total_flushes,
                "pending_batch": pending,
            }

    def stop(self) -> None:
        """Stop the processor and flush remaining events."""
        self._timer_running = False
        self.flush()
