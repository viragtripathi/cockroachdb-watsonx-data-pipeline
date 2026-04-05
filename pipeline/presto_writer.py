"""Presto SQL writer for inserting CDC batches into Iceberg tables via watsonx.data Presto REST API.

Flow:
  1. Get IAM bearer token from IBM Cloud (cached, auto-refreshes)
  2. POST INSERT SQL to Presto /v1/statement
  3. Poll nextUri until FINISHED or FAILED
"""

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import PipelineConfig

IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
POLL_INTERVAL = 0.5
POLL_MAX_WAIT = 120


class PrestoWriter:
    """Inserts CDC batch rows into an Iceberg table via the Presto REST API."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._token: str | None = None
        self._token_expiry: float = 0
        self.engine_host = config.presto_engine_host
        self.catalog = config.wxd_catalog
        self.namespace = config.wxd_namespace
        self.api_key = config.wxd_api_key
        self._session = self._build_session()

        if not self.engine_host:
            raise ValueError(
                "PRESTO_ENGINE_HOST is required. "
                "Find it in watsonx.data console > Presto engine > Connection information > engine_host"
            )
        if not self.api_key:
            raise ValueError("WATSONX_DATA_API_KEY is required for Presto authentication")

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _get_iam_token(self) -> str:
        """Get or refresh an IAM bearer token."""
        if self._token and time.monotonic() < self._token_expiry:
            return self._token

        resp = self._session.post(
            IAM_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            data={
                "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
                "apikey": self.api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", 3600)
        self._token_expiry = time.monotonic() + expires_in - 120
        print(f"🔑 IAM token acquired (expires in {expires_in}s)")
        return self._token

    def insert_batch(self, rows: list[dict]) -> bool:
        """Insert a batch of CDC rows into the Iceberg table. Returns True on success."""
        if not rows:
            return True

        table = f"{self.catalog}.{self.namespace}.expenses"
        sql = self._build_insert_sql(table, rows)
        return self._execute_sql(sql, f"{len(rows)} rows into {table}")

    def _build_insert_sql(self, table: str, rows: list[dict]) -> str:
        values = []
        for r in rows:
            recurring = "true" if r.get("recurring") else "false"
            values.append(
                f"('{_esc(r['expense_id'])}', '{_esc(r['user_id'])}', "
                f"'{_esc(r['description'])}', '{_esc(r['merchant'])}', "
                f"DOUBLE '{r['expense_amount']}', '{_esc(r['expense_date'])}', "
                f"'{_esc(r['shopping_type'])}', '{_esc(r['payment_method'])}', "
                f"{recurring}, '{_esc(r['cdc_operation'])}', '{_esc(r['cdc_timestamp'])}')"
            )
        return (
            f"INSERT INTO {table} "
            "(expense_id, user_id, description, merchant, expense_amount, "
            "expense_date, shopping_type, payment_method, recurring, "
            "cdc_operation, cdc_timestamp) VALUES " + ", ".join(values)
        )

    def _execute_sql(self, sql: str, description: str) -> bool:
        """Execute a SQL statement via Presto REST API and poll until complete."""
        try:
            token = self._get_iam_token()
        except Exception as e:
            print(f"❌ Presto: IAM token error: {e}")
            return False

        presto_url = f"https://{self.engine_host}/v1/statement"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/plain",
            "X-Presto-User": "admin",
            "X-Presto-Catalog": self.catalog,
            "X-Presto-Schema": self.namespace,
        }

        try:
            resp = self._session.post(presto_url, headers=headers, data=sql, timeout=30)
            resp.raise_for_status()
            result = resp.json()
        except requests.RequestException as e:
            print(f"❌ Presto POST failed: {e}")
            return False

        return self._poll_until_done(result, token, description)

    def _poll_until_done(self, result: dict, token: str, description: str) -> bool:
        """Poll Presto nextUri until the statement reaches FINISHED or FAILED."""
        next_uri = result.get("nextUri")
        state = result.get("stats", {}).get("state", "UNKNOWN")
        query_id = result.get("id", "?")
        start = time.monotonic()

        while next_uri and state not in ("FINISHED", "FAILED"):
            if time.monotonic() - start > POLL_MAX_WAIT:
                print(f"❌ Presto query {query_id} timed out after {POLL_MAX_WAIT}s (state={state})")
                return False

            time.sleep(POLL_INTERVAL)
            try:
                poll_resp = self._session.get(
                    next_uri,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30,
                )
                poll_resp.raise_for_status()
                result = poll_resp.json()
                next_uri = result.get("nextUri")
                state = result.get("stats", {}).get("state", "UNKNOWN")
            except requests.RequestException as e:
                print(f"❌ Presto poll error for query {query_id}: {e}")
                return False

        if state == "FAILED":
            error_msg = result.get("error", {}).get("message", "Unknown error")
            error_type = result.get("error", {}).get("errorName", "")
            print(f"❌ Presto INSERT failed [{error_type}]: {error_msg}")
            return False

        elapsed = time.monotonic() - start
        print(f"✅ Presto INSERT: {description} ({elapsed:.1f}s)")
        return True

    def verify_connection(self) -> bool:
        """Run a trivial query to verify Presto connectivity."""
        try:
            token = self._get_iam_token()
        except Exception as e:
            print(f"❌ Presto verify: IAM token error: {e}")
            return False

        presto_url = f"https://{self.engine_host}/v1/statement"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/plain",
            "X-Presto-User": "admin",
            "X-Presto-Catalog": self.catalog,
            "X-Presto-Schema": self.namespace,
        }
        try:
            resp = self._session.post(
                presto_url,
                headers=headers,
                data=f"SELECT COUNT(*) FROM {self.catalog}.{self.namespace}.expenses",
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()
            state = result.get("stats", {}).get("state", "UNKNOWN")
            print(f"✅ Presto connection verified (query state: {state})")
            return True
        except Exception as e:
            print(f"❌ Presto connection failed: {e}")
            return False


    def query(self, sql: str) -> list[dict]:
        """Execute a SELECT and return rows as list of dicts."""
        try:
            token = self._get_iam_token()
        except Exception as e:
            raise RuntimeError(f"IAM token error: {e}") from e

        presto_url = f"https://{self.engine_host}/v1/statement"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "text/plain",
            "X-Presto-User": "admin",
            "X-Presto-Catalog": self.catalog,
            "X-Presto-Schema": self.namespace,
        }

        resp = self._session.post(presto_url, headers=headers, data=sql, timeout=30)
        resp.raise_for_status()
        result = resp.json()

        columns = []
        all_data = []
        next_uri = result.get("nextUri")
        if result.get("columns"):
            columns = [c["name"] for c in result["columns"]]
        if result.get("data"):
            all_data.extend(result["data"])

        while next_uri:
            time.sleep(POLL_INTERVAL)
            poll_resp = self._session.get(
                next_uri,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            poll_resp.raise_for_status()
            result = poll_resp.json()
            if result.get("columns") and not columns:
                columns = [c["name"] for c in result["columns"]]
            if result.get("data"):
                all_data.extend(result["data"])
            next_uri = result.get("nextUri")
            state = result.get("stats", {}).get("state", "UNKNOWN")
            if state in ("FINISHED", "FAILED"):
                break

        if not columns:
            return []
        return [dict(zip(columns, row)) for row in all_data]


def _esc(val) -> str:
    """Escape single quotes for Presto SQL strings."""
    if val is None:
        return ""
    return str(val).replace("'", "''")
