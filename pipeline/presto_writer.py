"""Presto SQL writer for inserting CDC batches into Iceberg tables.

Supports two authentication modes:
  - IBM Cloud watsonx.data: IAM bearer token from cloud.ibm.com (apikey -> token)
  - Local watsonx.data Developer Edition: HTTP Basic auth (ibmlhadmin / password)

The mode is selected automatically based on which env vars are set:
  - WATSONX_DATA_USERNAME + WATSONX_DATA_PASSWORD -> local mode
  - WATSONX_DATA_API_KEY                          -> cloud mode

Flow (both modes):
  1. POST INSERT SQL to Presto /v1/statement
  2. Poll nextUri until FINISHED or FAILED
"""

import time

import requests
import urllib3
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
        self.engine_port = config.presto_port
        self.catalog = config.wxd_catalog
        self.namespace = config.wxd_namespace
        self.api_key = config.wxd_api_key
        self.username = config.wxd_username
        self.password = config.wxd_password
        self.verify_ssl = config.wxd_verify_ssl
        self.local_mode = config.wxd_local_mode
        self._session = self._build_session()

        if not self.engine_host:
            raise ValueError(
                "PRESTO_ENGINE_HOST is required. "
                "Cloud: find in watsonx.data console > Presto engine > Connection information. "
                "Local DE: set to 'localhost' (with PRESTO_PORT=8443) after port-forwarding."
            )
        if not (self.api_key or (self.username and self.password)):
            raise ValueError(
                "Presto auth required. "
                "Cloud: set WATSONX_DATA_API_KEY. "
                "Local DE: set WATSONX_DATA_USERNAME and WATSONX_DATA_PASSWORD."
            )

        if not self.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @property
    def _statement_url(self) -> str:
        return f"https://{self.engine_host}:{self.engine_port}/v1/statement"

    @property
    def _presto_user(self) -> str:
        # Local DE uses the basic-auth username; cloud uses "admin"
        return self.username if self.local_mode else "admin"

    def _request_kwargs(self) -> dict:
        """Auth + TLS kwargs applied to every Presto request."""
        if self.local_mode:
            return {"auth": (self.username, self.password), "verify": self.verify_ssl}
        return {"verify": self.verify_ssl}

    def _request_headers(self, content_type: str | None = None) -> dict:
        headers = {
            "X-Presto-User": self._presto_user,
            "X-Presto-Catalog": self.catalog,
            "X-Presto-Schema": self.namespace,
        }
        if content_type:
            headers["Content-Type"] = content_type
        if not self.local_mode:
            # Cloud uses bearer; local uses basic auth (handled via session.auth)
            headers["Authorization"] = f"Bearer {self._get_iam_token()}"
        return headers

    def _get_iam_token(self) -> str:
        """Get or refresh an IAM bearer token (cloud mode only)."""
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

    def insert_batch(self, rows: list[dict], table_name: str = "expenses") -> bool:
        """Insert a batch of CDC rows into the Iceberg table. Returns True on success."""
        if not rows:
            return True

        iceberg_table = f"{self.catalog}.{self.namespace}.{table_name}"
        if table_name == "expenses":
            sql = self._build_expenses_insert(iceberg_table, rows)
        else:
            sql = self._build_generic_insert(iceberg_table, rows)
        return self._execute_sql(sql, f"{len(rows)} rows into {iceberg_table}")

    def _build_expenses_insert(self, table: str, rows: list[dict]) -> str:
        """Build INSERT SQL for the expenses table (legacy, typed)."""
        values = []
        for r in rows:
            recurring = "true" if r.get("recurring") else "false"
            values.append(
                f"('{_esc(r['expense_id'])}', '{_esc(r['user_id'])}', "
                f"'{_esc(r['description'])}', '{_esc(r['merchant'])}', "
                f"DOUBLE '{r['expense_amount']}', '{_esc(r['expense_date'])}', "
                f"'{_esc(r['shopping_type'])}', '{_esc(r['payment_method'])}', "
                f"{recurring}, '{_esc(r.get('cdc_table', 'expenses'))}', "
                f"'{_esc(r['cdc_operation'])}', '{_esc(r['cdc_timestamp'])}')"
            )
        return (
            f"INSERT INTO {table} "
            "(expense_id, user_id, description, merchant, expense_amount, "
            "expense_date, shopping_type, payment_method, recurring, "
            "cdc_table, cdc_operation, cdc_timestamp) VALUES " + ", ".join(values)
        )

    @staticmethod
    def _build_generic_insert(table: str, rows: list[dict]) -> str:
        """Build INSERT SQL for any table using VARCHAR columns."""
        if not rows:
            return ""
        columns = list(rows[0].keys())
        col_list = ", ".join(columns)
        values = []
        for r in rows:
            vals = ", ".join(f"'{_esc(r.get(c, ''))}'" for c in columns)
            values.append(f"({vals})")
        return f"INSERT INTO {table} ({col_list}) VALUES " + ", ".join(values)

    def _execute_sql(self, sql: str, description: str) -> bool:
        """Execute a SQL statement via Presto REST API and poll until complete."""
        try:
            headers = self._request_headers(content_type="text/plain")
        except Exception as e:
            print(f"❌ Presto: auth error: {e}")
            return False

        try:
            resp = self._session.post(
                self._statement_url, headers=headers, data=sql, timeout=30,
                **self._request_kwargs(),
            )
            resp.raise_for_status()
            result = resp.json()
        except requests.RequestException as e:
            print(f"❌ Presto POST failed: {e}")
            return False

        return self._poll_until_done(result, description)

    def _poll_until_done(self, result: dict, description: str) -> bool:
        """Poll Presto nextUri until the statement reaches FINISHED or FAILED."""
        next_uri = result.get("nextUri")
        state = result.get("stats", {}).get("state", "UNKNOWN")
        query_id = result.get("id", "?")
        start = time.monotonic()
        kwargs = self._request_kwargs()

        # Headers for polling: preserve auth (bearer in cloud, basic in local handled via kwargs)
        poll_headers = {}
        if not self.local_mode:
            poll_headers["Authorization"] = f"Bearer {self._get_iam_token()}"

        while next_uri and state not in ("FINISHED", "FAILED"):
            if time.monotonic() - start > POLL_MAX_WAIT:
                print(f"❌ Presto query {query_id} timed out after {POLL_MAX_WAIT}s (state={state})")
                return False

            time.sleep(POLL_INTERVAL)
            try:
                poll_resp = self._session.get(next_uri, headers=poll_headers, timeout=30, **kwargs)
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
            headers = self._request_headers(content_type="text/plain")
        except Exception as e:
            print(f"❌ Presto verify: auth error: {e}")
            return False

        try:
            resp = self._session.post(
                self._statement_url, headers=headers,
                data=f"SELECT COUNT(*) FROM {self.catalog}.{self.namespace}.expenses",
                timeout=30, **self._request_kwargs(),
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
            headers = self._request_headers(content_type="text/plain")
        except Exception as e:
            raise RuntimeError(f"Presto auth error: {e}") from e

        kwargs = self._request_kwargs()
        resp = self._session.post(
            self._statement_url, headers=headers, data=sql, timeout=30, **kwargs,
        )
        resp.raise_for_status()
        result = resp.json()

        columns = []
        all_data = []
        next_uri = result.get("nextUri")
        if result.get("columns"):
            columns = [c["name"] for c in result["columns"]]
        if result.get("data"):
            all_data.extend(result["data"])

        poll_headers = {}
        if not self.local_mode:
            poll_headers["Authorization"] = f"Bearer {self._get_iam_token()}"

        while next_uri:
            time.sleep(POLL_INTERVAL)
            poll_resp = self._session.get(next_uri, headers=poll_headers, timeout=30, **kwargs)
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
