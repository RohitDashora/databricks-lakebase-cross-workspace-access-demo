# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Workspace Lakebase — Data API (HTTP / 443) read
# MAGIC
# MAGIC Same goal as `cross_ws_lakebase_test.py` — read a Lakebase database in a
# MAGIC **different** workspace from a classic-compute notebook — but over the
# MAGIC **[Lakebase Data API](https://docs.databricks.com/aws/en/oltp/projects/data-api)**
# MAGIC (a PostgREST-compatible HTTPS interface) instead of the Postgres wire
# MAGIC protocol.
# MAGIC
# MAGIC **Why this matters for cross-workspace networking:** the Data API is plain
# MAGIC **HTTPS on 443** — there is **no TCP 5432 hop**. So this path does not need
# MAGIC the Postgres data-plane egress (5432) that the psql/psycopg2 path requires,
# MAGIC and the Shared-cluster 5432 block is a non-issue. (Restricted-network
# MAGIC implications are covered in `NETWORKING.md`.)
# MAGIC
# MAGIC ## Prerequisites (one-time, in the Lakebase workspace)
# MAGIC 1. **Enable the Data API** on the Lakebase project — in the Lakebase App's
# MAGIC    **Data API** page. This auto-creates the `authenticator` role and `pgrst`
# MAGIC    schema, and surfaces the **REST endpoint URL** (copy it).
# MAGIC 2. The calling **service principal still needs a Postgres role + GRANTs** on
# MAGIC    the target database (same as the psql path — see the repo README).
# MAGIC 3. (Production) enable **Row-Level Security** on exposed tables.
# MAGIC
# MAGIC Auth uses the **Databricks OAuth bearer token** directly — there is **no**
# MAGIC separate Lakebase DB-credential mint for the Data API.

# COMMAND ----------

# MAGIC %pip install --quiet requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import requests

SCOPE = "cross-ws-lakebase"

dbutils.widgets.text("data_api_url", "", "Data API REST endpoint URL (from the Lakebase App)")
dbutils.widgets.text("schema", "public", "Schema")
dbutils.widgets.text("table", "", "Table to read")
dbutils.widgets.text("limit", "5", "Row limit")

def _secret(key):
    try:
        return dbutils.secrets.get(SCOPE, key)
    except Exception:
        return None

CLIENT_ID     = _secret("sp_client_id")
CLIENT_SECRET = _secret("sp_client_secret")
LB_HOST       = (_secret("lakebase_workspace_host") or "").rstrip("/")
# REST endpoint: prefer a secret if you stored it, else the widget.
REST_ENDPOINT = (_secret("lakebase_data_api_url") or dbutils.widgets.get("data_api_url")).rstrip("/")
SCHEMA = dbutils.widgets.get("schema").strip() or "public"
TABLE  = dbutils.widgets.get("table").strip()
LIMIT  = dbutils.widgets.get("limit").strip() or "5"

assert CLIENT_ID and CLIENT_SECRET, "Need sp_client_id / sp_client_secret in the secret scope."
assert LB_HOST.startswith("http"), "Need lakebase_workspace_host in the secret scope."
assert REST_ENDPOINT.startswith("http"), (
    "Need the Data API REST endpoint URL — enable the Data API in the Lakebase App "
    "and paste its URL into the data_api_url widget (or store it as the "
    "lakebase_data_api_url secret)."
)
assert TABLE, "Set the `table` widget to a table the SP can SELECT."

print(f"Lakebase workspace : {LB_HOST}")
print(f"Data API endpoint  : {REST_ENDPOINT}")
print(f"Reading            : {SCHEMA}.{TABLE} (limit {LIMIT})")
print(f"SP client_id       : {CLIENT_ID[:8]}…")

# COMMAND ----------

# Step 1 — mint a Databricks workspace OAuth token (m2m). This bearer token IS
# the Data API credential; no separate Lakebase DB-credential mint is needed.
oauth = requests.post(
    f"{LB_HOST}/oidc/v1/token",
    auth=(CLIENT_ID, CLIENT_SECRET),
    data={"grant_type": "client_credentials", "scope": "all-apis"},
    timeout=30,
)
if not oauth.ok:
    # Keep X-Databricks-Reason-Phrase: on a 403 it names which ingress control
    # rejected the call (IP access list vs PrivateLink vs public access).
    # raise_for_status() discards it. See cross_ws_lakebase_netdiag.py.
    _phrase = oauth.headers.get("X-Databricks-Reason-Phrase") or (oauth.text or "")[:300]
    raise RuntimeError(f"POST /oidc/v1/token: HTTP {oauth.status_code}: {_phrase}")
bearer = oauth.json()["access_token"]
print(f"Workspace OAuth token: len={len(bearer)}, expires_in={oauth.json().get('expires_in')}s")

# COMMAND ----------

# Step 2 — GET <REST_ENDPOINT>/<schema>/<table>?limit=N  (PostgREST style)
url = f"{REST_ENDPOINT}/{SCHEMA}/{TABLE}"
resp = requests.get(
    url,
    headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"},
    params={"limit": LIMIT},
    timeout=30,
)

result = {"path": "data-api (HTTP 443)", "url": url, "status": resp.status_code}
ct = resp.headers.get("content-type", "")

if resp.status_code == 200:
    rows = resp.json() if "json" in ct else resp.text
    result["row_count"] = len(rows) if isinstance(rows, list) else None
    result["rows"] = rows[:int(LIMIT)] if isinstance(rows, list) else rows
    print("✅ Data API read SUCCEEDED")
    print(json.dumps(result, indent=2, default=str))
else:
    body = (resp.text or "")[:400].replace("\n", " ")
    # Self-diagnosis keyed to the likely cause
    if resp.status_code in (401, 403):
        hint = ("Reachable, but rejected. The Data API authorizes via the SP's "
                "Postgres role + GRANTs — ensure the SP has a role and SELECT on "
                f"{SCHEMA}.{TABLE} (and RLS allows it). Not a networking problem.")
    elif resp.status_code == 404:
        hint = ("404 — the table/schema isn't exposed, or the Data API isn't enabled "
                "on this project. Enable it in the Lakebase App and confirm the schema "
                "is exposed and the REST endpoint URL is correct.")
    elif resp.status_code == 400:
        hint = ("400 'Invalid request' from the regional ingress — the URL shape is "
                "likely wrong or the Data API isn't enabled. Re-copy the REST endpoint "
                "URL from the Lakebase App's Data API page.")
    else:
        hint = "Unexpected status; see body."
    result["hint"] = hint
    result["body"] = body
    print(f"❌ Data API read FAILED ({resp.status_code})")
    print(json.dumps(result, indent=2, default=str))

dbutils.notebook.exit(json.dumps(result, default=str))
