# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Workspace Lakebase Read Test
# MAGIC
# MAGIC Runs in the **compute workspace** on classic compute, reads Lakebase that
# MAGIC lives in a **separate Lakebase workspace** (same Databricks account, same
# MAGIC cloud).
# MAGIC
# MAGIC Flow:
# MAGIC 1. Read SP creds from secret scope `cross-ws-lakebase`
# MAGIC 2. m2m OAuth against the Lakebase workspace at `/oidc/v1/token`
# MAGIC 3. Mint Lakebase DB credential via `POST /api/2.0/postgres/credentials`
# MAGIC 4. Resolve the endpoint host
# MAGIC 5. Connect with psycopg2 and run validation queries
# MAGIC
# MAGIC See the repo `GUIDE.md` for the end-to-end setup (SP creation, OAuth
# MAGIC client secret, Lakebase Postgres role, secret scope, gotchas).

# COMMAND ----------

# MAGIC %pip install --quiet psycopg2-binary requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import uuid
import json
import requests
import psycopg2

SCOPE = "cross-ws-lakebase"
CLIENT_ID                = dbutils.secrets.get(SCOPE, "sp_client_id")
CLIENT_SECRET            = dbutils.secrets.get(SCOPE, "sp_client_secret")
LAKEBASE_WORKSPACE_HOST  = dbutils.secrets.get(SCOPE, "lakebase_workspace_host").rstrip("/")
ENDPOINT_PATH            = dbutils.secrets.get(SCOPE, "lakebase_endpoint")  # projects/<P>/branches/<B>/endpoints/<E>

print(f"Lakebase workspace: {LAKEBASE_WORKSPACE_HOST}")
print(f"Lakebase endpoint:  {ENDPOINT_PATH}")
print(f"SP client_id:       {CLIENT_ID[:8]}…")

# COMMAND ----------

# Step 1: m2m OAuth against the Lakebase workspace
oauth_resp = requests.post(
    f"{LAKEBASE_WORKSPACE_HOST}/oidc/v1/token",
    auth=(CLIENT_ID, CLIENT_SECRET),
    data={"grant_type": "client_credentials", "scope": "all-apis"},
    timeout=30,
)
oauth_resp.raise_for_status()
ws_token = oauth_resp.json()["access_token"]
print(f"Workspace OAuth token: len={len(ws_token)}, expires_in={oauth_resp.json()['expires_in']}s")

# COMMAND ----------

# Step 2: Mint Lakebase DB credential
cred_resp = requests.post(
    f"{LAKEBASE_WORKSPACE_HOST}/api/2.0/postgres/credentials",
    headers={
        "Authorization": f"Bearer {ws_token}",
        "Content-Type": "application/json",
    },
    json={
        "request_id": str(uuid.uuid4()),
        "endpoint": ENDPOINT_PATH,
    },
    timeout=30,
)
cred_resp.raise_for_status()
db_token = cred_resp.json()["token"]
print(f"Lakebase DB token: len={len(db_token)}, expire_time={cred_resp.json().get('expire_time')}")

# COMMAND ----------

# Step 3: Resolve endpoint host via Lakebase API
project_id, _, branch_id_full = ENDPOINT_PATH.removeprefix("projects/").partition("/branches/")
branch_id, _, _endpoint_id    = branch_id_full.partition("/endpoints/")
branch_path = f"projects/{project_id}/branches/{branch_id}"

list_resp = requests.get(
    f"{LAKEBASE_WORKSPACE_HOST}/api/2.0/postgres/{branch_path}/endpoints",
    headers={"Authorization": f"Bearer {ws_token}"},
    timeout=30,
)
list_resp.raise_for_status()
endpoints = list_resp.json().get("endpoints", [])
host = next(e["status"]["hosts"]["host"] for e in endpoints if e["name"] == ENDPOINT_PATH)
print(f"Lakebase host: {host}")

# COMMAND ----------

# Step 4: Connect as the SP and run validation queries.
# The Postgres user for a Lakebase OAuth SP credential is the SP's applicationId
# (which is also the OAuth client_id).
pg_user = CLIENT_ID
print(f"Connecting to host={host} db=postgres sslmode=require as user={pg_user[:8]}…")

def connect(dbname):
    return psycopg2.connect(
        host=host, port=5432, dbname=dbname,
        user=pg_user, password=db_token,
        sslmode="require", connect_timeout=20,
    )

results = {"lakebase_endpoint": ENDPOINT_PATH, "host": host}

# Server-level metadata via the default postgres database
conn = connect("postgres")
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT version();")
    results["pg_version"] = cur.fetchone()[0]
    cur.execute("SELECT current_user, current_database();")
    cu, cdb = cur.fetchone()
    results["session"] = {"current_user": cu, "current_database": cdb}
    cur.execute("SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY 1;")
    results["databases"] = [r[0] for r in cur.fetchall()]
conn.close()

# For each non-template DB the SP can open, probe schemas/tables and read samples
per_db = {}
for db in results["databases"]:
    info = {}
    try:
        c = connect(db)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("""
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p','v','m','f')
                  AND has_table_privilege(c.oid, 'SELECT')
                  AND n.nspname NOT IN ('pg_catalog','information_schema')
                ORDER BY 1,2
                LIMIT 50;
            """)
            tables = cur.fetchall()
            info["readable_tables"] = [f"{s}.{t}" for s, t in tables]
            info["readable_count"] = len(tables)

            if tables:
                s, t = tables[0]
                cur.execute(f'SELECT count(*) FROM "{s}"."{t}";')
                info["sample_table"] = f"{s}.{t}"
                info["sample_table_count"] = cur.fetchone()[0]
                cur.execute(f'SELECT * FROM "{s}"."{t}" LIMIT 3;')
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()
                info["sample_columns"] = cols
                info["sample_rows"] = [
                    {col: (str(val) if val is not None else None) for col, val in zip(cols, row)}
                    for row in rows
                ]
        c.close()
    except Exception as e:
        info["error"] = f"{type(e).__name__}: {e}".strip()
    per_db[db] = info

results["per_database"] = per_db
print(json.dumps(results, indent=2, default=str))
dbutils.notebook.exit(json.dumps(results, default=str))
