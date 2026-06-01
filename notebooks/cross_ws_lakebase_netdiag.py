# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Workspace Lakebase — Network Diagnostic
# MAGIC
# MAGIC Run this on **classic compute in the compute workspace** when the basic
# MAGIC read (`cross_ws_lakebase_test.py`) fails or hangs and you suspect a
# MAGIC network restriction (IP access list, front-end / back-end PrivateLink,
# MAGIC egress firewall).
# MAGIC
# MAGIC It probes each network leg **independently** and prints a per-leg verdict
# MAGIC plus a diagnosis, so you can tell *which* hop is broken — the failures
# MAGIC otherwise all look like the same timeout.
# MAGIC
# MAGIC | Leg | What | Destination | Port |
# MAGIC |-----|------|-------------|------|
# MAGIC | **A** | OAuth + mint DB token + resolve host | `https://<lakebase-ws>` (workspace front door) | 443 |
# MAGIC | **B** | the Postgres connection itself | `ep-*.database.<region>.cloud.databricks.com` | **5432** |
# MAGIC
# MAGIC See `NETWORKING.md` in the repo for the restriction→leg matrix and the
# MAGIC signature→fix decision tree this notebook's diagnosis points at.
# MAGIC
# MAGIC **Nothing here is destructive** — it only reads. Safe to run anywhere.

# COMMAND ----------

# MAGIC %pip install --quiet psycopg2-binary requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs
# MAGIC Reads the same secret scope as the main demo. If you don't have the scope
# MAGIC set up yet, fill the widgets instead (scope values win if present).

# COMMAND ----------

import socket, ssl, time, uuid, json, ipaddress
import requests

dbutils.widgets.text("lakebase_workspace_host", "", "Lakebase workspace host (https://...)")
dbutils.widgets.text("lakebase_endpoint", "", "Endpoint (projects/<P>/branches/<B>/endpoints/<E>)")
dbutils.widgets.text("secret_scope", "cross-ws-lakebase", "Secret scope name")

SCOPE = dbutils.widgets.get("secret_scope").strip()

def _secret_or_widget(key, widget):
    try:
        v = dbutils.secrets.get(SCOPE, key)
        if v:
            return v
    except Exception:
        pass
    return dbutils.widgets.get(widget).strip()

def _secret_only(key):
    try:
        return dbutils.secrets.get(SCOPE, key)
    except Exception:
        return None

CLIENT_ID   = _secret_only("sp_client_id")
CLIENT_SECRET = _secret_only("sp_client_secret")
LB_HOST     = _secret_or_widget("lakebase_workspace_host", "lakebase_workspace_host").rstrip("/")
ENDPOINT    = _secret_or_widget("lakebase_endpoint", "lakebase_endpoint")

HAVE_CREDS = bool(CLIENT_ID and CLIENT_SECRET)

assert LB_HOST.startswith("http"), "Need a Lakebase workspace host (scope key lakebase_workspace_host or the widget)."
assert ENDPOINT.startswith("projects/"), "Need a Lakebase endpoint path (scope key lakebase_endpoint or the widget)."

LB_HOSTNAME = LB_HOST.split("://", 1)[1].split("/", 1)[0]

print(f"Lakebase workspace : {LB_HOST}")
print(f"Lakebase endpoint  : {ENDPOINT}")
print(f"SP creds available : {HAVE_CREDS}  (Leg A app-layer + Leg B need these)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe framework

# COMMAND ----------

RESULTS = []   # list of dicts: {step, leg, status, error_class, detail, latency_ms}

def record(step, leg, status, detail="", error_class="", latency_ms=None):
    RESULTS.append({
        "step": step, "leg": leg, "status": status,
        "error_class": error_class, "detail": detail, "latency_ms": latency_ms,
    })
    tag = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "SKIP": "⏭️", "INFO": "ℹ️"}.get(status, "•")
    lat = f"  [{latency_ms} ms]" if latency_ms is not None else ""
    ec = f"  <{error_class}>" if error_class else ""
    print(f"{tag} [{leg}] {step}: {status}{ec}{lat}")
    if detail:
        print(f"      {detail}")

def classify_ip(ip):
    try:
        return "private" if ipaddress.ip_address(ip).is_private else "public"
    except ValueError:
        return "?"

def dns_probe(step, leg, host):
    t0 = time.time()
    try:
        infos = socket.getaddrinfo(host, None)
        ips = sorted({i[4][0] for i in infos})
        kinds = {classify_ip(ip) for ip in ips}
        detail = ", ".join(f"{ip} ({classify_ip(ip)})" for ip in ips)
        record(step, leg, "PASS", f"resolves to {detail}", latency_ms=int((time.time()-t0)*1000))
        return ips, kinds
    except socket.gaierror as e:
        record(step, leg, "FAIL", f"DNS resolution failed: {e}", error_class="DNS_NXDOMAIN",
               latency_ms=int((time.time()-t0)*1000))
        return [], set()

def tcp_probe(step, leg, host, port, timeout=12):
    t0 = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            record(step, leg, "PASS", f"TCP connect to {host}:{port} succeeded",
                   latency_ms=int((time.time()-t0)*1000))
            return True
    except socket.timeout:
        record(step, leg, "FAIL", f"TCP connect to {host}:{port} timed out after {timeout}s",
               error_class="TCP_TIMEOUT", latency_ms=int((time.time()-t0)*1000))
    except ConnectionRefusedError:
        record(step, leg, "FAIL", f"TCP connect to {host}:{port} refused",
               error_class="TCP_REFUSED", latency_ms=int((time.time()-t0)*1000))
    except socket.gaierror as e:
        record(step, leg, "FAIL", f"DNS failure for {host}: {e}", error_class="DNS_NXDOMAIN")
    except OSError as e:
        record(step, leg, "FAIL", f"TCP connect to {host}:{port} failed: {e}",
               error_class="TCP_OSERROR", latency_ms=int((time.time()-t0)*1000))
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 0 — Cluster access mode (the silent 5432 killer)
# MAGIC **Standard / Shared (`USER_ISOLATION`) classic clusters block arbitrary
# MAGIC outbound TCP — including 5432** — regardless of any network config or your
# MAGIC credentials. The Postgres data path (Leg B) only works from a **Dedicated /
# MAGIC single-user** classic cluster (or serverless). This probe asks the Clusters
# MAGIC API about this cluster (authoritative), falling back to Spark conf. If it
# MAGIC warns, switch cluster access mode before debugging anything else.

# COMMAND ----------

def detect_access_mode():
    """Return (mode, source). Prefer the Clusters API (authoritative); fall back
    to Spark conf. Returns (None, reason) on serverless / when unavailable."""
    # Authoritative: ask the Clusters API about THIS cluster, using the
    # notebook's own context token against the local workspace.
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        token = ctx.apiToken().get()
        api_url = ctx.apiUrl().get()
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        if cluster_id:
            r = requests.get(
                f"{api_url}/api/2.0/clusters/get",
                headers={"Authorization": f"Bearer {token}"},
                params={"cluster_id": cluster_id}, timeout=15,
            )
            if r.ok:
                dsm = r.json().get("data_security_mode")
                if dsm:
                    return dsm, "clusters API"
    except Exception:
        pass
    # Fallback: Spark conf (not always populated).
    for key in ("spark.databricks.clusterUsageTags.clusterDataSecurityMode",
                "spark.databricks.clusterUsageTags.dataSecurityMode"):
        try:
            v = spark.conf.get(key)
            if v:
                return v, "spark conf"
        except Exception:
            continue
    return None, "no cluster_id (likely serverless)"

# Modes that block arbitrary outbound sockets (incl. 5432) — "Standard"/Shared family.
SHARED_MODES = {"USER_ISOLATION", "STANDARD", "DATA_SECURITY_MODE_STANDARD",
                "LEGACY_TABLE_ACL", "LEGACY_PASSTHROUGH"}
# Modes that permit outbound 5432 — "Dedicated"/Single-user family.
DEDICATED_MODES = {"SINGLE_USER", "NONE", "DEDICATED", "DATA_SECURITY_MODE_DEDICATED",
                   "LEGACY_SINGLE_USER", "LEGACY_SINGLE_USER_STANDARD"}

mode, src = detect_access_mode()
if mode is None:
    record("Cluster access mode", "B", "INFO",
           f"Could not detect access mode ({src}). If Leg B fails on 5432, confirm you're not on a "
           "Standard/Shared cluster.")
elif mode.upper() in SHARED_MODES:
    record("Cluster access mode", "B", "WARN",
           f"data_security_mode={mode} (Standard/Shared, via {src}) — Standard/Shared classic clusters "
           "BLOCK outbound 5432. Leg B cannot work here. Re-run on a Dedicated/Single-user cluster "
           "or serverless.")
elif mode.upper() in DEDICATED_MODES:
    record("Cluster access mode", "B", "PASS",
           f"data_security_mode={mode} (Dedicated/Single-user, via {src}) — outbound 5432 is permitted.")
else:
    record("Cluster access mode", "B", "INFO",
           f"data_security_mode={mode} (via {src}) — unrecognized mode. If Leg B fails on 5432, "
           "the cluster may be in a Standard/Shared family mode that blocks outbound sockets.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 1 — Egress identity (the IP you'd allowlist)
# MAGIC The public IP this cluster presents to the internet. Under back-end
# MAGIC PrivateLink / SCC this is your NAT gateway EIP; behind a proxy it's the
# MAGIC proxy. **This is the source IP to add to the Lakebase workspace's IP
# MAGIC access list.**

# COMMAND ----------

egress_ip = None
for url in ("https://checkip.amazonaws.com", "https://api.ipify.org", "https://ifconfig.me/ip"):
    try:
        r = requests.get(url, timeout=10)
        if r.ok:
            egress_ip = r.text.strip()
            record("Egress public IP", "—", "INFO",
                   f"This cluster egresses as {egress_ip}  (allowlist this on the Lakebase workspace IP ACL)")
            break
    except Exception:
        continue
if not egress_ip:
    record("Egress public IP", "—", "WARN",
           "Could not reach any echo service — egress to the public internet may be fully blocked "
           "(strict firewall / no NAT). That itself is a finding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 2 — DNS resolution (both hosts)
# MAGIC Private IPs here mean PrivateLink/private DNS is in effect; NXDOMAIN means
# MAGIC DNS isn't wired up for that destination.

# COMMAND ----------

dns_probe("Resolve workspace host (Leg A)", "A", LB_HOSTNAME)

# We don't know the DB host yet (it comes from Leg A), but if the basic notebook
# already told you the host, you can probe it here. Otherwise Probe 6 resolves it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 3 — Leg A network reachability (TCP 443 to the workspace)

# COMMAND ----------

legA_tcp = tcp_probe("TCP 443 to workspace front door", "A", LB_HOSTNAME, 443)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 4 — Leg A HTTP layer (reachable vs IP-ACL vs blocked)
# MAGIC Hits the OIDC well-known config (no auth needed). Distinguishes:
# MAGIC an **HTTP 200/4xx** (network is fine) from a **timeout/reset** (PL or
# MAGIC firewall) from an **IP-ACL 403**.

# COMMAND ----------

t0 = time.time()
try:
    r = requests.get(f"{LB_HOST}/oidc/.well-known/oauth-authorization-server", timeout=15)
    lat = int((time.time()-t0)*1000)
    body_snip = (r.text or "")[:200].replace("\n", " ")
    if r.status_code == 200:
        record("HTTP GET oidc well-known", "A", "PASS",
               f"HTTP 200 — workspace front door is reachable at the app layer", latency_ms=lat)
    elif r.status_code == 403:
        record("HTTP GET oidc well-known", "A", "FAIL",
               f"HTTP 403 — likely IP access list blocking this source IP ({egress_ip}). Body: {body_snip}",
               error_class="HTTP_403_IP_ACL", latency_ms=lat)
    else:
        record("HTTP GET oidc well-known", "A", "WARN",
               f"HTTP {r.status_code} (network reachable). Body: {body_snip}",
               error_class=f"HTTP_{r.status_code}", latency_ms=lat)
except requests.exceptions.Timeout:
    record("HTTP GET oidc well-known", "A", "FAIL",
           "Request timed out — front-end PrivateLink with public access disabled, or egress firewall "
           "blocking 443 to this host. No private route from this VPC.",
           error_class="HTTP_TIMEOUT", latency_ms=int((time.time()-t0)*1000))
except requests.exceptions.ConnectionError as e:
    record("HTTP GET oidc well-known", "A", "FAIL",
           f"Connection error (reset/refused): {e}", error_class="HTTP_CONN_ERROR",
           latency_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 5 — Leg A app layer: mint workspace OAuth token

# COMMAND ----------

ws_token = None
if not HAVE_CREDS:
    record("Mint workspace OAuth token", "A", "SKIP",
           "No SP client_id/secret in the scope — can't test token mint or Leg B. "
           "Set sp_client_id / sp_client_secret to test the full path.")
else:
    t0 = time.time()
    try:
        r = requests.post(
            f"{LB_HOST}/oidc/v1/token",
            auth=(CLIENT_ID, CLIENT_SECRET),
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            timeout=20,
        )
        lat = int((time.time()-t0)*1000)
        if r.status_code == 200 and r.json().get("access_token"):
            ws_token = r.json()["access_token"]
            record("Mint workspace OAuth token", "A", "PASS",
                   f"Got access_token (expires_in={r.json().get('expires_in')}s)", latency_ms=lat)
        elif r.status_code in (401, 400):
            record("Mint workspace OAuth token", "A", "FAIL",
                   f"HTTP {r.status_code} — network OK but credentials rejected. Check client_id/secret "
                   f"and that the SP exists in the Lakebase workspace. Body: {(r.text or '')[:200]}",
                   error_class="OAUTH_BAD_CREDS", latency_ms=lat)
        elif r.status_code == 403:
            record("Mint workspace OAuth token", "A", "FAIL",
                   f"HTTP 403 — IP access list blocking source IP {egress_ip}.",
                   error_class="HTTP_403_IP_ACL", latency_ms=lat)
        else:
            record("Mint workspace OAuth token", "A", "FAIL",
                   f"HTTP {r.status_code}: {(r.text or '')[:200]}",
                   error_class=f"HTTP_{r.status_code}", latency_ms=lat)
    except requests.exceptions.Timeout:
        record("Mint workspace OAuth token", "A", "FAIL",
               "Timed out — front-end PL/firewall (see Probe 4).", error_class="HTTP_TIMEOUT",
               latency_ms=int((time.time()-t0)*1000))
    except requests.exceptions.ConnectionError as e:
        record("Mint workspace OAuth token", "A", "FAIL", f"Connection error: {e}",
               error_class="HTTP_CONN_ERROR")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 6 — Leg A app layer: mint DB credential + resolve endpoint host

# COMMAND ----------

db_token = None
db_host = None
if not ws_token:
    record("Mint DB credential + resolve host", "A", "SKIP", "No workspace token from Probe 5.")
else:
    # mint DB token
    cred_request_id = str(uuid.uuid4())   # we generate it — useful for support escalation
    cred_response_request_id = None
    t0 = time.time()
    try:
        r = requests.post(
            f"{LB_HOST}/api/2.0/postgres/credentials",
            headers={"Authorization": f"Bearer {ws_token}"},
            json={"request_id": cred_request_id, "endpoint": ENDPOINT},
            timeout=20,
        )
        lat = int((time.time()-t0)*1000)
        cred_response_request_id = r.headers.get("x-request-id") or r.headers.get("x-databricks-request-id")
        body_snip = (r.text or "")[:300].replace("\n", " ")
        if r.status_code == 200 and r.json().get("token"):
            db_token = r.json()["token"]
            record("Mint Lakebase DB credential", "A", "PASS",
                   f"Got DB token (expire_time={r.json().get('expire_time')})", latency_ms=lat)
        elif r.status_code == 403:
            # IMPORTANT: a 403 HERE (not on OIDC) means the credential-mint endpoint enforces a
            # Lakebase-specific network policy that is SEPARATE from the workspace IP access list.
            # If OAuth (Probe 5) already passed from this same IP, the workspace IP ACL is NOT the gate.
            oauth_ok = any(rr["step"] == "Mint workspace OAuth token" and rr["status"] == "PASS"
                           for rr in RESULTS)
            note = ("OAuth from this IP already succeeded, so the workspace IP ACL is NOT the blocker — "
                    "the credential endpoint enforces a separate Lakebase network policy. "
                    if oauth_ok else
                    "Also check the workspace IP ACL (OAuth was not confirmed). ")
            record("Mint Lakebase DB credential", "A", "FAIL",
                   f"HTTP 403 on /api/2.0/postgres/credentials. {note}"
                   f"Allowlisting the workspace IP ACL is necessary but may be INSUFFICIENT for this "
                   f"endpoint. Escalate with request_id={cred_response_request_id or cred_request_id}. "
                   f"Body: {body_snip}",
                   error_class="LAKEBASE_CRED_403", latency_ms=lat)
        else:
            record("Mint Lakebase DB credential", "A", "FAIL",
                   f"HTTP {r.status_code} (request_id={cred_response_request_id or cred_request_id}): {body_snip}",
                   error_class=f"HTTP_{r.status_code}", latency_ms=lat)
    except Exception as e:
        record("Mint Lakebase DB credential", "A", "FAIL", f"{type(e).__name__}: {e}",
               error_class="HTTP_EXCEPTION")

    # resolve host
    if db_token is not None:
        project, _, rest = ENDPOINT.removeprefix("projects/").partition("/branches/")
        branch, _, _ = rest.partition("/endpoints/")
        t0 = time.time()
        try:
            r = requests.get(
                f"{LB_HOST}/api/2.0/postgres/projects/{project}/branches/{branch}/endpoints",
                headers={"Authorization": f"Bearer {ws_token}"},
                timeout=20,
            )
            r.raise_for_status()
            eps = r.json().get("endpoints", [])
            db_host = next(e["status"]["hosts"]["host"] for e in eps if e["name"] == ENDPOINT)
            record("Resolve endpoint host", "A", "PASS", f"DB host = {db_host}",
                   latency_ms=int((time.time()-t0)*1000))
        except Exception as e:
            record("Resolve endpoint host", "A", "FAIL", f"{type(e).__name__}: {e}",
                   error_class="HTTP_EXCEPTION")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 7 — Leg B DNS + raw TCP to the database endpoint (port 5432)
# MAGIC This is the key isolation: a **TCP timeout** here = egress firewall blocks
# MAGIC 5432 *or* the DB endpoint has no route from this network. A **successful
# MAGIC TCP connect** means the network is fine and any later failure is auth/grants.

# COMMAND ----------

legB_tcp = False
if not db_host:
    record("DNS + TCP 5432 to DB endpoint", "B", "SKIP",
           "DB host not resolved (Leg A incomplete). If you already know the host from the basic "
           "notebook, set it in code here to test Leg B in isolation.")
else:
    dns_probe("Resolve DB endpoint host (Leg B)", "B", db_host)
    legB_tcp = tcp_probe("TCP 5432 to DB endpoint", "B", db_host, 5432)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 7b — Leg A data API (`api.database.<region>`, 443)
# MAGIC Separate from the workspace host: `api.database.*` backs the SQL Editor UI
# MAGIC and table/branch metadata. It can be blocked independently of the 5432 data
# MAGIC path — that's the difference between "SQL Editor broken but queries work"
# MAGIC (Scenario 3) and "see tables but can't query" (Scenario 4). See NETWORKING.md.

# COMMAND ----------

if not db_host or ".database." not in db_host:
    record("Leg A data API (api.database)", "A", "SKIP",
           "DB host not resolved — can't derive the api.database.<region> host.")
else:
    api_host = "api.database." + db_host.split(".database.", 1)[1]
    dns_probe("Resolve api.database host (Leg A)", "A", api_host)
    if tcp_probe("TCP 443 to api.database", "A", api_host, 443):
        t0 = time.time()
        try:
            r = requests.get(f"https://{api_host}/", timeout=15)
            record("HTTP to api.database", "A", "PASS",
                   f"HTTP {r.status_code} — data API reachable at the app layer",
                   latency_ms=int((time.time()-t0)*1000))
        except requests.exceptions.Timeout:
            record("HTTP to api.database", "A", "FAIL",
                   "Timed out on 443 to api.database — proxy/PL/firewall blocks the data API while "
                   "5432 may be fine. Symptom: SQL Editor broken, notebook queries work (Scenario 3).",
                   error_class="DATA_API_BLOCKED", latency_ms=int((time.time()-t0)*1000))
        except requests.exceptions.ConnectionError as e:
            record("HTTP to api.database", "A", "FAIL",
                   f"Connection error to api.database: {e}", error_class="DATA_API_BLOCKED",
                   latency_ms=int((time.time()-t0)*1000))
    else:
        # tcp_probe already recorded a TCP_* failure; flag the data-API meaning too.
        record("api.database reachability", "A", "WARN",
               "api.database.<region> unreachable on 443 — SQL Editor / metadata will fail even if "
               "5432 works (Scenario 3). Allowlist api.database.<region> on 443.",
               error_class="DATA_API_BLOCKED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 8 — Leg B app layer: psycopg2 connect + SELECT
# MAGIC If Probe 7 passed but this fails with `password authentication failed`,
# MAGIC **the network is fine** — it's the SP Postgres role / GRANTs (see the main
# MAGIC README's gotchas #4 and #5), not a network restriction.

# COMMAND ----------

if not (db_host and db_token):
    record("psycopg2 connect + SELECT", "B", "SKIP", "Need DB host + DB token from Leg A.")
elif not legB_tcp:
    record("psycopg2 connect + SELECT", "B", "SKIP",
           "Skipping — raw TCP to 5432 already failed (Probe 7). Fix the network path first.")
else:
    import psycopg2
    t0 = time.time()
    try:
        conn = psycopg2.connect(
            host=db_host, port=5432, dbname="postgres",
            user=CLIENT_ID, password=db_token,
            sslmode="require", connect_timeout=20,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT current_user, current_database(), version();")
            row = cur.fetchone()
        conn.close()
        record("psycopg2 connect + SELECT", "B", "PASS",
               f"Connected & queried: user={row[0]}, db={row[1]}", latency_ms=int((time.time()-t0)*1000))
    except psycopg2.OperationalError as e:
        msg = str(e)
        if "password authentication failed" in msg:
            record("psycopg2 connect + SELECT", "B", "FAIL",
                   "Postgres auth failed — NETWORK IS FINE. Fix is the SP Postgres role / GRANTs "
                   "(README gotchas #4, #5), not networking.",
                   error_class="PG_AUTH_FAILED", latency_ms=int((time.time()-t0)*1000))
        elif "Invalid protocol version" in msg or "196608" in msg:
            record("psycopg2 connect + SELECT", "B", "FAIL",
                   "SSL was not negotiated — set sslmode=require. NETWORK IS FINE.",
                   error_class="PG_SSL_DISABLED", latency_ms=int((time.time()-t0)*1000))
        elif "Invalid authorization for databricks identity login" in msg:
            record("psycopg2 connect + SELECT", "B", "FAIL",
                   "Identity login rejected — OAuth token expired (~1h) or a group identity is being used "
                   "on a dedicated cluster. Re-mint the token; ensure the cluster's single-user identity "
                   "matches the SP/user that owns the Postgres role. NETWORK IS FINE.",
                   error_class="PG_TOKEN_OR_IDENTITY", latency_ms=int((time.time()-t0)*1000))
        elif "timeout expired" in msg or "could not connect" in msg:
            record("psycopg2 connect + SELECT", "B", "FAIL",
                   f"Connection timeout/refused at psycopg2 layer despite TCP probe — TLS or mid-stream "
                   f"block. Detail: {msg[:200]}", error_class="PG_CONN_TIMEOUT",
                   latency_ms=int((time.time()-t0)*1000))
        elif "SSL" in msg:
            record("psycopg2 connect + SELECT", "B", "FAIL",
                   f"TLS error — a TLS-intercepting proxy may be breaking the Postgres SSL negotiation. "
                   f"Detail: {msg[:200]}", error_class="PG_SSL_ERROR",
                   latency_ms=int((time.time()-t0)*1000))
        else:
            record("psycopg2 connect + SELECT", "B", "FAIL", msg[:250],
                   error_class="PG_OPERATIONAL", latency_ms=int((time.time()-t0)*1000))
    except Exception as e:
        record("psycopg2 connect + SELECT", "B", "FAIL", f"{type(e).__name__}: {e}",
               error_class="PG_EXCEPTION")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verdict + diagnosis

# COMMAND ----------

print("="*72)
print("PER-LEG VERDICT")
print("="*72)
for r in RESULTS:
    tag = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "SKIP": "⏭️", "INFO": "ℹ️"}.get(r["status"], "•")
    print(f"{tag} [{r['leg']}] {r['step']:<42} {r['status']:<5} {r['error_class']}")

def has(error_class):
    return any(r["error_class"] == error_class for r in RESULTS)

def leg_status(leg):
    rs = [r for r in RESULTS if r["leg"] == leg and r["status"] in ("PASS", "FAIL")]
    if not rs:
        return "UNKNOWN"
    return "FAIL" if any(r["status"] == "FAIL" for r in rs) else "PASS"

print()
print("="*72)
print("DIAGNOSIS  (see NETWORKING.md for the full signature→fix table)")
print("="*72)

diagnoses = []
if has("DNS_NXDOMAIN"):
    diagnoses.append("• DNS_NXDOMAIN — a hostname won't resolve. Under PrivateLink you need private DNS "
                     "(Route53 private hosted zone / Azure private DNS / GCP) for that destination. "
                     "[NETWORKING.md → DNS]")
if has("HTTP_403_IP_ACL"):
    diagnoses.append(f"• IP ACCESS LIST — the OIDC/well-known call returns 403. Add this cluster's egress IP "
                     f"({egress_ip}) to the Lakebase workspace IP access list. [NETWORKING.md → IP access lists]")
if has("LAKEBASE_CRED_403"):
    diagnoses.append(f"• LAKEBASE NETWORK POLICY (not the workspace IP ACL) — /api/2.0/postgres/credentials "
                     f"returns 403 even though OAuth succeeded from this same IP ({egress_ip}). The credential "
                     "endpoint enforces a Lakebase-specific network policy SEPARATE from the workspace IP "
                     "access list, so allowlisting the workspace IP ACL is necessary but not sufficient. "
                     "Capture the request_id above and escalate to Databricks. [NETWORKING.md → Other ingress controls]")
if has("HTTP_TIMEOUT") or has("HTTP_CONN_ERROR"):
    diagnoses.append("• LEG A BLOCKED — control-plane calls time out/reset. Either the Lakebase workspace has "
                     "front-end PrivateLink with public access disabled (and this VPC has no private route to "
                     "it), or an egress firewall blocks 443 to the workspace host. [NETWORKING.md → Front-end PL]")
if has("TCP_TIMEOUT") or has("TCP_REFUSED") or has("TCP_OSERROR"):
    diagnoses.append("• LEG B BLOCKED (port 5432) — the Postgres data path can't be reached. Most common cause: "
                     "egress firewall allows 443 but not 5432. Open egress to the DB endpoint host on 5432, or "
                     "set up a private endpoint for the database. [NETWORKING.md → Leg B / egress firewall]")
if has("DATA_API_BLOCKED"):
    diagnoses.append("• DATA API BLOCKED — api.database.<region> (443) is unreachable while the workspace host "
                     "may be fine. SQL Editor / table metadata will fail even if 5432 queries work "
                     "(Scenario 3). Allowlist api.database.<region> on 443. [NETWORKING.md → 4 scenarios]")
if has("PG_SSL_ERROR"):
    diagnoses.append("• TLS INTERCEPTION — a forward proxy is breaking the Postgres SSL negotiation on 5432. "
                     "Postgres SSL can't be MITM'd like HTTPS; bypass the proxy for the DB endpoint. "
                     "[NETWORKING.md → proxies]")
if has("PG_SSL_DISABLED"):
    diagnoses.append("• SSL NOT NEGOTIATED — connection got an 'Invalid protocol version' error. "
                     "Set sslmode=require. NETWORK IS FINE. [NETWORKING.md → signature table]")
if has("PG_TOKEN_OR_IDENTITY"):
    diagnoses.append("• TOKEN / IDENTITY — TCP 5432 connected; the DB token was expired (~1h) or a group "
                     "identity was used on a dedicated cluster. Re-mint the token; check the cluster's "
                     "single-user identity. NETWORK IS FINE. [NETWORKING.md → signature table]")
if has("PG_AUTH_FAILED"):
    diagnoses.append("• NOT A NETWORK PROBLEM — TCP 5432 connected fine; Postgres rejected the credential. "
                     "This is the SP Postgres role / GRANT step, not networking. [README → gotchas #4, #5]")
if has("OAUTH_BAD_CREDS"):
    diagnoses.append("• CREDENTIALS — Leg A reachable but the SP OAuth client_id/secret were rejected. "
                     "Network is fine; check the secret values and that the SP lives in the Lakebase workspace.")

if not diagnoses:
    if leg_status("A") == "PASS" and leg_status("B") == "PASS":
        diagnoses.append("• ALL CLEAR — both legs reachable and the SELECT succeeded. No network restriction "
                         "is blocking this path.")
    else:
        diagnoses.append("• Inconclusive — some legs were SKIPPED (likely missing SP creds). Add "
                         "sp_client_id / sp_client_secret to the scope and re-run for a full picture.")

for d in diagnoses:
    print(d)

print()
print(f"Leg A (control plane, 443) : {leg_status('A')}")
print(f"Leg B (data path, 5432)    : {leg_status('B')}")

dbutils.notebook.exit(json.dumps({
    "leg_a": leg_status("A"), "leg_b": leg_status("B"),
    "egress_ip": egress_ip,
    "cred_mint_request_id": globals().get("cred_response_request_id") or globals().get("cred_request_id"),
    "results": RESULTS,
}, default=str))
