"""Offline harness: execute the netdiag notebook with a stubbed Databricks runtime.

Replays a real-world scenario to verify CONTROL FLOW (not just pure helpers):
  OAuth 200 -> cred mint 200 -> endpoints 403 (PL phrase in BODY, no header)
Asserts: PL_INGRESS is diagnosed, and Leg B is still exercised via DB_HOST_OVERRIDE.
"""
import json
import socket
import sys
import types

import os
NB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                  "notebooks", "cross_ws_lakebase_netdiag.py")

SCENARIO = sys.argv[1] if len(sys.argv) > 1 else "pl_on_listing"
OVERRIDE = sys.argv[2] if len(sys.argv) > 2 else "ep-test-abc123.database.us-east-1.cloud.databricks.com"

WS_HOST = "https://analytics-target.cloud.databricks.com"
ENDPOINT = "projects/demo-project/branches/production/endpoints/primary"


# ---- fake dbutils -----------------------------------------------------------
class _Widgets:
    def __init__(self):
        self.vals = {
            "lakebase_workspace_host": WS_HOST,
            "lakebase_endpoint": ENDPOINT,
            "secret_scope": "cross-ws-lakebase",
            "db_host_override": OVERRIDE,
        }

    def text(self, name, default="", label=""):
        self.vals.setdefault(name, default)

    def get(self, name):
        return self.vals.get(name, "")


class _Secrets:
    STORE = {"sp_client_id": "aaaaaaaa-1111-2222-3333-444444444444",
             "sp_client_secret": "dose-not-matter"}

    def get(self, scope, key):
        if key in self.STORE:
            return self.STORE[key]
        raise Exception(f"no secret {key}")


class _NotebookExit(Exception):
    def __init__(self, payload):
        self.payload = payload


class _Notebook:
    def exit(self, s):
        raise _NotebookExit(s)


class _DBUtils:
    def __init__(self):
        self.widgets = _Widgets()
        self.secrets = _Secrets()
        self.notebook = _Notebook()


# ---- fake requests ----------------------------------------------------------
class FakeResp:
    def __init__(self, status, body="", headers=None):
        self.status_code = status
        self.text = body
        self.headers = headers or {}
        self.ok = 200 <= status < 400

    def json(self):
        return json.loads(self.text)


PL_BODY = ('{"X-Databricks-Reason-Phrase":"Unauthorized private link access to '
           'workspace: 1234567890123456"}')

CALLS = []


def fake_request(method, url, **kw):
    CALLS.append((method, url))
    # egress-IP echo services
    if "checkip" in url or "ipify" in url or "ifconfig" in url:
        return FakeResp(200, "203.0.113.9")
    if url.endswith("/oidc/.well-known/oauth-authorization-server"):
        return FakeResp(200, "{}")
    if url.endswith("/oidc/v1/token"):
        return FakeResp(200, json.dumps({"access_token": "wstok", "expires_in": 3600}))
    if url.endswith("/api/2.0/postgres/credentials"):
        if SCENARIO == "mint_fails":
            return FakeResp(403, PL_BODY)
        return FakeResp(200, json.dumps({"token": "dbtok", "expire_time": "2026-08-05T22:00:00Z"}))
    if "/endpoints" in url:
        if SCENARIO == "pl_on_listing":
            return FakeResp(403, PL_BODY)          # <-- the observed failure
        if SCENARIO == "authz":
            return FakeResp(403, '{"error_code":"PERMISSION_DENIED","message":"no access"}')
        return FakeResp(200, json.dumps({"endpoints": [
            {"name": ENDPOINT, "status": {"hosts": {"host": OVERRIDE or "ep-x.database.us-east-1.cloud.databricks.com"}}}]}))
    if "api.database" in url:
        return FakeResp(200, "{}")
    return FakeResp(200, "{}")


fake_requests = types.ModuleType("requests")
fake_requests.request = fake_request
fake_requests.get = lambda url, **kw: fake_request("GET", url, **kw)
fake_requests.post = lambda url, **kw: fake_request("POST", url, **kw)


class _Exc(Exception):
    pass


class _Timeout(_Exc):
    pass


class _ConnErr(_Exc):
    pass


fake_requests.exceptions = types.SimpleNamespace(
    Timeout=_Timeout, ConnectionError=_ConnErr, RequestException=_Exc, SSLError=_Exc)

# ---- fake psycopg2 ----------------------------------------------------------
fake_pg = types.ModuleType("psycopg2")


class _Cur:
    description = [types.SimpleNamespace(name="col")]

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return ("sp-user", "postgres", "PostgreSQL 16.0")

    def fetchall(self):
        return [("v",)]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    autocommit = True

    def cursor(self):
        return _Cur()

    def close(self):
        pass


fake_pg.connect = lambda **kw: _Conn()
fake_pg.OperationalError = type("OperationalError", (Exception,), {})
fake_pg.Error = type("Error", (Exception,), {})

# ---- fake socket (DNS + TCP) ------------------------------------------------
real_getaddrinfo = socket.getaddrinfo


def fake_getaddrinfo(host, port, *a, **k):
    # target workspace resolves PRIVATE -> the PrivateLink topology
    if "analytics-target" in host:
        return [(2, 1, 6, "", ("10.20.30.40", 443))]
    if "service-direct" in host:
        raise socket.gaierror("NXDOMAIN")   # missing service-direct DNS
    return [(2, 1, 6, "", ("52.1.2.3", port or 443))]


class _Sock:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass

    def settimeout(self, *a):
        pass


def fake_create_connection(addr, timeout=None, **k):
    return _Sock()


socket.getaddrinfo = fake_getaddrinfo
socket.create_connection = fake_create_connection

# ---- run --------------------------------------------------------------------
sys.modules["requests"] = fake_requests
sys.modules["psycopg2"] = fake_pg

src = open(NB).read()
g = {"__name__": "__main__", "dbutils": _DBUtils(), "spark": None,
     "display": lambda *a, **k: None}

payload = None
try:
    exec(compile(src, NB, "exec"), g)
except _NotebookExit as e:
    payload = e.payload
except Exception as e:
    print(f"\n!!! HARNESS: notebook raised {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 78)
print(f"HARNESS ASSERTIONS  (scenario={SCENARIO}, override={'set' if OVERRIDE else 'empty'})")
print("=" * 78)
res = g.get("RESULTS", [])
classes = {r["error_class"] for r in res if r["error_class"]}
steps = {r["step"]: r["status"] for r in res}
print("error_classes:", sorted(classes))

ok = True


def check(label, cond):
    global ok
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    ok = ok and cond


if SCENARIO == "pl_on_listing":
    check("PL_INGRESS classified from body-only phrase", "PL_INGRESS" in classes)
    check("no bogus IP_ACL diagnosis", "IP_ACL" not in classes)
    check("cred mint recorded PASS", steps.get("Mint Lakebase DB credential") == "PASS")
    if OVERRIDE:
        check("Leg B exercised despite endpoints 403",
              any(r["leg"] == "B" and r["status"] in ("PASS", "FAIL") for r in res))
        check("psycopg2 step not SKIPped",
              steps.get("psycopg2 connect + SELECT") != "SKIP")
elif SCENARIO == "authz":
    check("no-phrase 403 -> HTTP_403 (not a network class)", "HTTP_403" in classes)
    check("not misfiled as PL", "PL_INGRESS" not in classes)
elif SCENARIO == "mint_fails":
    check("PL_INGRESS on the mint call", "PL_INGRESS" in classes)

if payload:
    try:
        j = json.loads(payload)
        check("exit payload is valid JSON", True)
        check("payload carries no token values",
              "wstok" not in payload and "dbtok" not in payload)
        print(f"  legs: A={j.get('leg_a')} B={j.get('leg_b')}")
    except Exception as e:
        check(f"exit payload JSON ({e})", False)

print("=" * 78)
print("HARNESS:", "ALL PASS" if ok else "FAILURES ABOVE")
sys.exit(0 if ok else 1)
