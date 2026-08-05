# Offline test harness

`harness_netdiag.py` executes `notebooks/cross_ws_lakebase_netdiag.py` against a
stubbed Databricks runtime (`dbutils`, `requests`, `psycopg2`, `socket`), so the
notebook's control flow and 403 classification can be verified without a
workspace, credentials, or network access.

```bash
python3 tests/harness_netdiag.py <scenario> [db_host_override]
```

| Scenario | Simulates | Asserts |
|---|---|---|
| `happy` | every call returns 200 | both legs PASS |
| `pl_on_listing` | OAuth 200 → credential mint 200 → endpoint listing 403 carrying the PrivateLink reason phrase **in the body**, not the header | classified `PL_INGRESS`, never `IP_ACL`; Leg B still exercised via the override |
| `mint_fails` | PrivateLink rejection on the credential mint instead | classified `PL_INGRESS` (classification keys on the phrase, not on which call failed) |
| `authz` | 403 with no network reason phrase | classified `HTTP_403`, not a network layer |

Pass an empty second argument to test without `db_host_override`:

```bash
python3 tests/harness_netdiag.py pl_on_listing ""
```

Every scenario exits non-zero on assertion failure. The harness also checks that
the notebook's exit payload is valid JSON and contains no token values.

Note the stubs are deliberately thin — they verify decision logic, not wire
behaviour. Anything involving real TLS, DNS or Postgres semantics still needs a
live run.
