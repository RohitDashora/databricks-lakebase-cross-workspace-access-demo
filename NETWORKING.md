# Cross-Workspace Lakebase — Networking Guide

When the basic read works on an open workspace pair but **fails or hangs in a
locked-down environment** (IP access lists, PrivateLink, egress firewalls), use
this guide. It explains *which* network hop each restriction breaks and exactly
what to configure.

Pair it with [`notebooks/cross_ws_lakebase_netdiag.py`](notebooks/cross_ws_lakebase_netdiag.py) —
run that in your compute workspace and it tells you which leg is failing and why.

> **Cloud scope:** Lakebase is GA on **AWS** and **Azure** only. It is **not
> available on GCP** today — none of this applies there. AWS examples are
> primary; Azure differences are called out inline.

---

## The two-leg model (read this first)

The flow looks like one operation but is **two independent network paths with
different hostnames, ports, and governing controls.** Almost every config
decision falls out of keeping them separate.

| Leg | Purpose | Destination | Port |
|-----|---------|-------------|------|
| **OIDC** | mint the workspace OAuth token | the **workspace** URL (`<name>.cloud.databricks.com` / `adb-*.azuredatabricks.net`) | 443 |
| **A — control plane** | mint the Lakebase DB credential, resolve the endpoint host | `api.database.<region>.cloud.databricks.com` (the Lakebase management API) | 443 |
| **B — data path** | the actual Postgres connection + `SELECT` | `ep-*.database.<region>.cloud.databricks.com` | **5432** |

Key facts that drive everything below:

- **Legs A and B share one regional ingress**, `*.database.<region>.cloud.databricks.com`
  — distinct from the workspace URL. Allowlisting the workspace URL alone is **not** enough.
- **Leg B is TCP 5432**, not 443. Firewalls that allow only 443 silently kill it.
- **Provisioned vs Autoscaling differ.** Autoscaling endpoints look like
  `ep-*.database.<region>.cloud.databricks.com` and use the *Service-Direct*
  private path (below). Provisioned instances look like
  `instance-<uuid>.database.cloud.databricks.com` and ride standard front-end
  PrivateLink. Confirm which tier you're on.

---

## Authentication and network reachability are different layers

A frequent point of confusion: *"I'm authenticating with a service principal
client_id/secret — why does the cluster's access mode matter?"*

Because they govern different things. Credentials decide **who you are**; the
network path decides **whether your packets can even get to the database**. Auth
can't help if the connection never leaves the cluster.

`psycopg2.connect()` happens in this order:

```
1. open TCP socket to ep-*.database...:5432   ← cluster access mode + firewall + PrivateLink gate THIS
2. TLS handshake
3. send credentials (user = SP applicationId, password = Lakebase DB token)  ← auth happens HERE
4. Postgres accepts / rejects the login + checks GRANTs
```

- **Steps 1–2 are the network layer** — governed by cluster access mode, egress
  firewall, IP access lists, and PrivateLink. If this fails you get a **TCP
  timeout/refused** and your credentials are never even presented.
- **Steps 3–4 are the auth layer** — governed by your SP OAuth credentials and
  the Postgres role/GRANTs. If this fails you get **`password authentication
  failed`** or **`permission denied`**.

You need **both**: an open road (steps 1–2) *and* the right key (steps 3–4).
Valid credentials over a blocked network look exactly like a firewall problem;
a perfect network with a missing GRANT looks exactly like an auth problem. The
diagnostic notebook tests them as separate probes for exactly this reason —
`TCP_TIMEOUT` on Leg B is a network finding, `PG_AUTH_FAILED` is not.

**Analogy:** the client_id/secret is the key to the building. A Standard/Shared
cluster (below) is the road to the building being closed — the key is irrelevant
if you can't drive there.

## Prerequisite that masquerades as a network bug

**Standard / Shared (`USER_ISOLATION`) classic clusters block arbitrary outbound
TCP — including 5432** — no matter how the network is configured or how valid
your credentials are. This is a multi-tenancy isolation feature: user code on a
shared cluster isn't allowed to open arbitrary network sockets. It is **not**
Lakebase-specific. Leg B only works from:

- a **Dedicated / single-user** classic cluster, or
- **serverless** compute.

If `psycopg2` can't reach 5432 but the rest of your network looks fine, check the
cluster's access mode **first**. The diagnostic notebook flags this in Probe 0
(it reads the mode from the Clusters API).

---

## Restriction → leg impact matrix

| Restriction (where it lives) | Hits OIDC/Leg A? | Hits Leg B (5432)? | The lever |
|---|---|---|---|
| **IP access list** (Lakebase workspace) | Yes — on the **public path** only | Yes — on the **public path** only | Allowlist the compute cluster's egress IP. PrivateLink (private-IP) traffic is exempt from IP ACLs entirely. |
| **Front-end PrivateLink + public access disabled** (Lakebase workspace) | Yes — public traffic rejected | Yes | Caller's VPC needs a **private route** to the Lakebase workspace; cross-workspace is **not** automatic (see below). |
| **Service-Direct PrivateLink not set up** (for private Leg B) | — | Yes, if you require a private 5432 path | Provision Service-Direct (perf-intensive) PrivateLink — separate from front-end PL. |
| **Back-end PrivateLink / SCC** (compute workspace) | Indirect — removes public inbound; egress still via your VPC | same | Egress is governed by your VPC (NAT vs VPC endpoint), **not** by SCC and **not** by NCC. |
| **Egress firewall / proxy** (compute workspace) | needs 443 to workspace URL + `api.database.*` | needs **5432** to `ep-*.database.*` | Open both; 5432 is the usual omission. |

> **IP ACLs only apply to public-internet (public-IP) traffic.** Traffic that
> arrives over PrivateLink presents a private IP and is **not** subject to the
> IP access list. This is why "move to PrivateLink" and "allowlist my NAT IP"
> are two different, mutually-exclusive fixes for the same 403.

---

## Signature → diagnosis → fix

These map 1:1 to the `error_class` values the diagnostic notebook emits.

| Notebook signature | What it means | Fix |
|---|---|---|
| `DNS_NXDOMAIN` | a hostname won't resolve | Under PrivateLink you need **private DNS** for `*.database.<region>.cloud.databricks.com` (Route 53 private hosted zone / Azure private DNS zone). On the public path, check your resolver isn't filtering. |
| `HTTP_403_IP_ACL` (Leg A) | `Source IP ... is blocked by Databricks IP ACL` | Add the cluster's **egress IP** (printed by the notebook's Probe 1) to the Lakebase workspace IP access list. |
| `HTTP_TIMEOUT` / `HTTP_CONN_ERROR` (Leg A) | control-plane calls hang or reset | Either front-end PrivateLink with public access disabled and **no private route from this VPC**, or an egress firewall blocking 443. Provision a private route (front-end PL endpoint) or open 443 egress. |
| `OAUTH_BAD_CREDS` | Leg A reachable, creds rejected (401/400) | **Not a network problem.** Check `client_id`/`client_secret` and that the SP exists in the Lakebase workspace. |
| `TCP_TIMEOUT` / `TCP_REFUSED` / `TCP_OSERROR` (Leg B) | can't reach 5432 | (1) Shared cluster? switch to dedicated/serverless. (2) Egress firewall blocking 5432 → open it to `ep-*.database.<region>.cloud.databricks.com`. (3) Need a private path → Service-Direct PrivateLink. |
| `PG_SSL_ERROR` | TLS negotiation broke on 5432 | A TLS-intercepting forward proxy is breaking Postgres SSL (it can't be MITM'd like HTTPS). Bypass the proxy for the DB endpoint. (Squid: add `hosts_file /etc/hosts` and allow `*.database.*.cloud.databricks.com`.) |
| `PG_AUTH_FAILED` | TCP 5432 connected, Postgres rejected the login | **Network is fine.** This is the SP Postgres role / GRANT step — see the main [README](README.md) gotchas #4 and #5. |

---

## Config recipes by restriction

### 1. IP access list on the Lakebase workspace

The cluster reaches Lakebase over the **public path** (NAT gateway → internet),
so its public egress IP must be allowlisted.

1. Run the diagnostic — Probe 1 prints the egress IP (your NAT gateway EIP under
   SCC/back-end PL).
2. Add that IP (or your NAT EIP CIDR) to the Lakebase workspace's IP access list.
3. If you prefer to allowlist by destination instead of being allowlisted, note
   that IP ACLs are a property of the *destination* (Lakebase) workspace — the
   caller can't opt out; the Lakebase admin must add your IP.

Docs: [IP access lists (AWS)](https://docs.databricks.com/aws/en/security/network/front-end/ip-access-list).

### 2. Front-end PrivateLink + public access disabled (Lakebase workspace)

Cross-workspace access is **not automatic** just because both workspaces are in
the same Databricks account. Two independent requirements:

- **Authorization** — the OAuth token grants identity; it works cross-workspace.
- **Reachability** — with public access disabled, the caller's traffic must
  arrive over a **private route**. The compute VPC needs its own interface
  endpoint (or a peered/private route) to the Lakebase workspace's front-end
  PrivateLink service, and that endpoint must be permitted in the Lakebase
  workspace's Private Access Settings.

Docs: [Configure front-end PrivateLink (AWS)](https://docs.databricks.com/aws/en/security/network/front-end/front-end-private-connect).

### 3. Private path for Leg B — Service-Direct PrivateLink

To carry the **5432 data path** privately (no internet), Lakebase Autoscaling
uses **Service-Direct PrivateLink** ("inbound Private Link for performance-
intensive services") — a **separate** inbound endpoint from classic front-end
PrivateLink, and **not** an NCC rule.

- **AWS:** create a **VPC interface endpoint** to the region's Service-Direct
  service, attach it via Private Access Settings, and configure DNS so
  `<region>.service-direct.privatelink.cloud.databricks.com` resolves to the
  endpoint. **Limitation:** if the endpoint is in the *same VPC* as the
  workspace, only 80/443/53 are allowed — you need a **separate VPC** to carry
  5432.
- **Azure:** create a **Private Endpoint** to the Service-Direct resource
  (sub-resource `service_direct`), register it (must reach APPROVED), and add
  the A record in the `privatelink.azuredatabricks.net` private DNS zone.
- Requires Enterprise (AWS) / Premium (Azure) tier and the feature enabled at
  the account level. **Currently Public Preview.**
- **Provisioned** instances don't need this — they ride standard front-end PL.

Docs: [Service-Direct PrivateLink (AWS)](https://docs.databricks.com/aws/en/security/network/front-end/service-direct-privatelink) ·
[Service-Direct (Azure)](https://learn.microsoft.com/en-us/azure/databricks/security/network/front-end/service-direct-privatelink) ·
[Private Link for Lakebase (Azure)](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/private-link).

### 4. Back-end PrivateLink / SCC on the compute workspace

SCC removes the cluster's public **inbound** IP and routes cluster↔control-plane
traffic privately. It does **not** by itself give you a path to Lakebase. Egress
to Lakebase is governed by your **customer-managed VPC**:

- **Public path:** security groups + route tables + **NAT gateway** → reaches
  443 and 5432 on `*.database.<region>.*`; the Lakebase IP ACL then sees the NAT
  EIP and must allow it.
- **Private path:** the **Service-Direct VPC endpoint** (recipe 3).
- **NCC** governs *serverless* egress, not classic compute — don't reach for NCC
  to fix a classic-cluster Lakebase path.

### 5. Egress firewall / domain allowlist

Allow these from the compute workspace's egress (firewall / proxy):

| Destination | Port | For |
|---|---|---|
| your **workspace URL** (`<name>.cloud.databricks.com` / `adb-*.azuredatabricks.net`) | 443 | OIDC token mint |
| `*.database.<region>.cloud.databricks.com` | 443 | Leg A (Lakebase management API) |
| `*.database.<region>.cloud.databricks.com` | **5432** | Leg B (Postgres) |
| (Azure also) `*.database.<region>.azuredatabricks.net` | 443 + 5432 | both legs on Azure |

- If your firewall filters by IP, allowlist the per-region **Lakebase inbound IP
  ranges** from the public [IP addresses and domains](https://docs.databricks.com/aws/en/resources/ip-domain-region)
  page (example: us-east-1 `18.97.15.0/28`, us-west-2 `18.98.3.224/28`).
- `sslmode=require` is mandatory.
- **Squid proxy gotcha:** Squid ignores `/etc/hosts` for HTTPS `CONNECT`
  tunnels — add `hosts_file /etc/hosts` to `squid.conf`, and include
  `*.database.*.cloud.databricks.com` in the domain ACL, or PrivateLink DNS is
  bypassed.

---

## Three architecture paths (pick per environment)

| Path | How | Best when |
|------|-----|-----------|
| **1 — Public endpoint** (what the demo does) | NAT egress to the public Lakebase endpoint; allowlist the NAT IP on the workspace IP ACL | Egress is open or you can allowlist a stable NAT EIP |
| **2 — Private endpoint** | Service-Direct PrivateLink (AWS/Azure) so Leg B never touches the internet | Public access disabled, strict-egress, regulated workloads |
| **3 — Unity Catalog instead of a raw socket** | Register the Lakebase database in UC (read-only catalog) and query via a SQL warehouse, or use Lakehouse Federation | You'd rather not open 5432 from the caller at all; you want UC governance |

Path 3 moves the network path onto the UC connection's compute (often serverless,
NCC-governed) rather than your classic cluster:
[Register a Lakebase database in UC](https://docs.databricks.com/aws/en/oltp/projects/register-uc) ·
[Lakehouse Federation for PostgreSQL](https://docs.databricks.com/aws/en/query-federation/postgresql) ·
[Synced tables (reverse-ETL, for context)](https://docs.databricks.com/aws/en/oltp/projects/sync-tables).

---

## Operational notes

- **Token lifetimes:** workspace OAuth token and Lakebase DB credential are each
  ~1 hour. Re-mint inside long jobs.
- **Token-mint rate limit:** per-connection credential mints hit a SCIM
  (~1k/workspace) limit. For anything beyond a demo, **cache the OAuth token and
  pool connections** (e.g. PgBouncer) rather than minting per query.
- **Azure OAuth caveat:** OAuth requires **per-workspace URLs**. If the
  subscription is on the per-workspace-URL opt-out list (canonical name
  `<region>.azuredatabricks.net` instead of `adb-<id>.<n>.azuredatabricks.net`),
  OAuth breaks — remove it from the opt-out list.
- **Auth flow parity:** the SP m2m flow (`client_id`/`client_secret` →
  `/oidc/v1/token` → DB credential) is identical on AWS and Azure; only the
  workspace host differs. See [Lakebase authentication](https://docs.databricks.com/aws/en/oltp/projects/authentication).

---

## How to test in your own environment

1. Set up the SP, secret scope, and Postgres role per the [README](README.md).
2. Import [`notebooks/cross_ws_lakebase_netdiag.py`](notebooks/cross_ws_lakebase_netdiag.py)
   into your **compute** workspace.
3. Attach it to a **dedicated / single-user** classic cluster (not Shared).
4. Run it. Read the per-leg verdict and the `DIAGNOSIS` block.
5. Match each `error_class` to the [signature table](#signature--diagnosis--fix)
   above and apply the corresponding recipe.

The notebook is read-only and safe to run anywhere.
