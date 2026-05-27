# Cross-Workspace Lakebase Access from Classic Compute

Read a [Databricks Lakebase](https://docs.databricks.com/en/database/index.html)
Postgres database that lives in one workspace from a classic-compute notebook
running in a **different** workspace, with no PrivateLink / VPC peering / Unity
Catalog federation.

## What this shows

A notebook in workspace **A** (compute) authenticates as a Databricks service
principal, performs **two OAuth credential mints** against workspace **B**'s
control plane, and then opens a direct Postgres connection to the Lakebase
endpoint hosted by workspace B.

The setup is deliberately minimal — the goal is to make every moving part
visible so you can decide which parts to harden, productionize, or replace for
your own deployment.

## When to use this pattern

- You operate **separate workspaces for analytics and OLTP** (one for Spark
  workloads, one hosting Lakebase) and need analytics notebooks to read
  application data without going through Unity Catalog federation.
- You're **prototyping cross-workspace data access** before committing to a
  PrivateLink-backed architecture.
- You want a **service-principal-driven** read path (no user OAuth, no PATs).

## When *not* to use this pattern

- For **high-volume analytical reads**, use the [Lakebase Unity Catalog
  integration](https://docs.databricks.com/en/database/uc-integration.html)
  (federated catalog + synced tables) instead — purpose-built for that.
- If the workspaces are in **different Databricks accounts**, this exact flow
  doesn't apply; SP m2m OAuth is account-scoped.
- For **strict-egress** environments where the compute cluster cannot reach
  `*.database.cloud.databricks.com` over the public internet — you'll need
  PrivateLink + customer-managed networking instead.

## Repo layout

```
.
├── README.md                              this file
├── GUIDE.md                               end-to-end setup walkthrough
├── LICENSE                                Apache 2.0
└── notebooks/
    └── cross_ws_lakebase_test.py          drop into your compute workspace,
                                           attach to any classic cluster
```

## Quick start

1. Read [`GUIDE.md`](GUIDE.md) — it walks through:
   - Creating the SP and its OAuth client secret
   - Provisioning the SP's Postgres role on the Lakebase branch
   - Stashing creds in a secret scope
   - The notebook code itself
   - Seven gotchas we hit (none of them are in the public docs)
2. Run `notebooks/cross_ws_lakebase_test.py` on a classic cluster in your
   compute workspace.

## Architecture at a glance

```
Compute workspace                          Lakebase workspace
┌──────────────────┐                       ┌─────────────────────┐
│  Notebook        │── m2m OAuth ─────────▶│ /oidc/v1/token      │
│  (classic        │◀──── access_token ────│                     │
│   cluster)       │                       │                     │
│                  │── mint DB creds ─────▶│ /api/2.0/postgres/  │
│                  │◀──── DB token  ───────│  credentials        │
│                  │                       │                     │
│                  │── psycopg2.connect ──▶│ Lakebase Postgres   │
│                  │◀──── SELECT rows ─────│ (ep-…:5432)         │
└──────────────────┘                       └─────────────────────┘
       │
       └── reads SP client_id + client_secret from a Databricks secret scope
```

See [`GUIDE.md`](GUIDE.md) for proper Mermaid diagrams of the call sequence
and the credential mint chain.

## What you'll need

- Two Databricks workspaces on the same cloud + region + account. AWS verified;
  the pattern should generalize to Azure/GCP but token endpoints differ
  slightly.
- Databricks CLI **v0.285.0+** (Lakebase autoscaling tier APIs).
- A Lakebase autoscaling project in the Lakebase workspace whose `production`
  branch is in state `READY`.
- A classic compute cluster (job or interactive, DBR 13+) in the compute
  workspace — single-user mode tested.

## License

[Apache 2.0](LICENSE). Use freely; no warranty.
