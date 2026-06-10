# ccloud-rbac

A Go CLI that fetches **identity providers, identity pools, role bindings**
and related resources from the [Confluent Cloud APIs](https://docs.confluent.io/cloud/current/api.html)
and exports them into a single **self-contained interactive HTML report**, so
you can search, sort, group and filter to find out *who has read / write /
admin access to which Confluent Cloud resources*.

No database, no server, no external assets — the output is one `.html` file
you can open locally, attach to an audit ticket, or drop on a static share.

---

## Quick start

```bash
# Build (requires Go 1.22+; developed on 1.26)
go build -o ccloud-rbac .

# 1. Try it immediately with built-in mock data — no credentials needed:
./ccloud-rbac --mock -o report.html
open report.html        # macOS; use xdg-open on Linux

# 2. Run against your real Confluent Cloud organization:
export CONFLUENT_CLOUD_API_KEY=XXXXXXXXXXXXXXXX
export CONFLUENT_CLOUD_API_SECRET=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY
./ccloud-rbac -o report.html
```

## Usage

```
ccloud-rbac [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--mock` | `false` | Use built-in sample data ("Acme Corp") instead of the real API. No credentials needed. Starts an in-process fake API server, so the exact same fetch code path runs as in real mode. |
| `--api-key` | `$CONFLUENT_CLOUD_API_KEY` | Confluent **Cloud API key** (not a Kafka cluster API key). |
| `--api-secret` | `$CONFLUENT_CLOUD_API_SECRET` | Secret for the Cloud API key. |
| `--base-url` | `https://api.confluent.cloud` | API base URL. Override for testing or non-standard endpoints. |
| `-o` | `ccloud-rbac-report.html` | Output HTML file path. |
| `--json` | *(off)* | Also write the raw API snapshot as pretty-printed JSON to this path — useful for `jq` post-processing or diffing access over time. |
| `--timeout` | `5m` | Overall timeout for fetching all resources. |

Progress and warnings go to **stderr**; the only files written are the ones
you name with `-o` / `--json`. Exit code is non-zero on any failure.

### Examples

```bash
# Mock report + raw JSON snapshot
./ccloud-rbac --mock -o report.html --json snapshot.json

# Real org, flags instead of env vars
./ccloud-rbac --api-key XXXX --api-secret YYYY -o acme-rbac.html

# Who are all the principals? (from the JSON snapshot)
jq '.role_bindings[] | .principal' snapshot.json | sort -u

# Weekly audit snapshot, dated filename
./ccloud-rbac -o "rbac-$(date +%F).html" --json "rbac-$(date +%F).json"
```

### Credentials: what kind of API key?

You need a **Cloud API key** (organization-level), *not* a cluster-scoped
Kafka API key. Create one in the Confluent Cloud console under
**Settings → API keys → Add key → Cloud resource management**, or:

```bash
confluent api-key create --resource cloud
```

The key's owner needs permission to read IAM and org metadata — any of
`OrganizationAdmin`, or a read-capable admin role covering the org, works.
A least-privilege auditor identity needs read access to: organizations,
environments, clusters, users, service accounts, identity providers/pools,
and role bindings. If some role bindings are invisible to your key, they
will simply be missing from the report.

The tool is strictly **read-only**: it only issues GET requests.

---

## The HTML report

One file, four tabs, all interactive (vanilla JS, works offline):

### Overview
Stat cards (bindings, users, service accounts, providers, pools,
environments, clusters, admin principals), an **"Admin access at a
glance"** table listing every principal holding an admin-level role and
what they are admin over, and an environments-and-clusters inventory.

### Role Bindings — the main audit view
Every grant in the organization, one row per role binding:

- **Free-text search** across principal name/ID, role, resource, environment,
  cluster and the raw CRN pattern.
- **Filters**: access level (admin/write/read/other), principal type (users /
  service accounts / identity pools / SSO groups), scope (organization /
  environment / cluster / resource), environment, role.
- **Group by** principal, role, access level, environment, or resource type —
  e.g. group by *principal* to see each identity's full grant list, or by
  *access level* to review all admin grants together.
- **Sort** by clicking any column header (click again to reverse).
- Each row shows the resolved names *and* the raw `crn_pattern`, so wildcard
  grants like `topic=orders-*` are visible at a glance.

Typical questions and how to answer them:

| Question | How |
|----------|-----|
| Who can write to the `payments.transactions` topic? | Search `payments`, filter Access: write (and admin — admins can too) |
| What can service account `sa-orders1` do? | Search `sa-orders1`, or Principals tab → click the row |
| Who is admin over production? | Filter Environment: production + Access: admin |
| What do our CI/CD pipelines have access to? | Filter Principal: identity pools, or search the pool ID |
| Any org-wide grants? | Filter Scope: organization |

### Principals
Every user, service account and identity pool — **including identities with
zero grants** (dormant accounts) and *orphaned principals* that appear in a
role binding but no longer resolve to a listed identity (e.g. deleted users).
Shows binding count and **max access level** per principal; click a row to
expand its complete binding list.

### Identity Providers
Provider cards (issuer, JWKS URI) with their nested identity pools, each
showing the `identity_claim`, the CEL `filter` expression, and a
**"view bindings"** link that jumps to the Role Bindings tab pre-filtered to
that pool.

---

## How the Confluent Cloud entities relate

```
IdentityProvider (op-...)            external OIDC issuer (Okta, Azure AD, ...)
  └── IdentityPool (pool-...)        claims-based group of external identities
                                     (identity_claim + CEL filter)

User (u-...)              ┐
ServiceAccount (sa-...)   ├── are "principals" referenced as User:<id>
IdentityPool (pool-...)   ┘
        │
        ▼
RoleBinding (rb-...) = principal + role_name + crn_pattern
        │
        ▼
crn_pattern scopes the grant:
  crn://confluent.cloud/organization=<org>                          org scope
    /environment=<env-id>                                           env scope
      /cloud-cluster=<lkc-id>                                       cluster scope
        /kafka=<lkc-id>/topic=orders-*                              resource scope
        /kafka=<lkc-id>/group=...        (consumer groups)
        /kafka=<lkc-id>/transactional-id=...
      /schema-registry=<lsrc-id>/subject=...
```

Roles are mapped to coarse access levels for filtering:

| Access | Roles |
|--------|-------|
| admin  | OrganizationAdmin, EnvironmentAdmin, CloudClusterAdmin, ResourceOwner, KsqlAdmin, NetworkAdmin, FlinkAdmin, AccountAdmin, BillingAdmin |
| write  | DeveloperWrite, DeveloperManage, DataSteward, FlinkDeveloper, Accountant |
| read   | DeveloperRead, MetricsViewer, Operator, DataDiscovery |
| other  | any role not in the predefined map (rendered, filterable as "other") |

## Endpoints used

| Endpoint | Purpose |
|----------|---------|
| `GET /org/v2/organizations` | organization ID (root of all CRNs) |
| `GET /org/v2/environments` | environment names |
| `GET /cmk/v2/clusters?environment=<id>` | cluster names per environment |
| `GET /iam/v2/users` | human principals |
| `GET /iam/v2/service-accounts` | machine principals |
| `GET /iam/v2/identity-providers` | OIDC providers |
| `GET /iam/v2/identity-providers/{id}/identity-pools` | pools per provider |
| `GET /iam/v2/role-bindings?crn_pattern=...` | the actual grants |

All list endpoints are paginated (`page_size` / `page_token` via
`metadata.next` links); the client follows every page. Role bindings are
listed with the org-scope pattern plus an org-level `/*` wildcard; if the
API rejects the wildcard, the tool automatically falls back to querying each
environment subtree and de-duplicates by binding ID. If even environment
wildcards are rejected, it keeps what the exact-scope queries returned and
prints a warning about possible gaps.

## Mock data

`--mock` serves a realistic sample organization, **Acme Corp**:

- 2 identity providers (Okta Production, Azure AD CI/CD) with 4 pools
  (data-engineering, analytics-team, cicd-prod-deployer, flink-jobs)
- 8 users, 6 service accounts
- 3 environments (dev / staging / production), 4 Kafka clusters
- 28 role bindings spanning every scope: org-wide admin, environment admins,
  cluster admins, topic literals and `*`-prefix patterns, consumer groups,
  schema-registry subjects and transactional IDs

To customize the sample, edit `internal/mock/data.go` and rebuild.

## Limitations

- Principals of type `User:group-...` (SSO group mappings) appearing in role
  bindings are rendered as "SSO group", but the group-mappings endpoint
  itself is not fetched yet.
- The report shows **direct role bindings only**; it does not expand what
  each role implies into per-operation ACL semantics.
- Wildcard `crn_pattern` listing behavior is implemented per the docs but
  was developed without live credentials; the fallback path covers APIs
  that reject wildcards.

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `no credentials: pass --api-key/--api-secret ...` | Set `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET`, or use `--mock`. |
| `... returned 401 ...` | Wrong key/secret, or a Kafka cluster key instead of a Cloud key. |
| `... returned 403 ...` | The key's owner lacks read access to that endpoint; bindings it cannot see will be absent. |
| `warning: crn_pattern ... rejected` | The API refused a wildcard listing; the report may be missing sub-environment bindings (see Endpoints used). |
| Report opens but is empty | Check stderr counts from the run; `--json` the snapshot and inspect it. |

## Development

```bash
go test ./...                              # unit + mock-server integration tests
go test -coverpkg=./internal/... ./...     # ~87% aggregate coverage
go vet ./... && gofmt -l .
```

Layout:

```
main.go, snapshot.go        CLI entrypoint and JSON snapshot writer
internal/ccloud/            API types, paginated read-only client, fetch pipeline
internal/mock/              Acme Corp sample data + in-process fake API server
internal/model/             CRN parsing, role→access mapping, report model
internal/report/            HTML rendering (template.html, embedded via go:embed)
```

The generated HTML embeds the report model as JSON inside a
`<script type="application/json">` block; all interactivity is plain
JavaScript in `internal/report/template.html`.
