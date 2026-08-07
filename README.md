# Elephant repository

![Image](docs/elephant.png)

Elephant repository is a [NewsDoc](https://github.com/ttab/newsdoc) document
repository with versioning, ACLs for permissions, archiving, validation
schemas, workflow statuses, event output, and metrics for observability.

Documents go in through a [Twirp RPC API](https://twitchtv.github.io/twirp/docs/intro.html)
that speaks either [protobuf](https://protobuf.dev/) or plain JSON, are
validated against registered schemas, and are stored in PostgreSQL as
sequentially numbered versions. Every change is emitted on an eventlog that
other systems follow, and is copied to a S3-compatible store as a signed,
chained archive that can be verified independently of the database. A pessimistic
locking API, a per-document ACL model, a workflow and status machinery, a
publish scheduler and per-document metrics sit on top of that core.

PostgreSQL and a S3-compatible store are required. AWS EventBridge can be used
as an additional event sink, which is optional and disabled with
`--no-eventsink`.

## Documentation

| Document | What it settles |
|---|---|
| **README.md** (this document) | Orientation and the working reference: what the repository holds, how to build and run it, what every configuration flag does, and what is still missing. |
| [docs/architecture.md](docs/architecture.md) | How the service is built: the process model, the data flow through every worker, each subsystem, and the API surface. The design authority — start here to change something. |
| [docs/ops.md](docs/ops.md) | The operator's-eye view: dependencies, deployment shape, data flows, bootstrap order, and the failure modes with the signal that shows each one. |
| [docs/observability.md](docs/observability.md) | Every metric the service exports and what a change in it means. |
| [docs/permissions.md](docs/permissions.md) | The per-RPC permission matrix: which scopes each method accepts and which ACL check it applies. |
| [docs/logs.md](docs/logs.md) | How log metadata works and when to use it instead of returning error detail. |
| [docs/adr/](docs/adr/) | Decision records for the signed archive chain and the event outbox. |

Relative links and heading anchors across all markdown files are checked with:

```shell
mage docs:links
```

## Repository layout

```
cmd/repository/       The binary. CLI flags and the wiring of every subsystem.
repository/           Everything else: RPC services, the Postgres store,
                      archiver, eventlog builder, validator, workflows,
                      scheduler, SSE and websocket handlers.
sinks/                Event sink implementations (AWS EventBridge) and the
                      forwarder that feeds them.
postgres/             query.sql plus the sqlc-generated Queries struct.
schema/               Numbered tern migrations and the embedded migration FS.
internal/             Migration runner, CLI flag helpers, test backing services.
magefiles/            Task runner targets.
testdata/             Test fixtures and the test server's configuration.
docs/                 The documentation set above.
```

## Build & development tools

The toolchain is Go plus [mage](https://magefile.org/) as the task runner,
[tern](https://github.com/jackc/tern) for migrations, [sqlc](https://sqlc.dev/)
for query compilation, and Docker for the local Postgres and MinIO instances.
Run every mage target from the repository root — they use relative paths.

| Task | Command |
|---|---|
| Run all tests (needs Docker) | `go test ./...` |
| Run one test | `go test ./repository -run TestName` |
| Regenerate test fixtures | `REGENERATE=true go test ./...` |
| Lint | `golangci-lint run --timeout=4m` |
| Format | `golangci-lint fmt` |
| Check documentation links | `mage docs:links` |
| Compile SQL queries after editing `postgres/query.sql` | `mage sql:generate` |
| Apply migrations | `mage sql:migrate` |
| Roll back all migrations | `mage sql:rollback 0` |
| Roll back to a version | `mage sql:rollback 7` |
| Print the local connection string | `mage sql:connString` |
| Grant the reporting role | `mage GrantReporting` |
| Build without cluttering the workspace | `go build -o /tmp/repository ./cmd/repository` |

## Running a local dev instance

Bring it up in this order; each step is independently useful.

**1. Postgres and the database.**

```shell
mage sql:postgres pg16   # only if you don't already have one running
mage sql:db
mage sql:migrate
```

Connect with `psql $(mage sql:connString)`.

**2. MinIO and the buckets.** The archive bucket must exist before the server
starts — the archiver needs it immediately and will exit the process if it
cannot reach it.

```shell
mage s3:minio            # only if you don't already have one running
mage s3:bucket elephant-archive
mage s3:bucket elephant-assets
```

**3. A `.env` file.** These three are needed for MinIO only; production uses
standard AWS credential resolution.

```
S3_ENDPOINT=http://localhost:9000/
S3_ACCESS_KEY_ID=minioadmin
S3_ACCESS_KEY_SECRET=minioadmin
```

JWT validation needs `OIDC_CONFIG` pointed at a provider. `ttrun` resolves it
along with the client credentials from `ttrun.env`.

**4. The server.**

```shell
ttrun -- go run ./cmd/repository run --no-eventsink
```

`--no-eventsink` skips AWS EventBridge, which you almost certainly do not want
locally.

What a missing piece does: **without the archive bucket the process exits within
seconds**, because the archiver's first act is to generate and archive a signing
key and that step runs before the retry machinery exists. Pass `--no-archiver`
if you deliberately want to run without S3. Without MinIO's asset bucket, uploads and
attachment downloads fail but document writes are unaffected. Without an OIDC
provider, every authenticated call fails while
[the five unauthenticated methods](docs/architecture.md#scopes) keep working.

A fresh database has no active schema generation, which means **every document
write fails validation until schemas are registered.** The server does not
install them — that was removed in v1.7.0 — so registration is an
administrative step using the `Schemas` service. Tests install the embedded
schemas for themselves.

### Resetting a local dev environment

```shell
mage sql:rollback 0 && mage sql:migrate
```

Then empty the MinIO buckets. Database and archive have to be reset together:
the archiver's position lives in Postgres and the objects it points at live in
S3, so resetting one leaves the other inconsistent.

## Configuration reference

Every option is a CLI flag with an environment variable equivalent. Flags win.
`.env` is loaded at startup if present.

### Listeners and process

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--addr` | `ADDR`, `LISTEN_ADDR` | `:1080` | API listen address. Serves Twirp, SSE, websockets and `/signing-keys`. |
| `--tls-addr` | `TLS_ADDR`, `TLS_LISTEN_ADDR` | `:1443` | TLS listen address. Only listened on when `--cert-file` is set. |
| `--cert-file` | `TLS_CERT_PATH` | | TLS certificate. Setting it is what enables the TLS listener. |
| `--key-file` | `TLS_KEY_PATH` | | TLS private key. |
| `--profile-addr` | `PROFILE_ADDR` | `:1081` | `/metrics`, `/health/ready`, `/debug/pprof/`, `/debug/vars`, `/debug/bom`. Must not be reachable from outside the cluster — pprof is a DoS and disclosure surface. Note that readiness lives here while liveness (`/health/alive`) is on `--addr`. |
| `--log-level` | `LOG_LEVEL` | `error` | `debug` is what surfaces the "starting X" lines for each subsystem, which is how you confirm what is actually enabled. |
| `--cors-host` | `CORS_HOSTS` | | Allowed CORS hosts, wildcards supported. Also the allowlist the websocket `CheckOrigin` check uses, so a browser client needs its host here even though the socket is not a CORS request. |

### Database

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--db` | `CONN_STRING` | `postgres://elephant-repository:pass@localhost/elephant-repository` | The direct connection. Used for `LISTEN`/`NOTIFY` always, and for everything else when no bouncer string is set. |
| `--db-bouncer` | `BOUNCER_CONN_STRING` | | Routed through PgBouncer and used for every operation *except* pub/sub. **Set this and `--db` must still be a direct connection** — transaction pooling drops notifications, and every notification-driven refresh in the process would silently fall back to five-minute polling. |
| `--db-parameter` | `CONN_STRING_PARAMETER` | | Extra connection string parameter. `pool_max_conns` belongs here: neither pool sets `MaxConns`, so pgx defaults to `max(4, NumCPU())`, and on Kubernetes `NumCPU()` reads the node's vCPU count rather than the container's quota. |
| `--migrate-db` | `MIGRATE_DB` | `false` | Migrate on startup. For disposable environments only — migrations can be expensive and some must be sequenced against the deploy. |

### S3

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--archive-bucket` | `ARCHIVE_BUCKET` | `elephant-archive` | Signed archive objects, delete manifests, batch zips, public signing keys. Required: an archiver that cannot reach it exits the process. The `s3` readiness check probes this bucket but is optional, so it reports the problem without deregistering the pod. |
| `--asset-bucket` | `ASSET_BUCKET` | `elephant-assets` | Uploaded attachment objects. Not archived except on document delete. |
| `--s3-endpoint` | `S3_ENDPOINT` | | Endpoint override. MinIO only; leave unset in production so AWS credential resolution applies. |
| `--s3-key-id` | `S3_ACCESS_KEY_ID` | | Static access key. MinIO only. |
| `--s3-key-secret` | `S3_ACCESS_KEY_SECRET` | | Static secret. MinIO only. |

### Authentication

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--oidc-config` | `OIDC_CONFIG` | | OIDC discovery URL. Without it no token validates. |
| `--jwt-audience` | `JWT_AUDIENCE` | | Required JWT audience. |
| `--jwt-scope-prefix` | `JWT_SCOPE_PREFIX` | | Prefix stripped from scopes before they are matched against the scope names in [docs/architecture.md](docs/architecture.md#scopes). Must match how the provider issues them, or every scope check fails. |
| `--client-id` | `CLIENT_ID` | | OAuth client ID. |
| `--client-secret` | `CLIENT_SECRET` | | OAuth client secret. |

### Documents and defaults

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--default-language` | `DEFAULT_LANGUAGE` | `sv-se` | Language assigned to documents that don't declare one. Validated at startup. |
| `--default-timezone` | `DEFAULT_TIMEZONE` | `Europe/Stockholm` | Timezone used by type configuration time expressions that don't specify one. Validated at startup, which is why the image ships `tzdata`. |

### Subsystem switches

Each of these turns off a background subsystem. Turning off a singleton on
*every* replica is what actually stops it — one replica with it enabled still
holds the lock and does the work.

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--no-archiver` | `NO_ARCHIVER` | `false` | Stops archiving, delete finalisation, restore, purge and signing key rotation. Deletes will hang half-finished, so this is a local-development flag — it is also the way to run without S3, since an archiver that cannot reach the archive bucket exits the process. |
| `--no-eventlog-builder` | `NO_EVENTLOG_BUILDER` | `false` | Stops turning outbox rows into eventlog entries. Everything downstream stops with it while writes keep succeeding. Aliased `--no-replicator`. |
| `--no-eventsink` | `NO_EVENTSINK` | `false` | Disables the event sink forwarder. The usual local setting. |
| `--no-scheduler` | `NO_SCHEDULER` | `false` | Disables scheduled publishing. Documents stay withheld. |
| `--no-charcounter` | `NO_CHARCOUNTER` | `false` | Disables the built-in character-count metric calculator. |
| `--no-sse` | `NO_SSE` | `false` | Disables `/sse`. |
| `--no-websocket` | `NO_WEBSOCKET` | `false` | Disables `/websocket/:token` and the document stream fan-out behind it. |
| `--eventsink` | `EVENTSINK` | `aws-eventbridge` | Sink implementation. `aws-eventbridge` is the only one; an unknown value fails startup. |

### Eventlog streaming

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--eventlog-buffer-size` | `EVENTLOG_BUFFER_SIZE` | `500` | Events kept per instance for websocket resume. A client asking to resume from further back gets `eventlog_resume_oob` and must fall back to the `Documents.Eventlog` RPC. The buffer is per-instance, so reconnects that land elsewhere can miss even within the window. |
| `--eventlog-stream-burst` | `EVENTLOG_STREAM_BURST` | `70` | Token-bucket burst per subscription. |
| `--eventlog-stream-rate` | `EVENTLOG_STREAM_RATE` | `10` | Token-bucket rate, events/second, per subscription. On exceed the events that fit are sent, then a `rate_limited` error, then the subscription is stopped and the client is expected to resubscribe. The initial resume replay is exempt. Keep burst comfortably above the largest batch a client can legitimately receive at once. |

### Compatibility and recovery

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--emit-workflow-event` | `EMIT_WORKFLOW_EVENT` | `false` | Re-emits the legacy standalone `workflow` event alongside the folded `workflow_state` fields. Transition aid; slated for removal. |
| `--emit-acl-event` | `EMIT_ACL_EVENT` | `false` | Re-emits the legacy standalone `acl` event alongside the folded `acl` field. Transition aid; slated for removal. |
| `--tolerate-eventlog-gaps` | `TOLERATE_EVENTLOG_GAPS` | `false` | Lets the archiver skip missing eventlog IDs and missing archive objects. **This permanently accepts an unverifiable hole in the signature chain.** It exists for repositories that already have pre-existing gaps; do not enable it to silence an alert. |

## What the repository does

Orientation-level descriptions. [docs/architecture.md](docs/architecture.md) is
the authority on how any of it works.

**Versioning.** Every update is a new sequentially numbered version recording
when it was created, by whom, and optional version metadata. Old versions stay
fetchable, and `Documents.GetHistory` walks the history.

**ACLs.** A document is private to its creator until shared. Entries grant
**Read**, **Write** (new versions), **MetaWrite** (metadata without a full
version) and **SetStatus** to a subject or a unit (a group of people). This is
what makes private drafts and sharing with untrusted individuals possible
rather than everything being visible to everyone with a document scope.

**Document locks.** `Documents.Lock`, or the `Lock` field on
`Documents.Get`, takes a pessimistic lock with a secret token and a client-set
TTL, extended with `ExtendLock` and released with `Unlock`. What the lock blocks
depends on the exclusivity level (`LOCK_DOCUMENT`, `LOCK_STATUS`, `LOCK_ACL`,
`LOCK_EXCLUSIVE`); by default only document updates. A failed acquisition
returns the current holder's identity, application, comment, expiry and
exclusivity as error metadata, so a client can tell "I already hold this" from
a real conflict.

**Validation schemas.** Document types must be declared before documents of
that type can be stored, which both keeps the data clean and tells automated
systems the shape of the data —
[elephant-index](https://github.com/ttab/elephant-index) uses it to build
OpenSearch mappings. Schemas are [revisor](https://github.com/ttab/revisor)
constraint sets; see
[revisor's "Writing specifications"](https://github.com/ttab/revisor#writing-specifications).
Sets of schema versions are registered as immutable *generations* that are
activated as a unit.

**Workflow statuses.** Named, per-document, sequentially numbered pointers at a
document version; setting `usable` is publication by convention. A new version
does not change the `usable` status — publishing a new version means setting
`usable` again, pointing at it. The last status of a name is its *head*, and
`heads.<status>.id` counts how many times the status has been set rather than
being a version number.

**Scheduled publishing.** A document in the `withheld` state with a planned
publish time is published automatically at that time. New scheduled documents
are discovered within a minute, and a document more than 30 minutes past its
planned time is no longer attempted.

**Attachments.** `Documents.CreateUpload` returns an upload ID and a presigned
PUT URL; referencing the ID in an `Update` attaches the object. Attach and
detach appear on the event as `attached_objects` and `detached_objects`, and in
`Documents.GetMeta`. `Documents.GetAttachments` with `DownloadLink` returns a
download URL. Attached objects are not archived — see
[the reasoning](docs/architecture.md#uploads-and-attachments).

**Event output.** Every change — a new version, a new status, an ACL update, a
delete — is emitted on the eventlog, readable through `Documents.Eventlog`, the
SSE endpoint at `/sse` (200-event replay, `Last-Event-ID` resume, topic
filtering), the websocket API at `/websocket/:token`, and optionally forwarded
to AWS EventBridge. The enriched sink events exist so that, for example, a
Lambda can subscribe to published articles in one category without every system
loading everything just to decide whether it cares.

**Document metrics.** The `Metrics` service stores per-document integer
measurements under a named *kind* with an aggregation mode. These are document
data, not operational telemetry — the Prometheus metrics are a separate thing
entirely, documented in
[docs/observability.md](docs/observability.md). A character counter is built in
and enabled by default.

**Partial fetching.** `Documents.Get` and `Documents.GetMeta` accept a `Subset`
field of subset expressions, so a client can ask for the parts of a document it
needs instead of the whole thing.

## Calling the API

The service definitions live in
[elephant-api](https://github.com/ttab/elephant-api/blob/main/repository/service.proto),
not here. Every method is `POST /twirp/elephant.repository.<Service>/<Method>`
and accepts protobuf or JSON.

### Fetching a document

```shell
curl --request POST \
  --url http://localhost:1080/twirp/elephant.repository.Documents/Get \
  --header "Authorization: Bearer $TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{
	"uuid": "8090ff79-030e-419b-952e-12917cfdaaac"
}'
```

Add `version` for a specific version, or `status` to get the version that last
received a named status — `"status": "usable"` is how you fetch what is
published rather than what is latest.

### Fetching document metadata

```shell
curl --request POST \
  --url http://localhost:1080/twirp/elephant.repository.Documents/GetMeta \
  --header "Authorization: Bearer $TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{
	"uuid": "8090ff79-030e-419b-952e-12917cfdaaac"
}'
```

### Fetching the archive signing keys

Public and unauthenticated, because it is what makes independent verification
of the archive possible:

```shell
curl http://localhost:1080/signing-keys
```

```json
{
  "keys": [
    {
      "kid": "1",
      "kty": "EC",
      "crv": "P-384",
      "x": "...",
      "y": "...",
      "iat": 1700000000,
      "nbf": 1700000000,
      "exp": 1715552000
    }
  ]
}
```

The same keys are written to `signing-keys/{kid}.json` in the archive bucket as
individual JWKs. **Store your own copy if you intend to verify the archive
independently** — an attacker who can modify the archive can modify the keys
stored beside it.

## The database

The schema is numbered [tern](https://github.com/jackc/tern) migrations in
`./schema/`; queries are defined in `postgres/query.sql` and compiled by
[sqlc](https://sqlc.dev/) into `postgres/query.sql.go`. **Every application
query goes through sqlc** — no hand-written SQL elsewhere. Set `CONN_STRING` to
run the `mage sql:*` targets against a remote database.

[docs/architecture.md](docs/architecture.md#database) describes the tables and
the write path. The short version: one `document` row per document, append-only
`document_version` and `document_status`, `status_heads` for the current head
per status name, and an `event_outbox_item` row written in the same transaction
as the change. An update starts by taking a row lock on `document(uuid)`, which
is what makes version and status numbering gapless.

### Data mining examples

#### Published article cause

`¤` is `NULL` — the initial publication of an article.

```sql
SELECT date(s.created), s.meta->>'cause' AS cause, COUNT(*) AS num
FROM document_status AS s
WHERE s.name='usable'
GROUP BY date(s.created), cause
ORDER BY date(s.created), cause NULLS FIRST;
```

```
    date    │    cause    │ num
════════════╪═════════════╪═════
 2023-02-07 │ ¤           │ 620
 2023-02-07 │ correction  │   4
 2023-02-07 │ development │  64
 2023-02-07 │ fix         │  10
 2023-02-08 │ ¤           │ 734
 2023-02-08 │ correction  │   3
 2023-02-08 │ development │  97
 2023-02-08 │ fix         │  14
 2023-02-09 │ ¤           │ 613
 2023-02-09 │ correction  │   5
 2023-02-09 │ development │  89
 2023-02-09 │ fix         │   8
 2023-02-10 │ ¤           │ 428
 2023-02-10 │ correction  │   2
 2023-02-10 │ development │  52
 2023-02-10 │ fix         │  12
(16 rows)
```

#### Time to correction after first publish

```sql
SELECT s.uuid, i.created AS initially_published, s.created-i.created AS time_to_correction
FROM document_status AS s
     INNER JOIN document_status AS i
           ON i.uuid = s.uuid AND i.name = s.name AND i.id = 1
WHERE s.name='usable' AND s.meta->>'cause' = 'correction'
ORDER BY s.created;
```

```
                 uuid                 │  initially_published   │    time_to_correction
══════════════════════════════════════╪════════════════════════╪═══════════════════════════
 54123854-9303-4cc6-b98d-afa9b2656602 │ 2023-02-07 09:19:50+00 │ @ 11 mins 55 secs
 eedf4fe2-5b3a-4fa4-a2c8-cf2029ca268b │ 2023-02-07 09:20:58+00 │ @ 1 hour 59 mins 30 secs
 03d47f19-a4b5-4de5-b6e2-664d759683ec │ 2023-02-07 12:58:07+00 │ @ 4 mins 34 secs
 37041f9b-386b-47f5-a974-f054bb628292 │ 2023-02-07 13:10:55+00 │ @ 17 mins 5 secs
 f550fbce-6c8c-43cc-a31d-0cbdb464a681 │ 2023-02-08 05:15:02+00 │ @ 1 hour 13 mins 13 secs
 f550fbce-6c8c-43cc-a31d-0cbdb464a681 │ 2023-02-08 05:15:02+00 │ @ 3 hours 15 mins 2 secs
 6ee43615-2cb8-441a-9c0f-fb68a675e1f2 │ 2023-02-08 08:30:02+00 │ @ 3 mins 56 secs
 5d75600e-4d26-488e-bcd2-1c27bd05794f │ 2023-02-09 01:30:02+00 │ @ 1 hour 2 mins 31 secs
 629ddc10-47e0-46ae-b47d-6c9fbb3ad7e0 │ 2023-02-09 08:24:37+00 │ @ 1 hour 27 mins 13 secs
 44e6653b-8be7-4175-8e4c-0c24c132e774 │ 2023-02-09 10:36:31+00 │ @ 5 hours 9 mins 25 secs
 71b61828-510d-4a6b-a8fa-574101eb54f5 │ 2023-02-09 08:30:26+00 │ @ 9 hours 54 mins 52 secs
 be6c03f8-81d1-40dd-bbe1-9b0c727b39a8 │ 2023-02-09 09:54:13+00 │ @ 8 hours 40 mins 27 secs
 d6413696-d189-4ad0-9454-8f0681a3f541 │ 2023-02-10 05:00:02+00 │ @ 1 hour 32 mins 2 secs
(13 rows)
```

#### High newsvalue articles per section

```sql
SELECT vs.section, vs.newsvalue, COUNT(*)
FROM (
     SELECT d.uuid, s.created,
            (jsonb_path_query_first(
                v.document_data,
                '$.meta[*] ? (@.type == "core/newsvalue").data'
            )->>'score')::int AS newsvalue,
            jsonb_path_query_first(
                v.document_data,
                '$.links[*] ? (@.rel == "subject" && @.type == "core/section")'
            )->>'title' AS section
     FROM document_status AS s
          INNER JOIN document AS d ON d.uuid = s.uuid
          INNER JOIN document_version AS v
                ON v.uuid = d.uuid
                   AND v.version = d.current_version
                   AND v.type = 'core/article'
     WHERE
        s.name='usable'
        AND s.id = 1
        AND date(s.created) = '2023-02-08'
) AS vs
WHERE vs.newsvalue <= 2 AND newsvalue > 0
GROUP BY vs.section, vs.newsvalue
ORDER BY vs.section, vs.newsvalue;
```

```
 section │ newsvalue │ count
═════════╪═══════════╪═══════
 Ekonomi │         1 │     2
 Ekonomi │         2 │     5
 Inrikes │         1 │     2
 Inrikes │         2 │    12
 Kultur  │         2 │     2
 Nöje    │         2 │     5
 Sport   │         1 │     4
 Sport   │         2 │     7
 Utrikes │         1 │     2
 Utrikes │         2 │     7
(10 rows)
```

## Archiving

The repository records every eventlog event, and the document or status data it
refers to, to a S3-compatible store. Each archived object embeds the signature
of its parent, so the archive is a narrow
[merkle tree](https://en.wikipedia.org/wiki/Merkle_tree) — a tamper-evident log
that could back a transparency log a trusted third party verifies against. The
reasoning is in [ADR 0001](docs/adr/0001-signed-archive-merkle-chain.md), and
the mechanics — key rotation, the chain layout, batch compaction, and the
delete, restore and purge lifecycles — are in
[docs/architecture.md](docs/architecture.md#4-archiving).

Signatures are ASN.1 signatures over the SHA-256 hash of the marshalled archive
object, formatted:

```
v1.[key ID].[sha256 hash as raw URL base64].[signature as raw URL base64]
```

and set as the `X-Amz-Meta-Elephant-Signature` metadata header on the object.

**Signing happens at archive time rather than write time because `jsonb` is not
guaranteed to be byte-stable**, so a signature taken over a database row could
not be re-derived later. The canonical signed artifact is the S3 object;
verifying live database contents means verifying the archive object's signature
and then checking that the row is *logically* equivalent to the archived data,
not byte-equal.

## Pending work

**`ensureSigningKeys` needs to be able to fail without taking the service down.**
It runs in `Archiver.Run` *before* the retry group is created, so a failure gets
no retries at all: `Run` returns, the archiver's errgroup member fails, and the
process exits within seconds. It is also called every 24 hours from the
delete/restore/purge poll loop, which is unlocked and therefore runs on every
replica, so a persistent failure there exits the whole fleet after ~5 minutes of
retries rather than one replica at a time. Nothing about key provisioning
justifies being the most fragile thing in the process.

The change is not just "retry it", because two operations with very different
criticality are bundled into one function:

* *Provisioning* — read the key set from Postgres, mint a new key if there is
  none or if the newest expires within 7 days, under the `LockSigningKeys`
  advisory lock. Archiving genuinely cannot proceed without a usable key.
* *Publication* — write the new key's **public** half to
  `signing-keys/<kid>.json` in the archive bucket. This is a convenience for
  independent verifiers; the private key lives in Postgres, so failing to
  publish does not stop anything from being signed. **This is also the half most
  likely to fail, because it is the only half that touches S3.** Splitting the
  two, and letting publication retry in the background indefinitely, removes
  most of the fragility on its own.

The correctness property that has to survive the refactor is that **no archive
object may be written unsigned, or signed with a key that is not valid at the
object's archived time.** That guard already exists — `storeArchiveObject` bails
when `SigningKeySet.CurrentKey(archivedTime)` returns nil — so the safety
property is not what is missing. What is missing is that a nil key currently
surfaces as a per-object error, and the archive workers' retry group escalates a
sustained per-object error into a process exit. A non-fatal design needs the
workers to *wait* for a usable key instead of erroring on its absence;
`TypeConfigurations` already does exactly this with an `initWait` channel that
gates its accessors, and that pattern fits here.

Note the real deadline, because a non-fatal design has to respect it rather than
retry forever in silence: rotation is triggered 7 days before the current key
expires and the replacement gets `NotBefore = now + 2 days`, so there is about
five days of genuine slack. If provisioning fails for longer than that the
current key expires, `CurrentKey` returns nil, and archiving halts completely.
There is no metric for signing keys at all today, so that window would pass
unobserved — a gauge for "seconds until the current key expires" and a counter
for provisioning failures should land with the refactor. While in there, the nil
key branch does `fmt.Errorf("no signing keys have been configured: %w", err)`
with an `err` that is always nil at that point, which renders as
`%!w(<nil>)`; it is worth fixing in the same change, since it is precisely the
error path that becomes load-bearing.

**The authentication middleware is hand-rolled and should move to elephantine's
`ServiceOptions`.** `ServerOptions.SetJWTValidation` in `repository/serve.go`
predates `elephantine.NewDefaultServiceOptions`, which offers the same
default-deny behaviour through `ServiceAuthRequired` and validates in a Twirp
`RequestRouted` hook rather than in HTTP middleware — a better place for it,
since the hook has the routed method in hand. Two things make it more than a
swap: `/sse` shares the middleware and is not a Twirp route, so it needs its own
handling; and elephantine answers an invalid token with `permission_denied`
where the current middleware returns a 401, so error codes shift for malformed
tokens. There is also no JWT caching — every request re-validates — which the
`TODO` at the call site has noted for some time.

**No alerting or dashboards live in this repository.** Every metric in
[docs/observability.md](docs/observability.md) exists and nothing fires on any
of them. [The ranked watch list](docs/ops.md#what-to-watch-in-order) is the
intended starting point for the five alerts that should exist. One of them is
newly load-bearing: the `s3` readiness check is optional, so
`health_check_up{name="s3"}` is the only automatic signal that the archive
bucket has become unreachable.

**SSE is uninstrumented.** No metric counts SSE connections, publishes, replay
misses, or the events it skips when marshalling fails, so an SSE-only outage is
invisible in metrics. This is the biggest observability gap in the service, and
it matters because a client of a stalled SSE stream sees exactly what a client
of an idle repository sees.

**Workflow and type configuration reload failures are not counted.**
`Workflows.reloadLoop` logs a hint naming
`elephant_workflow_refresh_failure_count`, but no collector is registered and
nothing turns the log field into a metric. Schemas and deprecations got failure
counters in v1.9.0; these two did not, so a workflow reload failing repeatedly
is visible in logs only while the instance keeps evaluating stale rules.

**Attached objects are not archived, and it is still an open question whether
they should be.** If this is used for images and video, automatically
duplicating everything may not be what we want. They are copied to the archive
bucket when their document is deleted — *moved*, not copied, so they leave the
asset bucket — so a document can be restored with its attachments, but only the
latest version of the currently attached objects. Backup of the asset bucket has
to be solved outside the repository.

**Neither connection pool sets `MaxConns`**, so the effective pool size is
`max(4, runtime.NumCPU())` and `NumCPU()` reads the cpuset rather than the
cgroup CPU quota. On Kubernetes with the default CPU manager policy that tracks
the node's vCPU count, so pool size changes invisibly when a pod is
rescheduled. It should be set explicitly, sized for the workload.

**The legacy event flags are still shipping.** `--emit-workflow-event` and
`--emit-acl-event` re-add event shapes removed in v1.8.0 and are slated for
removal; the consumers still relying on them need to be identified first.

**`docs/permissions.md` is maintained by hand.** It is a per-method matrix
derived from scope checks scattered across four API files, with nothing
verifying that it still matches the code. It was already out of date once. A
generator, or a test that walks the services, would fix it properly.
