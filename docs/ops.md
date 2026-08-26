# Elephant repository — operations

For whoever is holding the pager, or trying to work out why writes, events or
publishing have stopped. It assumes you can read the metrics and reach the
database, and it does not assume you have read the code.

| Document | What it settles |
|---|---|
| [README](../README.md) | Orientation and the working reference: what the repository holds, how to build and run it, and what every configuration flag does. |
| [architecture.md](architecture.md) | How the service is built: the process model, the data flow through every worker, each subsystem, and the API surface. |
| **ops.md** (this document) | The operator's-eye view: dependencies, deployment shape, data flows, bootstrap order, and the failure modes with the signal that shows each one. |
| [observability.md](observability.md) | Every metric the service exports and what a change in it means. |

This document names the signal for each failure mode;
[observability.md](observability.md) defines what each metric actually
measures. It does not explain *why* the design is what it is — that is
[architecture.md](architecture.md).

> **On deployment specifics:** the Kubernetes manifests, Terraform, alert rules
> and dashboards for this service live outside this repository, and this
> document does not reproduce them. What is here is what the binary itself
> requires and how it behaves — replica counts, resource limits and the actual
> bucket and database names for a given environment have to come from the
> deployment repository.

## What the service is

One binary, `repository run`, doing two jobs that fail independently:

* **The synchronous half** — the Twirp API, SSE, websockets. This is what
  clients see. It needs Postgres for everything and S3 for uploads and
  attachment downloads.
* **The asynchronous half** — eventlog builder, archiver, event forwarder,
  publish scheduler, lock cleaner. This is what makes writes durable,
  observable and eventually published.

The halves are coupled in one direction: **an archiver that fails for more than
about five minutes takes its own process down, including that instance's API.**
That is deliberate — an instance that cannot make writes durable should not keep
accepting them.

It is not usually a fleet-wide outage, though, because a replica that does not
hold the archiver's job locks parks its archive workers instead of running them,
and a parked worker cannot fail. An S3 outage with no delete, restore or purge
work pending therefore kills the leader, hands the lock to another replica, and
kills that one about five minutes later — a rolling restart rather than a
simultaneous one, and with replicas to spare the API keeps serving throughout.
The `s3` readiness check is deliberately optional so that it does not undo
that by deregistering every pod at once. See
[Archiving has stalled](#archiving-has-stalled).

## Components

| Repository | What it is to us |
|---|---|
| `ttab/elephant-repository` | This service. |
| `ttab/elephant-api` | The protobuf service definitions. The API contract is versioned there, not here. |
| `ttab/newsdoc` | The document format. |
| `ttab/revisor` | The validation engine schemas are written against. |
| `ttab/elephantine` | Shared service plumbing: auth, logging, metrics helpers, job locks, graceful shutdown, the API server. |
| `ttab/elephant-index` | Downstream consumer. Reads schemas to build OpenSearch mappings and follows the eventlog. |

## Deployment shape

| Role | Configuration | Runs |
|---|---|---|
| API replica | The default: no `--no-*` flags | Everything. API, SSE, websockets, and every background worker, with the singletons gated on job locks. |

There is one role. **Adding replicas adds API, SSE and websocket capacity and
adds delete/restore/purge throughput; it does not add eventlog-building,
archiving, forwarding or scheduling capacity, because those are single-leader.**
If the eventlog builder is the bottleneck, more replicas will not help.

The `--no-*` flags make it possible to split the halves — a deployment of
API-only replicas plus one worker replica — but nothing in this repository
assumes that split, and the failure modes below assume the default shape.

The container exposes 1080 (API) and 1081 (metrics and pprof), runs as
`ENTRYPOINT ["repository", "run"]`, and ships `tzdata` because
`--default-timezone` needs the zoneinfo database.

## Runtime dependencies

| Dependency | Needed for | What happens without it |
|---|---|---|
| PostgreSQL (direct connection) | Everything. All document state, the eventlog, job locks, `LISTEN`/`NOTIFY` | Total outage. Nothing works, and the readiness probe fails. |
| PostgreSQL `LISTEN`/`NOTIFY` on the direct pool | Prompt refresh of schemas, workflows and type configuration; prompt event fan-out | Degraded, not down. Every notification-driven refresh falls back to its five-minute poll, and the eventlog builder to its one-minute poll. **This is why the pubsub pool must not be routed through PgBouncer** — transaction pooling silently drops the notifications. |
| PgBouncer (optional, `--db-bouncer`) | Connection multiplexing for everything except pub/sub | Not needed. Without it both pools are the direct connection. |
| S3 archive bucket | Archiving, delete finalisation, restore, purge, signing key storage | The archiver exhausts its retries and exits, killing replicas one at a time as each becomes the lock holder; deletes stop finalising. The API keeps serving — the `s3` readiness check is optional, so pods stay in rotation and only `health_check_up{name="s3"}` reports it. A pod *starting* while the bucket is unreachable exits within seconds instead. |
| S3 asset bucket | Creating uploads, attaching objects, downloading attachments | Uploads and attachment downloads fail. Document writes without attachments are unaffected. |
| OIDC provider (`--oidc-config`) | Validating JWTs | Every authenticated call fails. The five unauthenticated methods keep working. Note that key sets are typically cached, so a brief provider outage may not be visible. |
| AWS EventBridge (optional) | The event sink | Forwarding stops and retries; the position is persisted so it catches up. Nothing else is affected. Disable with `--no-eventsink`. |

Truly required: **Postgres and the archive bucket.** Everything else either
degrades to polling or takes a feature offline rather than the service.

## Endpoints and ports

| Port | Default | What is on it |
|---|---|---|
| API | `:1080` (`--addr`) | `/twirp/elephant.repository.*`, `/sse`, `/websocket/:token`, `/signing-keys`, `/version`, `/health/alive` |
| TLS API | `:1443` (`--tls-addr`) | The same, when `--cert-file` is set. Not started otherwise. |
| Profile | `:1081` (`--profile-addr`) | `/metrics`, `/health/ready`, `/debug/pprof/`, `/debug/vars`, `/debug/bom` |

Liveness is `/health/alive` on the API port; **readiness is `/health/ready` on
the profile port**, and it reports per-check results — `s3` (write, read and
delete a probe object in the archive bucket) and `postgres` (read the active
schemas). A probe pointed at the wrong port checks the wrong thing: the
liveness endpoint says nothing about whether Postgres or S3 is reachable.

**`postgres` is a hard check; `s3` is optional.** A failing `s3` check appears in
the response body as `"ok": false, "optional": true` and drives
`health_check_up{name="s3"}` to 0, but returns 200 and leaves the pod in
rotation. That is deliberate: the synchronous API only needs Postgres for reads
and document writes, so deregistering every replica over an archive bucket
outage would turn a degraded dependency into a total outage. The consequence is
that **an S3 outage has no automatic mitigation and no probe will catch it** —
it needs an alert on `health_check_up{name="s3"}`, which does not exist yet.

`GET /signing-keys` is public and unauthenticated by design: it is the JWKS
document needed to verify archive signatures.

## Data flows

### 1. Write

```
 client ──▶ Documents.Update ──▶ scope check ──▶ ACL check
                                      │
                                      ▼
                          BEGIN  [row lock on document(uuid)]
                            ├─ validate (in-memory validator)
                            ├─ document_version / document_status
                            ├─ document / status_heads
                            ├─ derived time + labels (type configuration)
                            └─ event_outbox_item  +  NOTIFY event_outbox
                          COMMIT
```

The row lock is what serialises writers to a single document; writers to
different documents never contend. **The version row and its event commit
together or not at all**, so there is no window in which a write exists without
an event, or an event without a write.

Operationally, the write path depends on the *in-memory* validator and type
configuration, not on the database's current values. An instance that has
failed to reload will happily accept writes validated against stale schemas.

### 2. Event fan-out

```
 event_outbox_item
       │  eventlog builder   [job lock: eventlog-builder]
       │  wakes on NOTIFY event_outbox, else polls every 1 min
       ▼
    eventlog  (dense, sequential IDs assigned by the builder)
       │
       ├──▶ SSE          per-instance, 200-event replay, follows NOTIFY
       │                 eventlog with a 5 s ticker floor
       ├──▶ DocumentStream per-instance, --eventlog-buffer-size replay,
       │                 enriches with document + meta, feeds websockets
       └──▶ EventForwarder [job lock: forwarder] position in `eventsink`,
                          → AWS EventBridge
```

The three consumers are independent and keep their own positions: a wedged
event sink does not slow websockets, and a websocket instance falling behind
does not affect the archive. **Nothing downstream can advance past the eventlog
builder**, though, which is why the builder is the first thing to check when
"nothing is happening".

### 3. Archive

```
    eventlog
       │  event archiver  [job lock: eventlog-archiver]
       │  100 rows/poll, requires event id == position + 1
       ▼
    S3 archive bucket
       ├─ events/<id>.json                      signed, chained to parent
       ├─ documents/<uuid>/versions|statuses/…  signed
       ├─ events_1k/…zip     [job lock: eventlog-batch-archiver]
       ├─ events_10k/…zip    [same]
       ├─ generations/…      [job lock: generation-archiver]
       └─ signing-keys/<kid>.json
```

The S3 object is written before the transaction recording the new position
commits, with a compensating delete if the transaction fails. **S3 is the
authority on what has been archived; the database position follows it.**

### 4. Delete, restore, purge

```
 Delete RPC ─▶ row lock ─▶ wait for full archive ─▶ delete_record
            ─▶ document row becomes system_state='deleting'
            ─▶ client sees success   ← document now unreadable and unwritable
                        │
                        ▼  archiver poll loop  [NO job lock — every replica,
                        │  FOR UPDATE SKIP LOCKED]
            move documents/<uuid>/* → deleted/<uuid>/<record id>/  (8 workers)
            move attached assets out of the asset bucket → …/attached/
            write manifest.json
            delete document row, finalise delete_record
```

**Between the client seeing success and the archiver finishing, the document is
in a state where nothing can read or write it.** If the archiver is not
running, deletes accumulate in that state indefinitely and look to users like
documents that have vanished but cannot be recreated.

Restore replays from `manifest.json`, emitting events with
`system_state = restoring` that consumers are expected to ignore, followed by a
`restore_finished` event. Purge deletes the archived objects and strips the
delete record down to the audit trail.

### 5. Scheduled publishing

```
 withheld status + planned publish time
       │  scheduler  [job lock: scheduler]
       │  sleeps until the next known publish time, max 1 min
       ▼
 Documents.Update as internal://scheduler (doc_admin)
   guarded by IfWorkflowState='withheld' and the withheld status head ID
       │
       └─ 30 minutes past the planned time: no further attempts
```

## Single-leader work

Eight job locks. Exactly one holder each, cluster-wide. `pg_job_lock_held` says
whether this instance is the holder; frequent `pg_job_lock_transitions_total`
means leases are being lost, usually to database latency.

| Lock | Does | When nobody holds it |
|---|---|---|
| `eventlog-builder` | Turns outbox rows into eventlog entries | Everything downstream stops: no SSE, no websocket events, no sink delivery, no archiving. Writes still succeed and the outbox grows. |
| `eventlog-archiver` | Writes signed event and document objects to S3 | No new archive objects. Deletes block, since they wait for full archival. |
| `eventlog-batch-archiver` | Compacts 1000 and 10000-event zips | Batches stop being produced. Nothing on the write or read path notices. |
| `generation-archiver` | Archives schema generation lifecycle | Generation history stops being archived. No runtime effect. |
| `forwarder` | Delivers enriched events to EventBridge | Sink consumers stop receiving. Position is persisted, so it catches up. |
| `scheduler` | Publishes withheld documents at their planned time | Scheduled publishing stops. Past 30 minutes, missed documents are not retried even once a leader returns. |
| `cleaner` | Deletes expired document lock rows | `document_lock` grows. No functional effect — expired locks are already ignored by readers and writers. |
| `bootstrap-generation` | One-shot schema generation bootstrap at startup | Startup blocks on acquiring it, then proceeds. |

**The `scheduler` lock is the only one where an outage causes permanent loss
rather than lag.** Everything else catches up from its persisted position.

## Where state lives

| Store | Holds | Authoritative for |
|---|---|---|
| PostgreSQL | Documents, versions, statuses, ACLs, locks, the eventlog, schemas and generations, workflows, type configuration, document metrics, job locks, archiver positions, signing keys | Everything live. This is the system of record. |
| S3 archive bucket | Signed event, version and status objects; delete manifests; batch zips; generation archives; public signing keys | The signed history. It is the authority on *what was archived*, and the only place a delete manifest exists once a document row is gone. |
| S3 asset bucket | Uploaded attachment objects | Attachment bytes, until the document is deleted — the delete *moves* them into the archive bucket rather than copying, so they leave this bucket. **Not archived** otherwise; there is no backup of attachments inside this service. |
| Instance memory | Compiled validator, workflows and status rules, type configuration extractors, SSE and docstream replay buffers | Nothing. All of it is derived and rebuilt on restart — but it can be *stale*, which is the point of `elephant_validator_schema_generation`. |

## Bootstrap order

1. **Migrate the database.** `mage sql:migrate`, or `--migrate-db` for
   disposable environments only. Migrations can be expensive and some must be
   sequenced against the deploy (see the `**Migrations:**` blocks in
   [CHANGELOG.md](../CHANGELOG.md) — 021 needs a maintenance window, 024 must
   run *after* v1.4.0 is deployed, 027 must run *before* v1.9.0). Running an
   old binary against a new schema, or the reverse, is where the sharp edges
   are.
2. **Create the buckets.** The archive bucket must exist before the process
   starts — the archiver's signing-key check runs before any retry machinery, so
   an unreachable bucket exits the process in seconds. Readiness will *not* warn
   you about this: the `s3` check is optional, and the pod dies before it
   matters.
3. **Start the process.** In order, it: connects both pools, optionally
   migrates, starts the notification listener and lock cleaner, acquires
   `bootstrap-generation` and bootstraps the schema generation *to completion*,
   builds the validator from the active generation, loads workflows, ensures the
   socket signing key, then starts the background workers and the API server.
4. **Register schemas.** A repository with no active schema generation accepts
   no document types — every write fails validation. Schema registration is an
   administrative task using the `Schemas` service; **the server no longer
   installs the embedded core schemas at startup** (removed in v1.7.0 along
   with `--no-core-schema` and `--ensure-schema`), so a fresh environment is
   inert until something registers them.
5. **Configure types, statuses and workflows** as needed. A type with no
   configured workflow gets an implicit one synthesised from its statuses, so
   this step is optional for simple types.

Out of order: starting before migrating gives query errors against missing
columns; starting without the archive bucket exits the process on the archiver's
signing-key check without ever failing readiness, so the symptom is a crash loop
with a healthy-looking probe; registering schemas before the generation
bootstrap has run is not possible, since the bootstrap holds a lock the API
does not wait on but the validator does.

## Failure modes

### Writes succeed but nothing downstream sees them

The eventlog builder has stopped. Outbox rows accumulate and every consumer —
SSE, websockets, the event sink, the archive — sees nothing new, while the API
keeps reporting success.

**Signal:** `elephant_eventlog_events_total` flat while
`rpc_requests_total{method="Update"}` climbs. Confirm with
`select count(*) from event_outbox_item` — a healthy value is single or double
digits, since rows are deleted as they are processed.

**Action:** find the `eventlog-builder` lock holder
(`pg_job_lock_held{...}`, or `select * from job_lock where name =
'eventlog-builder'`). If nobody holds it, check whether every replica has the
builder disabled. If someone holds it and `elephant_eventlog_start_total` is
climbing, the builder is crash-looping — the error is in that instance's logs.
Restarting the lock holder releases the lease and lets another instance take
over.

### Archiving has stalled

**Signal:** `elephant_archiver_event_archiver_position` not advancing while
`elephant_eventlog_events_total` does, or
`elephant_event_archived_total{status="error"}` climbing, or
`task_restarts_total` climbing for an archiver task.

**Action:** the error text is in the archiver logs. The two common causes are
S3 (check `client_requests_total{client="s3"}` and the readiness probe) and a
gap in the eventlog. The gap case is distinctive: the archiver refuses to
proceed with `inconsistent eventlog, expected event N, got M`, because
skipping would break the signature chain and make everything after it
unverifiable. **Do not reach for `--tolerate-eventlog-gaps` to make an alert go
away** — it permanently accepts an unverifiable hole in the archive, and exists
for repositories that already have pre-existing holes. Work out why an ID is
missing first.

Remember the escalation, and its shape. After about five minutes of retries the
archiver's task group fails and the process exits, taking that instance's API
with it. **The shape depends on which archive worker is failing:**

* **The eventlog, batch and generation archivers only run on the lock holder.**
  Other replicas park waiting for the lock and cannot fail. So a failure here
  kills the leader, another replica steals the stale lock ~40 s later, and dies
  ~5 minutes after that. You see a rolling restart, one pod at a time, and the
  API stays up if there are replicas to spare. Do not read a single
  CrashLoopBackOff here as "the service is down".
* **The delete/restore/purge poll loop runs on every replica**, so if that is
  what is failing — which requires delete, restore or purge work to be pending —
  every replica fails at once and you get a fleet-wide crash loop.
* **A signing-key failure at startup gets no retries at all** and exits in
  seconds, because `ensureSigningKeys` runs before the retry group exists. It
  only touches S3 when there are unarchived keys, so this is the fresh-install
  and just-after-rotation case.

Throughout all of these the pods stay in rotation, because the `s3` readiness
check is optional — so the API keeps serving from whichever replicas are alive,
and `health_check_up{name="s3"}` is what tells you the bucket is the problem.
Fix S3 and it recovers on its own; the archiver resumes from its persisted
position.

### Deleted documents are stuck and cannot be recreated

A delete has been accepted but not finalised, so the document row is a
`system_state = 'deleting'` placeholder that refuses reads and writes.

**Signal:** `elephant_archiver_deletes_total` not advancing while deletes are
being requested, or `select count(*) from delete_record where finalised is
null` growing. `elephant_archiver_delete_moves_total{status="error"}` points at
S3.

**Action:** this is downstream of archiving — fix the archiver and the backlog
drains, since the delete poll loop runs on every replica and retries
indefinitely. Note that this poll loop is *not* under a job lock, so "the
leader is down" is not an explanation for it having stopped.

### An instance is validating against stale schemas

A schema generation was activated, the API reports it as active, and one or
more instances are still enforcing the previous one — accepting documents that
should now be rejected, or rejecting ones that should now pass.

**Signal:** `min(elephant_validator_schema_generation)` below the generation
`Schemas.GetAllActive` reports. If `elephant_schema_refresh_failures_total` is
also climbing, the instance is stuck rather than merely lagging.

**Action:** **do not wait this out — a reload is supposed to be immediate.**
Activation publishes its notification inside the activating transaction, so on
commit every listening instance wakes and reloads at once; the expected lag is
the time it takes to compile the schema set, not minutes. The five-minute timer
in the reload loop is a safety net for a notification that was *lost*, not the
normal path, so **a lag of more than a few seconds is itself the symptom** and
points at one of three things:

1. **Notification delivery is broken.** The usual cause is the pubsub pool being
   routed through PgBouncer, where transaction pooling silently swallows
   `LISTEN`/`NOTIFY` — `--db` must be a direct connection. Check that first,
   because it degrades every notification-driven refresh in the process, not just
   schemas. A dropped and reconnected listener loses anything published during
   the gap, which the timer then covers.
2. **The reload is failing.** Check
   `elephant_schema_refresh_failures_total`. **Nothing will retry a persistently
   failing reload into working** — the instance keeps the schemas it last loaded,
   indefinitely, while reporting healthy. Restart it.
3. **The instance is not listening at all**, e.g. its subscriber goroutine died.
   The logs are the only signal; there is no metric for listener health.

If the lag clears within five minutes and `elephant_schema_refresh_failures_total`
is flat, the reload worked but the notification did not reach that instance —
which is worth chasing rather than filing as normal, since the next thing to
depend on a notification may not have a timer behind it.

Note that a notification arriving while a reload is in flight can be dropped —
the subscriber does a non-blocking send onto a buffer of one — and that this is
harmless: a reload reads current state rather than applying a delta, so a
coalesced notification loses nothing. Duplicate-notification loss is not a lag
source; connection-level loss is.

The same reasoning applies to `elephant_deprecation_refresh_failures_total` for
deprecation enforcement, and to type configuration — whose reload failures are
logged but not counted at all.

### Scheduled documents are not going out

**Signal:** `elephant_scheduled_delayed` above zero and staying there.
`elephant_scheduled_publish_total{outcome="failure"}` distinguishes "the
scheduler is running and the updates are failing" from "the scheduler is not
running at all", which is what you get when the gauge is stale because nobody
holds the `scheduler` lock.

**Action:** **this is the failure mode with a deadline.** Thirty minutes past a
document's planned publish time the scheduler stops attempting it, and it will
not be picked up again when the scheduler recovers — it just stays withheld.
Anything already past that window has to be published by hand. Check the
`scheduler` lock holder and its logs; a failing update is usually a status rule
or a validation failure, which the log will name.

### Websocket clients are silently receiving nothing

A stalled document stream on one instance looks exactly like an idle repository
from the client's side: the connection stays open and no events arrive.

**Signal:** `repository_docstream_position` flat on one instance while other
instances advance, or `repository_docstream_emit_total{status="error"}`
climbing. Both are per-instance — a cluster-wide average hides this.

**Action:** restart the affected instance; clients reconnect and resume from
their last event ID. If `repository_websocket_rate_limited_total{reason=
"eventlog_stream"}` is what is growing instead, clients are being cut off for
exceeding their token bucket and are expected to resubscribe — raise
`--eventlog-stream-rate`/`--eventlog-stream-burst` if the workload legitimately
needs more, or fix the client that is not resubscribing.

### Clients cannot resume and get `eventlog_resume_oob`

A client asked to resume from an event older than the instance's replay buffer.
The buffer is per-instance and holds `--eventlog-buffer-size` events (500 by
default), so a client that reconnects to a different instance, or after a long
gap, can fall out of it.

**Signal:** `repository_websocket_response_total{status="eventlog_resume_oob"}`.

**Action:** clients are expected to handle this by falling back to the
`Documents.Eventlog` RPC and catching up from there. Sustained volume means
either the buffer is too small for the reconnect patterns in play or a client is
not implementing the fallback.

### Sink consumers are missing events

**Signal:** `elephant_event_forwarder_skipped_total` growing. This is the only
counter in the service that means permanent loss rather than lag.

**Action:** check the `reason` label. `deleted` is routine — a document deleted
before the forwarder reached its event cannot be enriched, so the event is
dropped, and the delete event itself still goes out. Any other reason needs the
logs. Rising `elephant_event_forwarder_latency_seconds` without skips is lag,
not loss, and resolves itself.

### Everything is slow and the pool is exhausted

**Signal:** `pgxpool_empty_acquires_total` and
`pgxpool_empty_acquire_wait_seconds_total` growing together, with
`pgxpool_acquired_conns` pinned at `pgxpool_max_conns`.

**Action:** neither pool sets an explicit `MaxConns`, so pgx defaults to
`max(4, runtime.NumCPU())` — and on Kubernetes with the default CPU manager
policy `NumCPU()` reads the node's vCPU count, not the container's quota.
**Pool size therefore changes when a pod is rescheduled onto a
differently-sized node, with no configuration change.** Set `pool_max_conns` in
the connection string rather than trying to reason about it.

### A single document's writers are queueing

Every write to a document takes a row lock on `document(uuid)` first, so
concurrent writers to the *same* document serialise. This is by design and is
what makes version numbering gapless.

**Signal:** slow `Documents.Update` in `rpc_duration_seconds` concentrated on
one document, visible in `pg_stat_activity` as sessions waiting on a tuple
lock.

**Action:** usually a client retry loop hammering one document, or a very large
document. There is no lock-timeout knob here; the fix is on the client side.

## What to watch, in order

1. **`elephant_eventlog_events_total` against write volume.** Nothing
   downstream can advance past the builder, so this failing makes every other
   consumer look broken while the API reports success.
2. **`elephant_archiver_event_archiver_position` against the eventlog head.**
   Archiving lag becomes stuck deletes, and then becomes a process that exits.
3. **`elephant_scheduled_delayed`.** The only failure mode with a 30-minute
   deadline after which documents are permanently missed, and the one users
   notice first.
4. **`min(elephant_validator_schema_generation)` against the active
   generation**, with `elephant_schema_refresh_failures_total`. A stuck
   instance enforces stale rules while reporting healthy, and nothing retries
   it into working.
5. **`elephant_event_forwarder_skipped_total`.** The service's only counter
   that means permanent loss for a downstream consumer.

## Common operations

**Check which instance is doing the single-leader work.**

```sql
SELECT name, holder, touched, iteration FROM job_lock ORDER BY name;
```

`touched` is the last heartbeat; a `touched` that has stopped moving while
`iteration` stands still is a holder that has died without releasing.

**Check the outbox backlog.**

```sql
SELECT count(*) FROM event_outbox_item;
```

**Check archive lag in events.**

```sql
SELECT (SELECT max(id) FROM eventlog) - (SELECT position FROM eventlog_archiver WHERE size = 1)
       AS archive_backlog;
```

**Find deletes that have not been finalised.**

```sql
SELECT uuid, id, created FROM delete_record WHERE finalised IS NULL ORDER BY created;
```

**Find documents stuck in a system state.**

```sql
SELECT uuid, type, system_state, updated FROM document WHERE system_state IS NOT NULL;
```

**See which schema generation is active in the database**, then compare against
`elephant_validator_schema_generation` per instance:

```sql
SELECT id, status, created FROM schema_generation ORDER BY id DESC LIMIT 5;
```

**Force an instance to reload schemas** — there is no admin RPC for this;
activate or re-activate a generation to publish the notification, or restart
the instance.

**Verify an archived event's signature chain.** Fetch the public keys from
`GET /signing-keys`, then the object from
`events/<20-digit id>.json` in the archive bucket; the signature is the
`X-Amz-Meta-Elephant-Signature` header, formatted
`v1.<key id>.<sha256, raw URL base64>.<signature, raw URL base64>`, and each
object carries its parent's signature in `parent_signature`.

**Take profiles.** `go tool pprof http://<instance>:1081/debug/pprof/profile`
for CPU, `.../heap` for memory, `.../goroutine?debug=2` for a stuck worker.

## Security

**Inbound.** JWT bearer tokens validated against the OIDC provider named by
`--oidc-config`, with `--jwt-audience` and `--jwt-scope-prefix` applied. Scope
check, then a per-document ACL check for document operations. The scope
vocabulary and the bypass scopes are in
[architecture.md](architecture.md#scopes); the per-method matrix is in
[permissions.md](permissions.md).

**`doc_admin` bypasses every ACL check**, and `doc_read_all` bypasses every
read ACL. Those two scopes are the whole access-control model for anything
holding them; treat them as administrative.

**A valid token is required for every Twirp call and for `/sse`** — the auth
middleware answers 401 rather than passing an unauthenticated request to the
handler. Every method except the unimplemented `Documents.Evict` then asserts its
own scope requirement on top.

Two routes intentionally bypass that middleware: `GET /signing-keys`, which is
public so that the archive can be verified independently, and
`GET /websocket/:token`, which authenticates with a socket token from
`Documents.GetSocketToken` and then a JWT on the session.

**Not a write path.** `/metrics`, `/health/ready`, `/debug/pprof/`,
`/debug/vars` and `/debug/bom` are on a separate port and must not be exposed
outside the cluster — pprof in particular is a denial-of-service and
information-disclosure surface, and `/debug/bom` discloses the full dependency
manifest.

**Outbound credentials.** S3 uses standard AWS credential resolution in
production; `--s3-endpoint`, `--s3-key-id` and `--s3-key-secret` exist for
local MinIO only. EventBridge uses the default AWS SDK config. Locally,
credentials come from `ttrun`, which resolves the references in `ttrun.env` —
that file contains references, never values.

**Key systems.** Two independent key sets, both generated and stored by the
service in Postgres:

* *Archive signing keys* — ECDSA P-384, 180-day validity, rotated
  automatically with a 7-day advance generation and a 2-day heads-up. Public
  halves are served at `/signing-keys` and copied into the archive bucket.
  **Anyone verifying the archive independently must keep their own copy of the
  public keys**; keys stored beside the data they sign prove nothing against an
  attacker who can rewrite both.
* *Socket token key* — an ECDSA key created on first start
  (`EnsureSocketKey`), used to sign the short-lived tokens in
  `/websocket/:token` URLs.

## Not in place yet

* **A signing-key failure is fatal and un-retried, and needs to stop being
  both.** `ensureSigningKeys` runs before the archiver's retry group exists, so a
  failure exits the process in seconds with no retry; the 24-hour re-check runs
  from the unlocked poll loop, so a persistent failure there exits every replica
  rather than one at a time. The fix is to split key *provisioning* (Postgres,
  genuinely required) from key *publication* (S3, a convenience for verifiers —
  the private key is in Postgres, so nothing is blocked by a failed upload), and
  to make the archive workers wait for a usable key instead of erroring on its
  absence. Operationally the thing to know until then: **an archive bucket
  outage that spans a pod restart, or a key rotation, is fatal rather than
  degrading.** See [Pending work](../README.md#pending-work) for the detail and
  the five-day rotation budget involved.
* **No signing-key metrics at all.** Nothing reports when the current key
  expires, whether rotation succeeded, or whether public keys have been
  published to the archive bucket. Rotation is automatic and has roughly five
  days of slack, so a rotation that has been failing is invisible until
  archiving stops outright.
* **No alerting or dashboards in this repository.** Every metric in
  [observability.md](observability.md) exists, and nothing here defines a rule
  that fires on it. The [watch list](#what-to-watch-in-order) is the intended
  starting point for five alerts that do not exist yet.
* **`health_check_up{name="s3"}` has no alert, and now needs one.** Making the
  S3 readiness check optional was the right trade — it stops an archive bucket
  outage from deregistering the whole fleet — but it also removed the only
  automatic reaction to that outage. Until an alert exists, an unreachable
  archive bucket surfaces as replicas restarting one at a time with readiness
  reporting 200.
* **SSE is uninstrumented.** No metric counts SSE connections, publishes,
  replay misses or marshalling skips, so an SSE-only outage is invisible in
  metrics and has to be found in logs. This is the largest observability gap in
  the service.
* **Workflow reload failures are not counted.** `Workflows.reloadLoop` logs a
  hint naming `elephant_workflow_refresh_failure_count`, but no such collector
  is registered — a workflow reload failing repeatedly is visible in logs only.
  Type configuration reload failures are in the same position.
* **No liveness signal for the delete/restore/purge poll loop.** An idle loop
  and a wedged one are indistinguishable from the metrics; only the absence of
  progress on `elephant_archiver_deletes_total` gives it away, and only when
  there is work to do.
* **Attachments have no backup.** Attached objects are not archived; they are
  copied into the archive bucket only when their document is deleted, and only
  the latest version of the currently attached objects. Backing up the asset
  bucket is out of scope for this service and has to be solved around it.
* **No `MaxConns` on either pool.** See [the pool exhaustion failure
  mode](#everything-is-slow-and-the-pool-is-exhausted): the effective pool size
  is a function of the node the pod landed on.
* **Legacy event flags are still shipping.** `--emit-workflow-event` and
  `--emit-acl-event` re-emit event shapes that were removed in v1.8.0 and are
  slated for removal. Consumers still relying on them need to be found before
  that happens.
