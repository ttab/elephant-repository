# Architecture

How the repository is built: the process model, every goroutine the server
starts, the path a write takes from RPC to archive, and the API surface. Start
here before changing anything.

| Document | What it settles |
|---|---|
| [README](../README.md) | Orientation and the working reference: what the repository holds, how to build and run it, and what every configuration flag does. |
| **architecture.md** (this document) | How the service is built: the process model, the data flow through every worker, each subsystem, and the API surface. The design authority. |
| [ops.md](ops.md) | The operator's-eye view: dependencies, deployment shape, data flows, bootstrap order, and the failure modes with the signal that shows each one. |
| [observability.md](observability.md) | Every metric the service exports and what a change in it means. |
| [permissions.md](permissions.md) | The per-RPC permission matrix: which scopes each method accepts and which ACL check it applies. |

This document does not cover metric names (see
[observability.md](observability.md)), what to do when a subsystem breaks (see
[ops.md](ops.md)), or how to run the thing locally (see the
[README](../README.md)). It describes the design, not the operation.

## Process model

The server is a single binary, `cmd/repository run`, that starts every
subsystem in one process. There are no separate worker deployments and no
role flags — **every replica runs every enabled subsystem, and the ones that
must be singletons coordinate through Postgres job locks rather than through
deployment topology.** Scaling out therefore adds API and streaming capacity
but does not add archiving or eventlog-building capacity.

| Goroutine | Job lock | Conditional on | Notes |
|---|---|---|---|
| Notification listener | — | always | `LISTEN`/`NOTIFY` fan-out; runs on the pubsub pool, not the bouncer pool. |
| Document lock cleaner | `cleaner` | always | Wakes every 5 minutes. |
| Schema generation bootstrap | `bootstrap-generation` | always | Runs to completion during startup, before the validator is built. |
| Validator reload loop | — | always | Per-instance in-memory state. |
| Workflow reload loop | — | always | Per-instance in-memory state. |
| Type configuration reload loop | — | always | Per-instance in-memory state. |
| Eventlog builder | `eventlog-builder` | `!--no-eventlog-builder` | |
| Event forwarder | `forwarder` | `!--no-eventsink` and a sink is configured | |
| Publish scheduler | `scheduler` | `!--no-scheduler` | |
| Archiver: delete/restore/purge poll loop | **none** | `!--no-archiver` | Concurrency-safe by `FOR UPDATE SKIP LOCKED`, not by a lock. |
| Archiver: eventlog archiver | `eventlog-archiver` | `!--no-archiver` | |
| Archiver: eventlog batch archiver | `eventlog-batch-archiver` | `!--no-archiver` | |
| Archiver: generation archiver | `generation-archiver` | `!--no-archiver` | |
| SSE server | — | `!--no-sse` | Per-instance replay buffer. |
| Document stream fan-out | — | `!--no-websocket` | Created by the socket handler; per-instance replay buffer. |
| API server (HTTP, and TLS when a cert is configured) | — | always | |

Three of those distinctions are load-bearing:

* **The three reload loops and the two replay buffers are per-instance, not
  per-cluster.** Two replicas can be enforcing different schema generations,
  different workflows, or different type configurations at the same time, and
  each SSE and websocket client sees only the buffer of the instance it is
  connected to. Convergence is driven by a notification published in the
  committing transaction, so it is normally immediate; the five-minute timer
  behind each loop is a fallback for a lost notification, not the expected
  path — see [Schemas, generations and the
  validator](#schemas-generations-and-the-validator).
* **The archiver's delete/restore/purge poll loop is the one background worker
  with no job lock.** It runs on every replica with the archiver enabled and
  relies on `SELECT ... FOR UPDATE SKIP LOCKED` to keep replicas off each
  other's work. Adding replicas therefore does add delete, restore and purge
  throughput, unlike every other background worker.
* **An archiver failure takes its own process down, one replica at a time.** The
  four archiver goroutines run under `elephantine.NewErrGroup` with 30 retries
  at a 10-second static backoff — roughly five minutes of retrying, with the
  counter reset after an hour without a failure. Past that the group fails, and
  because the archiver shares an `errgroup` with the API server, that process
  exits. This is deliberate: durability is not a best-effort side job (see
  [ADR 0001](adr/0001-signed-archive-merkle-chain.md)), and an instance that
  cannot archive should not keep serving writes it cannot make durable.
  **It is not normally a fleet-wide outage**, because of the section below.

### What a non-leader's archiver actually does

`JobLock.RunWithContext` blocks until the lock is acquired and only then calls
the worker; a replica that does not hold the lock parks there, re-attempting
every `CheckInterval` (20 s by default) until the context is cancelled.
**A parked worker cannot fail, so three of the four archiver goroutines do
nothing at all — and risk nothing at all — on a replica that is not the
leader.**

That is what keeps an archiving failure from being a fleet-wide event. With S3
unreachable and no delete, restore or purge work pending, only the lock holder
is making S3 calls. It exhausts its retries and exits after about five minutes;
another replica steals the now-stale lock (`StaleAfter` is 40 s by default),
fails the same way, and exits about five minutes later. The result is a rolling
restart of the fleet, one pod at a time, and with more than a couple of replicas
the API keeps serving throughout.

Two paths break that property and fail on every replica at once:

* **The delete/restore/purge poll loop has no job lock**, so it runs everywhere.
  It only makes S3 calls when there is work to do — each `process*` function
  starts with a query that returns no rows when idle — so an S3 outage with
  deletes pending fails every replica simultaneously, where the same outage with
  nothing pending only troubles the leader.
* **`ensureSigningKeys` runs before the errgroup is even created, so it gets no
  retries at all.** If it fails, `Run` returns immediately and the process
  exits in seconds rather than in five minutes. It touches S3 only when there
  are unarchived keys — a fresh installation, or the window just after a key
  rotation — which is exactly why a developer pointing a new repository at a
  missing archive bucket sees an instant exit rather than a slow one.

The `s3` readiness check exists on every replica — it writes, reads and deletes
a probe object in the archive bucket — and is registered with
`AddOptionalReadyFunction` **specifically so that it cannot undo the rolling
property described above.** As a hard check it would fail readiness on every
replica the moment the bucket became unreachable, deregistering the entire fleet
and turning a degraded background dependency into a total outage; the
synchronous API needs only Postgres for reads and document writes. A failing
optional check still reports `"ok": false` and drives
`health_check_up{name="s3"}` to 0, so it remains alertable. Durability is
enforced by the archiver exiting, not by the probe. See
[ops.md](ops.md#archiving-has-stalled).

### Connection pools

There are up to two pgx pools. `--db` is the direct connection; `--db-bouncer`,
when set and different, becomes the pool used for everything except pub/sub.
**`LISTEN`/`NOTIFY` does not survive PgBouncer's transaction pooling, so the
notification listener always runs on the direct pool** — configuring only the
bouncer connection string would silently break every notification-driven
refresh in the process.

Neither pool sets an explicit `MaxConns`, so pgx defaults to
`max(4, runtime.NumCPU())`. On Kubernetes with the default CPU manager policy
that tracks the *node's* vCPU count rather than the container's quota, so pool
size changes invisibly when a pod is rescheduled onto a differently-sized node.
Set `pool_max_conns` in the connection string if that matters.

## Data flow

### 1. The write path

`Documents.Update` and `Documents.BulkUpdate` are the only ways document state
changes. The sequence, in `PGDocStore.Update`:

```
  RPC ──▶ scope check ──▶ ACL check ──▶ upload existence check
      ──▶ serialise document (outside the transaction)
      ──▶ BEGIN
          ├─ SELECT ... FOR UPDATE on document(uuid)    ← write serialisation
          ├─ preflight: if-match, workflow state, status heads
          ├─ validate against the in-memory validator
          ├─ insert document_version / document_status rows
          ├─ update document / status_heads
          ├─ compute timespans + labels from the type configuration
          ├─ intrinsic metrics (character count)
          ├─ INSERT INTO event_outbox_item                ← the event
          └─ NOTIFY event_outbox
          COMMIT
```

**The row lock on `document(uuid)` is what makes sequential version and status
numbering safe** — versions are `current_version + 1` read under the lock, not
a sequence, so a document's versions are dense and gapless. Everything that
writes to a document takes that lock first, which is also why a single
pathological document serialises its own writers but never blocks writers of
other documents.

**The version row and the event that announces it are written in the same
transaction.** That is the transactional outbox pattern: an event exists if and
only if its change committed, so no write is silently unannounced and no
rolled-back write leaks an event. See
[ADR 0002](adr/0002-event-outbox-to-eventlog.md).

The serialisation and validation work deliberately happens *before* `BEGIN` so
that marshalling cost and schema validation are not paid while holding a row
lock and a pooled connection.

#### Meta documents

A document whose URI marks it as a meta document is attached to a main
document. Meta document writes take the main document's row lock too, so a meta
write and a main write cannot interleave. The `meta_doc_write_all` scope
bypasses the ACL check for meta writes only; `accessCheck` explicitly
re-requires `doc_write` or `doc_admin` when the requested permission set
includes a plain write, so the scope cannot be used to create ordinary
documents.

### 2. Outbox to eventlog

The `EventlogBuilder` is the only writer of the `eventlog` table. It reads up
to 20 outbox rows at a time, and per row, in one transaction: inserts the
eventlog row with ID `lastID + 1`, publishes `NOTIFY eventlog`, and deletes the
outbox row.

**Event IDs come from the builder, not from a sequence or a clock, and the
builder is a singleton under the `eventlog-builder` job lock** — that is what
makes the eventlog a dense total order that consumers can resume from by ID.
A second builder would produce duplicate IDs; the job lock is not an
optimisation.

It wakes on `NOTIFY event_outbox` and otherwise polls once a minute. When it
reads a full batch it loops immediately without waiting, so a backlog drains at
the speed of the database rather than at the poll interval. The one-minute poll
is the floor on how long a *dropped* notification can delay an event.

#### Events that no longer exist

Workflow state changes used to be their own `workflow` events, and an ACL
update that accompanied a new version used to be its own `acl` event. Both are
now folded onto the `document` or `status` event that caused them, as
`workflow_state`/`workflow_checkpoint` and `acl`. This history is worth
knowing, because the old shape had a real defect: the version event was emitted
*before* the accompanying ACL event, so a consumer could observe a new version
before the permissions it was created with. Folding them into one event fixed
that, and **re-splitting them would reintroduce the ordering hole.**
`--emit-workflow-event` and `--emit-acl-event` re-emit the legacy events
alongside the folded fields as a transition aid for external consumers, and are
slated for removal. Standalone `acl` events are still emitted for an ACL update
that comes with no new version, and on archive restore.

### 3. Fan-out

Three consumers read the eventlog independently, each keeping its own position.
None of them can hold the others back.

```
 eventlog ──┬──▶ SSE            per-instance, 200-event replay buffer, /sse
            ├──▶ DocumentStream per-instance, --eventlog-buffer-size buffer,
            │                   enriched with document + meta, /websocket/:token
            └──▶ EventForwarder single-leader (`forwarder` lock), position in
                                the `eventsink` table, AWS EventBridge
```

* **SSE** (`repository/sse.go`) prepopulates its 200-message replay buffer from
  the database at startup, then follows `NOTIFY eventlog` with a 5-second
  ticker as a floor. Topics are `firehose`, the document type, the event type,
  and `<event>.<type>`; clients resume with `Last-Event-ID`. An event whose
  JSON marshalling fails is skipped and never retried — the position advances
  past it.
* **DocumentStream** (`repository/document_stream.go`) is the enriching
  fan-out behind the websocket API. For each batch of events it bulk-loads the
  *latest* version of every referenced document plus its meta, so a subscriber
  gets the event and the document in one item. Events with
  `system_state = restoring` are dropped here, so websocket subscribers never
  see restore traffic. `SubscribeFrom` delivers the buffered replay
  synchronously under the emit lock before registering the live handler, which
  is what lets the replay be exempt from rate limiting without a subscriber
  ever seeing a live event interleaved with its catch-up.
* **EventForwarder** (`sinks/eventsink.go`) reads through the public
  `Documents.Eventlog` RPC with an internal `doc_read_all eventlog_read`
  identity, enriches each `document` and `status` event with the document body,
  and posts to EventBridge. A document deleted before the forwarder reaches its
  event is counted as skipped and the event is downgraded to ignored rather
  than retried forever.

### 4. Archiving

Four workers, three of them single-leader. The archive is not a backup: every
object embeds its parent's signature, so the whole thing is a verifiable chain
(see [ADR 0001](adr/0001-signed-archive-merkle-chain.md)).

```
 eventlog
    │
    │  `eventlog-archiver` lock, 100 rows/poll, 100ms→5s adaptive delay
    ▼
 events/<20-digit id>.json          ← signed, embeds parent event's signature
    │  + documents/<uuid>/versions/…  and  documents/<uuid>/statuses/…
    │
    │  `eventlog-batch-archiver` lock, on archived-item notify or 1min poll
    ▼
 events_1k/<first>_<last>.zip       ← 1000 events + signatures.txt
    │
    ▼
 events_10k/<first>_<last>.zip      ← built from ten 1k zips

 schema_generation_event
    │  `generation-archiver` lock, 10s poll
    ▼
 generations/events/<id>.json, generations/<id>/{generation,schemas,exemplars}
```

**The event archiver refuses to skip.** It requires the next event to be
exactly `position + 1` and fails otherwise, because a gap would break the
signature chain and make the archive unverifiable from that point on.
`--tolerate-eventlog-gaps` relaxes this, and also makes the batch archiver
tolerate a missing `NoSuchKey` object; it exists for repositories with
pre-existing holes and should not be on by default.

Each archived event's S3 write happens *before* the transaction that records
the new position commits, with a cleanup deferred to remove the object if the
transaction fails. Order matters: the object store is the authority on what was
archived, and the database position follows it.

Signing is done at archive time rather than write time because Postgres
`jsonb` is not byte-stable, so a signature over a database row could not be
re-derived later. **The canonical signed artifact is the marshalled S3 object,
which means verifying live database contents is necessarily indirect**: verify
the archive object, then check the row is *logically* equivalent to it, not
byte-equal.

#### Signing keys

`ensureSigningKeys` runs at archiver startup and then at most once every 24
hours from the poll loop, under the `LockSigningKeys` advisory transaction lock
so concurrent replicas cannot both mint a key. Keys are ECDSA P-384, valid for
180 days, with a new key generated 7 days before the current one expires and a
2-day heads-up before it is used. Key IDs are base-36 counters. Public keys are
served unauthenticated at `GET /signing-keys` as JWKS with `iat`/`nbf`/`exp`,
and written to `signing-keys/<kid>.json` in the archive bucket.

Because the replacement is minted 7 days out and only becomes usable after 2,
handover happens about five days before the outgoing key expires.
`SigningKeySet.CurrentKey(t)` picks the newest key whose validity window
contains `t`, and returns nil if none does. **`storeArchiveObject` refuses to
write an object it cannot sign** — that nil check is what guarantees the archive
never contains an unsigned or wrongly-dated object, and it is the invariant any
change here has to preserve.

The unwelcome consequence today is that a missing key is expressed as a
per-object error, which the archive workers' retry group escalates into a process
exit — and `ensureSigningKeys` itself runs *before* that retry group exists, so
its own failures are not retried at all. Key provisioning is therefore the most
fragile step in the process, which is backwards. Making it fail softly while
keeping the invariant above is
[pending work](../README.md#pending-work); until it lands, treat a signing-key
failure as fatal rather than degrading.

Anyone verifying the archive independently must keep their own copy of the
keys. The archived copies are a convenience: an attacker who can rewrite the
archive can rewrite the keys stored beside it, so keys colocated with the data
they sign prove nothing.

#### Deletes, restores and purges

The delete/restore/purge poll loop is the unlocked worker. Each pass tries each
of the three in turn, and a pass that finds no work backs off a randomised
~250–500 ms; a pass that does work runs again immediately.

**A delete is finished from the client's point of view long before it is
finished in the archive, and the archiver is what closes the gap.** `Delete`
takes the document's row lock, waits for its versions and statuses to be fully
archived, writes a `delete_record`, and replaces the document row with a
`system_state = deleting` placeholder. Reads and writes are refused from that
point. The archiver then moves every `documents/<uuid>/` object to
`deleted/<uuid>/<record id>/` with eight concurrent workers, **moves** each
attached asset out of the asset bucket into `…/attached/<name>` in the archive
bucket — a copy followed by a delete of the source, so a deleted document's
attachments no longer exist in the asset bucket — writes a `manifest.json`, and
only then deletes the document row and marks the record finalised. The archiver
owns the finalisation specifically so that consistency between Postgres and S3
never has to be maintained across a transaction and an object-store call.

The manifest is the authority for a restore, and once the document row is gone
it is the only record of the document's last version, status heads, ACL and
attachments.

A restore creates a `system_state = restoring` document row, which is *not*
announced on the eventlog, and replays versions and statuses from the delete
manifest. Every resulting event carries `system_state = restoring` so
processors can ignore them, and a `restore_finished` event marks the end.

A purge removes the archived objects and clears the version, ACL and status
head data from the delete record. What survives is the audit trail: UUID, URI,
type, who deleted it and when, and when it was purged.

### 5. Scheduled publishing

The scheduler holds the `scheduler` job lock and looks for documents in the
`withheld` workflow state with a planned publish time. It polls at most once a
minute — that bounds how fast a *newly scheduled* document is discovered, not
the resolution at which publishing happens, since the loop sleeps until the
next known publish time when there is one.

Publishing is an ordinary `Documents.Update` performed as
`internal://scheduler` with `doc_admin`, setting `usable` on the withheld
version. It is guarded by `IfWorkflowState: "withheld"` and by the withheld
status head ID, so a document that moved on in the meantime is not published
out from under whoever moved it. Documents whose publish time passed more than
30 minutes ago are no longer attempted; they stay withheld until something else
acts on them. Sources listed in the exclude list (currently `oc`) are skipped
entirely.

### 6. Document lock cleaner

Runs every 5 minutes under the `cleaner` lock and deletes `document_lock` rows
that expired at least 5 minutes ago.

This is purely storage reclamation: request handlers and the acquire path
filter on `expires > now` independently, so an expired lock is already
invisible to writers before the cleaner sees it. **The cutoff must lag `now`,
never lead it.** It was once `now + 5m`, which made every freshly-acquired lock
with a TTL of 5 minutes or less eligible for deletion on the next tick —
elephant-collab's 5-minute default meant its locks were nearly always swept,
surfacing to holders as spurious "document locked" and "not locked" errors
immediately after a successful acquire. The sign of that cutoff is the whole
bug.

## Subsystems

### Schemas, generations and the validator

A document type must be declared by a schema before it can be stored. Schemas
are [revisor](https://github.com/ttab/revisor) constraint sets, and a
*generation* is an immutable, content-addressed set of schema versions plus
exemplar documents, identified by a hash over its contents.

A generation is `pending`, `active` or `deactivated`. Registration is
idempotent on that hash — **re-registering a known set of schema versions
returns the existing generation's ID, and since v1.9.0 also applies the
activation the caller asked for.** It previously returned the ID without
activating, so a deploy that re-registered its known-good schema set as
`ACTIVATION_ACTIVE` reported success while the previously active generation
stayed in place, with no signal that nothing had happened. A registration that
does not ask for activation still leaves an existing generation's status alone;
deactivation is `SetActive`'s job, and an unspecified activation is
indistinguishable from a deliberate deactivation by the time registration
sees it.

Validation is served from a `*revisor.Validator` held in memory per instance,
rebuilt by the reload loop on `NOTIFY schemas`, `NOTIFY deprecations`,
`NOTIFY type_configured`, an explicit `RefreshSchemas` call, or a five-minute
timer. The RPCs that report the active generation read the database.
**Those two can disagree, which is why `elephant_validator_schema_generation`
exists**: the RPC can report a generation that no instance is yet enforcing,
and comparing the gauge against it is the only way to see an instance serving
stale schemas.

**Propagation is normally immediate, and the timer is a fallback, not the
mechanism.** Activation publishes its `SchemaEvent` through
`FanOut.Publish(ctx, tx, …)` inside the activating transaction, so PostgreSQL
delivers the `NOTIFY` on commit — no event is emitted for an activation that
rolls back, and every listening instance wakes as soon as one commits. The
five-minute timer only matters when a notification is *lost*, which in practice
means the listener connection dropped and reconnected, or the pubsub pool was
misrouted through PgBouncer. A notification dropped because a reload was already
in flight costs nothing: the subscriber does a non-blocking send onto a buffer of
one, and since a reload reads current state rather than applying a delta, a
coalesced notification loses no information. The operational consequence is in
[ops.md](ops.md#an-instance-is-validating-against-stale-schemas) —
**a propagation lag of more than a few seconds is a symptom, not the design.**

A failed read of the active generation ID fails the whole reload rather than
swapping in a validator labelled generation 0 — a mislabelled validator would
make the gauge lie, which is worse than a stale one that says so.
`elephant_schema_refresh_failures_total` counts the failures; the instance
keeps enforcing what it last loaded, indefinitely if the failures continue.

A pending generation, if one exists, is also compiled and every write is
soft-validated against it. Failures are counted in
`elephant_pending_validation_failures_total` and logged but never rejected, so
the counter is a forecast of what would break if the pending generation were
activated. A pending generation that fails to load is logged and skipped rather
than failing the reload, because soft validation must not be able to stop the
active schemas from refreshing.

Deprecations are per-label and either enforced or not. An unenforced
deprecation logs, increments `elephant_deprecations_total` and
`elephant_docs_with_deprecations_total`, and allows the write; an enforced one
fails validation.

The server no longer registers the embedded core schemas at startup — that was
removed in v1.7.0 along with `--no-core-schema` and `--ensure-schema`. Schema
management belongs to administrative tooling now. Tests still install the
embedded schemas for themselves.

### Workflows and statuses

A status is a named, per-document, sequentially numbered pointer at a document
version. `usable` is publication by convention, not by mechanism. **The last
status of a given name is the "head", and `heads.<status>.id` is the count of
how many times that status has been set — not a version number.** The version
the head points at is a separate field; conflating them is a recurring mistake.

Setting a status with version `-1` is an unpublish: a status that points at no
version.

A *workflow* is a per-type ordered set of steps with an optional checkpoint.
A type with no explicit workflow gets one synthesised from its configured
statuses: no checkpoint, every non-disabled status a step. Workflow state is
folded onto the event that changed it (see
[Events that no longer exist](#events-that-no-longer-exist)).

*Status rules* are [expr](https://github.com/expr-lang/expr) expressions
compiled at load time and evaluated per status update. A rule sees the status,
the update, the document, the version meta, the current heads, the caller's
claims, and the current workflow state — that last one is what makes rules like
"only allow unpublish if previously published" expressible. A rule marked as an
access rule turns its violation into a permission error rather than a
validation error. `Document.Type` is defaulted to the document's type when no
concrete version is loaded, so rules on `version = -1` updates are evaluated
rather than silently skipped.

Workflows, statuses and rules are per-instance in-memory state, reloaded on the
same pattern as the validator: immediately on a `NOTIFY workflows` published in
the changing transaction, with a five-minute timer behind it as a fallback for a
lost notification.

### Type configurations

Per-document-type configuration, `Schemas.ConfigureType`, that decides what
gets derived from a document on write:

* **Time expressions** produce the `tstzmultirange` in `document.time`, which
  is what time-range document listings query.
* **Label expressions** produce `document.labels`.
* **Variants** declare variant type names (`core/article#timeless`) that the
  validator resolves; they are compiled into `revisor.Variant` values and
  attached to the validator, so a variant change is a validator rebuild.
* **Bounded collection** marks types whose membership is enumerable.

Reloaded immediately on `NOTIFY type_configured`, **with a five-minute timer
added behind it**. The timer is recent, and the history is worth knowing: this
loop used to reload on notification *only*, so a single lost notification left an
instance deriving timespans and labels from a stale configuration until someone
restarted it — silently, since the write path succeeds either way. The timer
bounds that to five minutes; it is not the expected propagation path, and a
configuration change that routinely takes minutes to take effect means
notifications are not arriving. The extractors are rebuilt wholesale on each refresh; a
configuration that fails to compile leaves the previous one in place rather
than crashing, once the first load has succeeded. The *first* load failing is
fatal, because there is nothing to fall back to.

### Document locks

A pessimistic, advisory-by-cooperation lock: a client calls `Documents.Lock`
(or sets `Lock` on a `Get`), receives a secret token, and holds it for a
client-chosen TTL, extending with `ExtendLock` and releasing with `Unlock`.

Exclusivity decides what the lock actually blocks:

| Level | Blocks |
|---|---|
| `LOCK_DOCUMENT` (default) | Document updates: new versions, attach/detach, delete |
| `LOCK_STATUS` | The above plus status updates |
| `LOCK_ACL` | The above plus ACL updates |
| `LOCK_EXCLUSIVE` | Document, status and ACL updates |

**The default deliberately does not block status or ACL updates.** Before
v1.9.0 a lock blocked all three, which contradicted the documented behaviour;
consumers that relied on the old breadth must now ask for the matching
exclusivity level. Supplying a *wrong* token is rejected regardless of
exclusivity — exclusivity narrows what an absent token blocks, not what a bad
one does.

A failed acquisition returns the holder's identity, application, comment,
expiry and exclusivity as Twirp error metadata (`lock_holder_sub`, `lock_app`,
`lock_comment`, `lock_expires`, `lock_exclusivity`) rather than an opaque
"locked by someone else", so a client can tell "I already hold this" from a
real conflict.

### Uploads and attachments

`Documents.CreateUpload` returns an upload ID and a presigned S3 PUT URL (15
minute expiry). The client uploads the bytes, then references the upload ID in
an `Update` call, which attaches it to the document. The `Update` path verifies
that the object actually exists before starting any work, so a forgotten upload
fails the write rather than producing a document with a dangling attachment.

Attach and detach show up on the document event as `attached_objects` and
`detached_objects`, and in `GetMeta`. Download links come from
`GetAttachments` with `DownloadLink` set.

**Attached objects are not archived, and that is a deliberate open question**
rather than an oversight: if this is used for images and video, automatically
duplicating them may not be wanted. They *are* copied into the archive bucket
when their document is deleted, so a document can be restored with its
attachments — but only the latest version of the currently attached objects.
Backup of attachments is out of scope for the repository.

### Document metrics

The `Metrics` service stores per-document integer measurements under a *kind*
with an aggregation mode (`replace` or `increment`). These are document data,
not operational telemetry; nothing here reaches Prometheus.

A `MetricCalculator` can compute measurements during the write transaction.
`CharCounter` is the only built-in one, enabled unless `--no-charcounter`.
Write access can be narrowed to a single kind with the subscope
`metrics_write:<kind>`.

## Twirp APIs and scopes

Four Twirp services under `/twirp/elephant.repository.<Service>/<Method>`,
speaking protobuf or JSON. The service definitions live in
[elephant-api](https://github.com/ttab/elephant-api/blob/main/repository/service.proto),
not in this repository.

| Service | Settles |
|---|---|
| `Documents` | Document CRUD, versions, statuses, ACLs, locks, uploads, eventlog reads, delete/restore/purge |
| `Schemas` | Schema and generation registration and activation, meta types, type configuration, deprecations |
| `Workflows` | Statuses, status rules, per-type workflows |
| `Metrics` | Document metric kinds and values |

Plus three non-Twirp endpoints: `/sse`, `/websocket/:token`, and the
unauthenticated `GET /signing-keys`.

### Scopes

Authorization is a JWT scope check followed, for document operations, by a
per-document ACL check.

| Scope | Grants |
|---|---|
| `doc_read` | Read documents, subject to the ACL |
| `doc_read_all` | Read any document, bypassing the read ACL |
| `doc_write` | Create versions, subject to the ACL |
| `doc_delete` | Delete documents, subject to the ACL |
| `doc_restore` | Restore deleted documents; list deleted |
| `doc_purge` | Purge deleted documents; list deleted |
| `doc_import` | Use import directives on `Update` |
| `doc_admin` | Everything document-related, bypassing every ACL check |
| `meta_doc_write_all` | Write meta documents, bypassing the meta-write ACL only |
| `asset_upload` | Create uploads |
| `eventlog_read` | Read the eventlog over RPC, SSE and websocket |
| `schema_read` | Read schemas, generations, exemplars |
| `schema_admin` | Register and activate schemas and generations; configure types; meta types; deprecations |
| `workflow_admin` | Statuses, status rules, workflows |
| `metrics_read` | Read document metrics and kinds |
| `metrics_write` | Write document metrics; `metrics_write:<kind>` narrows it to one kind |
| `metrics_admin` | Register and delete metric kinds; read and write |

ACL entries grant `Read`, `Write`, `MetaWrite` and `SetStatus` to a subject or
a unit URI. **A document is private to its creator until it is shared** —
there is no default-readable state. Scope-based bypasses are the only way past
an ACL: `doc_admin` for anything, `doc_read_all` for reads,
`meta_doc_write_all` for meta writes.

Every method except the unimplemented `Documents.Evict` requires a scope.
`Documents.Validate` and `Documents.Prune` take the same write scopes as
`Update`, because both are dry runs of the write path rather than reads.

**A valid token is required before a request reaches any Twirp handler.**
`SetJWTValidation` rejects a request with no or invalid `Authorization` header
with a 401 rather than passing it on, so authorization is default-deny: a handler
that forgot its scope check would fail closed. That is belt and braces, not a
substitute — the scope check is still part of writing a method, since the
middleware knows nothing about which scope a method needs.

The middleware does not cover every route, and the exceptions are deliberate:

| Route | Authentication |
|---|---|
| `POST /twirp/…` | Middleware requires a valid token, then the handler asserts its scope |
| `GET /sse` | Same middleware; the `token` query parameter is copied into the `Authorization` header first, so a browser client that cannot set headers still authenticates |
| `GET /websocket/:token` | Bypasses the middleware — the socket token in the path is verified against the server's socket key, and the session then authenticates with a JWT |
| `GET /signing-keys` | Bypasses the middleware. Public by design: it is what makes independent verification of the archive possible |

The middleware validates on every request; there is no JWT caching, which is
noted as a `TODO` at the call site. Moving to elephantine's `ServiceOptions` with
`ServiceAuthRequired` would put this in a Twirp hook instead of HTTP middleware —
see [pending work](../README.md#pending-work). See also
[ops.md](ops.md#security).

`repository/permissions.go` is the authority for the scope constants;
[permissions.md](permissions.md) has the per-method matrix.

## Database

Numbered [tern](https://github.com/jackc/tern) migrations in `./schema/`,
queries in `postgres/query.sql` compiled by [sqlc](https://sqlc.dev/) into
`postgres/query.sql.go`. **Every application query goes through sqlc**; there
is no hand-written SQL elsewhere in the codebase.

The shape:

* `document` is one row per document, carrying `current_version`, `updated`,
  `updater_uri`, `nonce`, `system_state`, the derived `time` multirange and
  `labels` array, and `main_doc` for meta documents.
* `document_version` is append-only, one row per version.
* `document_status` is append-only; `status_heads` carries the current head per
  name.
* `event_outbox_item` is the transactional outbox; `eventlog` is the ordered,
  enriched log.
* `eventlog_archiver` holds one row per archive granularity (1, 1000, 10000)
  with a position and the last signature — the chain state.
* `document_archive_counter` tracks how much of a document is still
  unarchived, which is what `Delete` waits on.
* `delete_record` and `restore_request` drive the delete and restore
  lifecycles.
* `schema_generation` and friends hold generations, their schemas, exemplars,
  lifecycle events and archiver position.
* `job_lock` backs the single-leader coordination.

The relationship between `document` and its versions and statuses used to be
formalised in `create_version` and `create_status` stored procedures. **Those
were dropped in migration `024_index_foreign_keys.sql`; the logic lives in
`PGDocStore` now.** Anything that still refers to those procedures is stale —
they do not exist, and migration 024 must only be applied after v1.4.0 is
deployed, because the pre-v1.4.0 code calls them.
