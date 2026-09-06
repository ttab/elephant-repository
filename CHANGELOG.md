# Changelog

All notable changes to this project after v1.0.0 are documented here. The
entries below are derived from release tags; see the linked PRs for full
detail.

## [v1.9.0] - Unreleased

**New API surface (Connect):** every method is now served a second time, on
`POST /elephant.repository.<Service>/<Method>` in addition to
`POST /twirp/elephant.repository.<Service>/<Method>`. The new paths speak the
[Connect](https://connectrpc.com/) protocol in protobuf or JSON, and, to callers
inside the cluster, gRPC and gRPC-Web. Both mounts wrap the same handlers behind
the same authentication middleware and the same scope and ACL checks, so nothing
about a call but its encoding depends on which family it arrived on, and no
scope, message or event changed. **The Twirp paths are unchanged and stay** —
they are removed in a future major release, not on a traffic timer.

Four things differ for a caller that moves:

- The error body is `{"code":"not_found","message":"...","details":[…]}`
  instead of `{"code":"not_found","msg":"...","meta":{…}}`. The code strings are
  the same, so a client that branches on `code` needs no new cases, but `msg` is
  `message` and the error metadata — the `lock_*` keys on a lock conflict,
  `argument`, `required_any_of_scopes` — moves from the `meta` map into an
  `elephantine.rpc.ErrorMeta` detail. A Go client reads it with
  `rpc.Meta(err)`, a TypeScript client with `findDetails(ErrorMeta)`.
- Three codes are answered with a different HTTP status: `failed_precondition`
  with `400` rather than `412`, `canceled` with `499` rather than `408`, and
  `deadline_exceeded` with `504` rather than `408`. `failed_precondition` is
  the one that matters, since document locks, system locks and workflow rule
  violations return it — **anything keyed on 412 at an ingress, in a dashboard
  or in a client has to read the RPC code instead**.
- **A JSON response spells its field names differently.** Connect marshals with
  protojson's defaults, so a field declared `ref_type` comes back as `refType`;
  Twirp marshals with `UseProtoNames` and keeps `ref_type`. Requests are
  unaffected, since protojson accepts either spelling on both stacks, and so are
  the generated Go, `@protobuf-ts` and `connect-es` clients, which parse into the
  message type. **A caller that reads a JSON response by hand with `fetch` or
  `curl` and changes only the path prefix gets a `200` and reads `undefined` for
  every multi-word field.** This is deliberate: the standard Connect encoding is
  what every Connect runtime and generated client assumes, so the mount does not
  install a `UseProtoNames` codec to make Connect look like Twirp.
- Connect clients send `Connect-Protocol-Version` and `Connect-Timeout-Ms`,
  which are now in the CORS allow list. The server does not require the version
  header, so `curl` and raw `fetch` keep working.

gRPC and gRPC-Web are served on the Connect paths, and the plaintext listener
now speaks HTTP/2 as well as HTTP/1.1 so that a gRPC client can reach it at all.
**That reach ends at the cluster**: the ingress speaks HTTP/1.1 to its targets
and no gRPC target group is provided, so gRPC is a way for another service to
call this one and is not offered to external callers.

An ingress that routes on `/twirp/` needs a sibling rule for the new paths
before a client can use them.

**Breaking (authorization):** the API now requires a valid token. A request to a
Twirp method or to `/sse` with a missing or invalid `Authorization` header is
answered with 401, so authorization is default-deny. `GET /signing-keys` stays
public and `GET /websocket/:token` continues to authenticate with its own socket
token.

**Breaking (authorization):** five methods now require a scope.
`Schemas.GetDocumentTypes`, `Schemas.GetMetaTypes` and
`Schemas.ListActive` require `schema_read` or `schema_admin`.
`Documents.Validate` and `Documents.Prune` require `doc_write`, `doc_admin` or
`meta_doc_write_all` — the same set as `Documents.Update`, since both are dry
runs of the write path.

Callers of these five need the scopes above and will otherwise get
`permission_denied`. `eleconf` calls all three `Schemas` methods, so a read-only
configuration run needs `schema_read`; an applying run already holds
`schema_admin` for the registration calls it makes. Note the asymmetry when
granting scopes: `Validate` and `Prune` read schemas but live on the `Documents`
service and take *write* scopes, not `schema_read`.

**Behaviour change (authentication errors):** an unauthenticated call is now
answered by elephantine's shared authentication middleware, which renders the
error in the protocol the caller is speaking instead of writing a plain-text
body. The status and the code do not move — a missing token and an invalid one
are both `401`/`unauthenticated`, as they were — but the body does: a Twirp
caller gets a Twirp error body (`{"code":"unauthenticated","msg":…}`), a Connect
caller a Connect one, and a gRPC or gRPC-Web caller the code in trailers. `GET
/sse` moves with them, from the plain-text `invalid authorization: …` it used to
answer to a Connect-shaped JSON error body, since nothing about that request
says which RPC protocol its caller speaks. **A client that matched on the old
plain-text body has to read the status or the `code` field instead.** A scope
failure on `/sse` is unaffected: that comes from the SSE handler, not the
middleware, and is still plain text.

**Behaviour change (eventlog long poll):** `Documents.Eventlog` answers
`deadline_exceeded` when the wait was ended by the caller's deadline, and
`canceled` only when the caller went away. It answered `canceled` for both. This
is visible on the Connect paths, where a client's deadline travels as
`Connect-Timeout-Ms` and becomes the handler's context deadline — Twirp had no
timeout header and ignored the deadline entirely — so a client that retries a
timeout but gives up on a cancellation could not previously tell the two apart.
`deadline_exceeded` is `504` on Connect and `408` on Twirp.

**Behaviour change (request bodies):** request bodies are capped at 8 MiB on
both listeners, which comes in with the elephantine upgrade. A request that
declares a larger `Content-Length` is refused with `413` before it reaches a
handler, and a body of unknown length fails on the read that passes the limit.
Bodies were unbounded before. A document update whose serialised request exceeds
8 MiB — a very large document, or a `BulkUpdate` of many documents — now fails
where it used to succeed.

**Behaviour change (document locks):** a document lock now only blocks
document updates (new versions, attached objects, deletes) by default; status
and ACL updates on a locked document are no longer blocked unless the lock was
acquired with a higher exclusivity level. This matches the long-documented API
behaviour, but consumers that relied on locks also blocking status or ACL
updates must now acquire their locks with a matching exclusivity. (#604)

**Build:** the module's `go` directive is `1.27.1`, the latest patch of the 1.27
line, up from Go 1.26.5, and the release image builds on
`golang:1.27.1-alpine3.24`. Anyone who builds the binary outside the Dockerfile
needs a toolchain at least that new; a builder pinned to an older one either
downloads it at build time or fails outright, depending on `GOTOOLCHAIN`.

**Behaviour change (metrics):** `rpc_requests_total`, `rpc_duration_seconds` and
`rpc_responses_total` keep their names, labels and label values, and both mounts
report into them, so a call is counted once whichever protocol carried it. But
`rpc_responses_total{status}` reports the status actually sent, so a Connect
`failed_precondition` lands on `status="400"` rather than `status="412"` — a
panel counting 412s for lock conflicts undercounts as callers move over, and one
counting 400s starts mixing lock conflicts with malformed requests. The
replacement is the new
`rpc_protocol_responses_total{service,method,protocol,code}` counter, which
carries the RPC code itself, so lock conflicts are `code="failed_precondition"`
regardless of protocol. Its `protocol` label (`twirp`, `connect`, `grpc`,
`grpc-web`) is also what says whether a method still has Twirp callers. Unauthenticated
calls are counted now as well: the authentication middleware reports the
response it writes into `rpc_responses_total{status="401"}` and
`rpc_protocol_responses_total{code="unauthenticated"}` and logs it, where the
old middleware answered without either, so a burst of 401s is visible for the
first time — and a dashboard that reads `rpc_responses_total` as successful
traffic now sees them.

**Behaviour change (readiness):** the `s3` readiness check is now optional, so
an unreachable archive bucket no longer fails `/health/ready`. It previously
returned 500 on every replica at once, deregistering the whole fleet and turning
a degraded background dependency into a total API outage — reads and document
writes only need Postgres. The check still runs and still reports
`"ok": false, "optional": true` in the response body, and still drives
`health_check_up{name="s3"}` to 0. **If you relied on the readiness probe to
react to an archive bucket outage, you now need an alert on
`health_check_up{name="s3"}`**, because nothing else reacts to it: the archiver
exiting the process remains the durability guarantee, and that only kills
replicas one at a time as the `eventlog-archiver` job lock moves between them.

**Migrations:**

- `027_lock_exclusivity.sql` — adds an `exclusivity` column to `document_lock` (`text`, not null, default `'document'`). **Must be applied before deploying v1.9.0**: the lock queries in v1.9.0 reference the new column, so acquiring, reading, or checking document locks fails against an unmigrated database. The migration is a plain `alter table add column` with a default on a small, short-lived table, so no maintenance window is needed.

Changes:

- `Schemas.GetDocumentTypes` now lists variant types (`core/article#timeless`) alongside the schema-declared types they are configured on. Variants are declared through `ConfigureType`, not in a schema, so they were missing from the listing entirely, and a client that enumerates types to read their configuration never saw them. `eleconf` did exactly that when diffing workflows, and so reported the workflow of every variant type as missing and re-applied it on every run. Consumers that iterate the response should expect entries containing a `#` suffix.
- `RegisterGeneration` now applies the requested activation to a generation that already exists. Registration is idempotent on the schema and exemplar versions, and it previously returned the existing generation's ID without activating it, so re-registering a known set of schema versions as `ACTIVATION_ACTIVE` reported success while leaving the previously active generation in place. Registrations that don't ask for activation still leave an existing generation's status alone; use `SetActive` to deactivate.
- The new `elephant_validator_schema_generation` gauge reports the schema generation an instance is actually validating against. Validation is served from an in-memory validator that reloads on notification or every five minutes, so `ListActiveSchemas` and `GetAllActiveSchemas` (which read the database) can report a generation before any instance enforces it; compare the gauge against the active generation to spot instances serving stale schemas.
- Failures to reload configuration are now observable rather than silent: `elephant_schema_refresh_failures_total` and `elephant_deprecation_refresh_failures_total` count failed reloads (the instance keeps enforcing what it last loaded), a failed read of the active generation ID now fails the whole schema reload instead of relabelling the validator as generation 0, and errors loading the pending generation's schemas or reading the generation ID for `GetAllActiveSchemas` are logged instead of discarded.
- Document type configuration is now reloaded every five minutes in addition to on notification. A dropped notification previously left an instance on a stale configuration until it was restarted.
- Document locks can be acquired with an exclusivity level via the new `exclusivity` field on `LockRequest` and on lock-on-Get (`AcquireLock`): `LOCK_DOCUMENT` (default, blocks document updates only), `LOCK_STATUS` (also blocks status updates), `LOCK_ACL` (also blocks ACL updates), or `LOCK_EXCLUSIVE` (blocks both). The level is exposed in `DocumentMeta.lock` and on lock conflicts via the `lock_exclusivity` error metadata key. Supplying a non-matching lock token is still rejected outright, regardless of exclusivity. (#604)
- Eventlog websocket subscriptions can now filter by event type via the new `GetEventlog.events` field, validated against the known event types. (#597)
- The document stream replay buffer is now slice-backed and configurable with `--eventlog-buffer-size` (`EVENTLOG_BUFFER_SIZE`, default 500). Resuming out of bounds still returns `eventlog_resume_oob`. (#597)
- Each subscription's live stream is now rate limited with a token bucket (`--eventlog-stream-burst` 70, `--eventlog-stream-rate` 10/s). On exceed, the events that fit are emitted followed by a `rate_limited` error, and the subscription is stopped; clients are expected to resubscribe. The initial resume replay is exempt. (#597)
- The documentation is now a set with a settled division of labour: `README.md` for orientation, commands and the full configuration reference; `docs/architecture.md` for the design; `docs/ops.md` for dependencies, failure modes and what to watch; `docs/observability.md` for every exported metric and what a change in it means. `docs/permissions.md` has been corrected — it was missing `doc_restore`, `doc_purge`, `meta_doc_write_all`, `asset_upload` and `metrics_read`, and did not record that `Restore` and `Purge` perform no ACL check. Relative links and heading anchors are checked by `mage docs:links` in the lint job.
- `Restore` and `Purge` are documented as performing no per-document ACL check. This is unchanged behaviour — the document is deleted, so there is no ACL left to check against — but it means `doc_restore` and `doc_purge` act on any deleted document and should be treated as administrative scopes.
- The API is served on both the Connect and the Twirp paths, as described above. Handlers construct their errors with the `elephantine/rpc` helpers, and the Twirp mount's interceptor translates them back, so a Twirp caller sees the same code, the same message and the same `meta` map as before. That equivalence is now tested rather than asserted: `TestIntegrationErrorParity` runs missing scope, not found, invalid argument, validation failure, lock conflict and failed precondition over both stacks against one server and compares code, message and every metadata key, `TestIntegrationErrorBodies` pins the raw JSON error bodies and HTTP statuses of both (including the `failed_precondition` case where Twirp answers 412 and Connect 400, and the 401 refusal the authentication middleware writes itself, which is the one error body no handler produces), and `TestIntegrationSuccessBodies` pins a success body per stack, which is what the JSON field-name difference is held to. An uncoded handler error is not translated by anything: every handler codes its own errors at the return site, a failed query or marshalling failure included, because the two stacks default an uncoded error differently — Twirp to `internal`, Connect to `unknown` — and the handler is the one place that knows which is right. The API test suite runs against both stacks — `TEST_RPC_STACK=connect go test ./repository/...` builds every test client from the generated Connect constructors, and CI runs the suite both ways.
- The service is served from `elephantine.APIServer` — the same server the rest of the fleet serves from — instead of a router, a hook chain, an authentication middleware and an `http.Server` of its own. Both RPC mounts are registered from one `elephantine.ServiceOptions`, so authentication, logging and the RPC metrics are identical across the stacks by construction rather than by two chains that have to be kept in step, and the shared RPC collectors are registered exactly once. The three endpoints that are not RPC moved onto the same server's mux, and which of them authenticates is unchanged: `/sse` alone goes through the shared authentication middleware, while `GET /websocket/:token` and `GET /signing-keys` bypass it as before. The test suite builds its server with the same registration code and `elephantine.NewTestAPIServer`, so what the tests measure is the shape production serves. Paths, CORS hosts and headers, the TLS listener, the h2c listener, the request body cap and the `/version` and `/health/alive` endpoints are unchanged; the error rendering described above is the one thing a caller can see.
- The plaintext listener is built with `elephantine.PlaintextProtocols()`, so it serves HTTP/1.1 and unencrypted HTTP/2 side by side rather than HTTP/1.1 alone. That is what makes the gRPC protocol the Connect mount serves reachable at all; without it a gRPC client fails on the connection with nothing in the logs to say why. The two are told apart by the HTTP/2 connection preface, so Twirp, SSE, the websocket upgrade and every other HTTP/1.1 caller are unaffected, and `TestIntegrationGRPC` calls the API over gRPC so the setting cannot be dropped unnoticed.
- Every handler error now carries an RPC code. The failed-query and marshalling paths that returned a plain Go error, which Twirp reported as `internal`, return `internal` explicitly on both stacks. One code changed as a result: `Metrics.RegisterMetricKind` answers an aggregation value it does not know with `invalid_argument` rather than `internal`, since it is the caller's value that is wrong.
- A `permission_denied` from a scope check now carries the scopes that would have been accepted as the `required_any_of_scopes` error metadata key, on both stacks. The message is unchanged. `GET /sse` answers a scope failure with the same status as before, but the plain-text body no longer has the `twirp error ` prefix in front of the code.
- Dependency upgrades: elephantine to v0.29.0 (the `rpc` package, the shared RPC collectors, the request body cap and the job lock's move to `pg/joblock`), elephant-api to v0.25.0 (the generated Connect handlers and clients), connectrpc.com/connect v1.20.0, the AWS SDK suite, urfave/cli/v3 to v3.11.0, minio-go to v7.3.0, MicahParks/keyfunc to v3.8.2, ttab/mage, and the Prometheus and `golang.org/x` support modules. (#597, #604)

## [v1.8.1] - 2026-06-10

- Bump newsdoc to v1.1.0, which adds an inline child selector `#(...)` to value extractor selectors: it gates a selector on having matching descendant blocks without terminating the chain or changing what is yielded, complementing the existing terminal `#` form. (#603)
- Dependency upgrades: Go to 1.26.4, elephant-api to v0.23.1, the AWS SDK suite, and golang.org/x/sync. (#603)

## [v1.8.0] - 2026-06-04

**Breaking (eventlog shape):** the changes below alter the events that external
consumers see on the eventlog. The previous behaviour can be restored
per-server with the flags listed; both flags only re-add the legacy standalone
events alongside the new folded representations (the folded fields are always
present) and are slated for removal in a future release.

- Workflow state changes are no longer emitted as standalone `workflow` events. `workflow_state` and `workflow_checkpoint` are folded onto the triggering `document` or `status` event that caused them. The `workflow_state` table is still updated as before. Consumers branching on `event == "workflow"` will stop seeing those events unless `--emit-workflow-event` (`EMIT_WORKFLOW_EVENT`) is set, which re-emits the legacy standalone `workflow` event alongside the folded fields. (#590)
- ACL updates that accompany a document version are no longer emitted as a separate `acl` event; the ACL is folded onto the `document` event's `acl` field instead. Standalone `acl` events are still emitted when an ACL is updated on its own (no new version) and on archive restore. Consumers that depended on a separate `acl` event after each create/version can restore the old behaviour with `--emit-acl-event` (`EMIT_ACL_EVENT`), which re-emits the legacy event alongside the folded field. This also fixes a minor ordering blemish: the document version event used to be emitted before the accompanying ACL event, so a consumer could observe a new version before the permissions it was created with — both are now carried by a single event. (#595)

Changes:

- Document types without an explicitly configured workflow now get an implicit workflow synthesised from their configured statuses: no checkpoint, every non-disabled status is a step. Checkpoint and step zero are also optional for explicitly configured workflows now — `SetWorkflow` no longer requires `step_zero`, `checkpoint`, or `negative_checkpoint`, with the constraint that `negative_checkpoint` may only be set when `checkpoint` is also set. (#590)
- Status rules can now reference workflow state: `StatusRuleInput` carries the current `WorkflowState` (populated from the in-flight workflow tracking at rule evaluation time), enabling rules like "only allow unpublish if previously published". `buildStatusRuleInput` also defaults `Document.Type` to the doc type when no concrete version is loaded, so rules for status updates with `version = -1` (unpublish) are no longer silently skipped. (#590)
- Dependency upgrades: Go toolchain to 1.26.3, golang base image to 1.26.4-alpine3.23, elephantine to v0.27.1, pgx to v5.10.0, urfave/cli/v3 to v3.9.0, prometheus/common to v0.68.0, the AWS SDK suite, and the `golang.org/x/{crypto,net,sys,text}` group. (#586, #589, #596)

## [v1.7.2] - 2026-05-26

- Fix the lock cleaner cutoff sign: `removeExpiredLocks` set its cutoff to `now + 5m` instead of `now - 5m`, so the cleaner swept locks with up to 5m of lease remaining. Freshly-acquired locks with ≤ 5m TTLs (e.g. elephant-collab's 5m default) were nearly always evicted on the next 5-minute cleaner tick, causing transient "document locked" / "not locked" errors for holders. RPC handlers and the lock-acquire path already filtered independently against the live expiry, so this was purely a storage-reclamation bug. (#594)

## [v1.7.1] - 2026-05-22

- Lock conflicts on the `Lock` RPC now surface the existing holder's identity via twirp error metadata (`lock_holder_sub`, `lock_app`, `lock_comment`, `lock_expires`) instead of collapsing to an opaque "locked by someone else" message. Clients compare `lock_holder_sub` against their own JWT subject to distinguish "I already hold this" from "held by someone else". The success path of `LockResponse` now also carries `Expires` (RFC3339). (#584)
- Implement lock acquisition on `Get`: the previously stubbed `Lock` field on `GetDocumentRequest` now performs a real lock acquisition with TTL validation, write-permission check, and the same conflict-metadata propagation as the standalone `Lock` RPC. A successful response carries the granted lock token and expiry in `GetDocumentResponse.Lock`. Bumps elephant-api to v0.23.0. (#584)
- Bump Go to 1.26.3 and update direct dependencies (`aws-sdk-go-v2/service/s3` to v1.101.0, golangci-lint action pinned to v2.11.4). (#585)

## [v1.7.0] - 2026-05-05

**Breaking:**

- The server no longer registers the embedded core schemas at startup. The `--no-core-schema` / `NO_CORE_SCHEMA` and `--ensure-schema` / `ENSURE_SCHEMA` flags have been removed; schema management is now expected to be handled by administrative tooling. Tests still install the embedded schemas automatically.

Changes:

- remove startup schema upgrades, adapt to revisorschemas v1.5.0 (reverse-domain naming) (#580)
- add `BulkGetDeliverableInfo` RPC for fetching deliverable info for up to 200 documents in one call (#579)

## [v1.6.3] - 2026-04-21

- bump revisor to v1.0.0 to fix variant type resolution during pruning; add test verifying that documents with variant types (e.g. `core/article#timeless`) can be pruned (#573)

## [v1.6.2] - 2026-04-21

- use `NewAPIServer` for `/version` and `/debug/bom` endpoints (#568)
- dependency upgrades

## [v1.6.1] - 2026-04-13

- add `Prune` RPC for automatic document correction (#563)

## [v1.6.0] - 2026-04-13

**Migrations:**

- `026_schema_generations.sql` — adds the `schema_generation` machinery: `schema_generation` (with `active`/`pending`/`deactivated` status enum), `schema_generation_schema`, `schema_exemplar`, `schema_generation_exemplar`, `schema_generation_event`, and `schema_generation_archiver` tables, plus a `schema_generation` column on `document_version`.

Changes:

- implement schema generations (#562)

## [v1.5.3] - 2026-04-07

- add support for extending variant types
- dependency upgrades (#561)

## [v1.5.2] - 2026-03-31

- update reporting tables
- fix nil pointer panic in `documentSet.RemoveDocument`

## [v1.5.1] - 2026-03-27

- move reporting tables to `schema/reporting_tables.json`
- resolve `GetDeliverableInfo` ambiguity for multi-planning-item deliverables (#560)

## [v1.5.0] - 2026-03-06

**Migrations:**

- `025_signing_key_archived.sql` — adds an `archived` boolean column to `signing_keys` so retired keys can be flagged once their data has been written to the archive.

Changes:

- add partial document support to the socket API (#548)
- implement support for subset expressions for partial doc fetching (#546)
- archive signing keys and batch eventlog items to S3 (#541)
- add support for connecting through PgBouncer (#543)
- make it possible to configure type variants (#542 #551)
- docstream observability (#535)

## [v1.4.0] - 2026-01-19

**Migrations:**

- `024_index_foreign_keys.sql` — drops the unused `acl_audit` table, adds a concurrent index on `planning_assignment(planning_item)` to avoid table scans on cascading deletes, drops the legacy `eventlog` publication, and removes the old `create_status` / `create_version` stored procedures. **Must be run only after v1.4.0 is deployed; running it earlier breaks creates and ACL updates.**

Changes:

- delete performance improvements (#533)
- add TLS support (#532)

## [v1.3.8] - 2026-01-12

- archiver metrics (#530)
- socket API: fix response panic, handle panics in socket session goroutines (#526)

## [v1.3.7] - 2025-12-15

- bump alpine from 3.22 to 3.23 (#519)

## [v1.3.6] - 2025-12-15

- upgrade dependencies (#522)
- websocket debug improvements (#521)

## [v1.3.5] - 2025-12-12

- bump websocket message size

## [v1.3.4] - 2025-12-12

- add a websocket `CheckOrigin` function to handle CORS (#520)

## [v1.3.3] - 2025-12-02

- access timespans and labels within `r.Extra` nil-check (#516)

## [v1.3.2] - 2025-11-26

- update darknut to v0.1.3 — fix handling of string and bool pointers when decoding documents, and add an `optional` option

## [v1.3.1] - 2025-11-20

- forward timezone to document store

## [v1.3.0] - 2025-11-19

**Migrations:**

- `022_search.sql` — drops the unused `document_link` table, adds a `document_type` config table, adds `time` (`tstzmultirange`) and `labels` (`text[]`) columns to `document` (with GIST/GIN indexes) and `document_version`, adds `timezone`/`timerange` columns to `planning_assignment`, and adds a `system_config` key/value table.
- `023_index_type.sql` — concurrent index on `document(type, language)` to support type/language listings.

Changes:

- websocket API: document timespans and labels (#498)

## [v1.2.5] - 2025-10-07

- use delete manifest as source of truth for restores (#494)

## [v1.2.4] - 2025-10-07

- update to go 1.25.1 (#486)
- handle create conflicts gracefully (#493)
- handle non-document updates on documents that don't exist (#492)
- update dependencies

## [v1.2.3] - 2025-09-23

- status overview creator (#485)

## [v1.2.2] - 2025-09-15

- expose tolerate-eventlog-gaps flag

## [v1.2.1] - 2025-09-04

- bump go version to 1.24.7

## [v1.2.0] - 2025-09-04

**Migrations:**

- `021_eventlog_position.sql` — adds `nonce` columns to `document`, `delete_record`, and `eventlog`; adds `signature` to `eventlog`; backfills the eventlog with nonces from the source rows; introduces the `eventlog_archiver` and `document_archive_counter` tables that drive the archive signature chain. Heavy migration — run during a maintenance window; safe only because no production deletes have been recreated.

Changes:

- expose migrations (#475)
- archive eventlog (#463)
- update archiving docs
- update dependencies

## [v1.1.2] - 2025-07-23

- metrics upsert (#447)

## [v1.1.1] - 2025-07-21

- fix Dockerfile inconsistencies

## [v1.1.0] - 2025-07-21

- extend schema API: list schemas without their bodies, return only changed schemas from `GetAllActive`, expose registered meta types, and fix workflow 404 handling (#446)

## [v1.0.1] - 2025-06-17

**Migrations:**

- `020_meta_doc_index.sql` — concurrent index on `document(main_doc)` to speed up meta-document lookups.

Changes:

- create index on main document reference
- add more log metadata for document updates (#444)
- update dependencies (#445)
- develop against pg 17

## [v1.0.0] - 2025-05-10

- initial 1.0 release
