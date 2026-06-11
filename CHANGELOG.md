# Changelog

All notable changes to this project after v1.0.0 are documented here. The
entries below are derived from release tags; see the linked PRs for full
detail.

## [v1.9.0] - Unreleased

**Behaviour change (document locks):** a document lock now only blocks
document updates (new versions, attached objects, deletes) by default; status
and ACL updates on a locked document are no longer blocked unless the lock was
acquired with a higher exclusivity level. This matches the long-documented API
behaviour, but consumers that relied on locks also blocking status or ACL
updates must now acquire their locks with a matching exclusivity. (#604)

**Migrations:**

- `027_lock_exclusivity.sql` — adds an `exclusivity` column to `document_lock` (`text`, not null, default `'document'`). **Must be applied before deploying v1.9.0**: the lock queries in v1.9.0 reference the new column, so acquiring, reading, or checking document locks fails against an unmigrated database. The migration is a plain `alter table add column` with a default on a small, short-lived table, so no maintenance window is needed.

Changes:

- Document locks can be acquired with an exclusivity level via the new `exclusivity` field on `LockRequest` and on lock-on-Get (`AcquireLock`): `LOCK_DOCUMENT` (default, blocks document updates only), `LOCK_STATUS` (also blocks status updates), `LOCK_ACL` (also blocks ACL updates), or `LOCK_EXCLUSIVE` (blocks both). The level is exposed in `DocumentMeta.lock` and on lock conflicts via the `lock_exclusivity` error metadata key. Supplying a non-matching lock token is still rejected outright, regardless of exclusivity. (#604)
- Eventlog websocket subscriptions can now filter by event type via the new `GetEventlog.events` field, validated against the known event types. (#597)
- The document stream replay buffer is now slice-backed and configurable with `--eventlog-buffer-size` (`EVENTLOG_BUFFER_SIZE`, default 500). Resuming out of bounds still returns `eventlog_resume_oob`. (#597)
- Each subscription's live stream is now rate limited with a token bucket (`--eventlog-stream-burst` 70, `--eventlog-stream-rate` 10/s). On exceed, the events that fit are emitted followed by a `rate_limited` error, and the subscription is stopped; clients are expected to resubscribe. The initial resume replay is exempt. (#597)
- Dependency upgrades: elephant-api to v0.24.0, the AWS SDK suite, urfave/cli/v3 to v3.9.1, and the Go toolchain. (#597, #604)

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
