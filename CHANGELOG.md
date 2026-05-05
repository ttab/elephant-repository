# Changelog

All notable changes to this project after v1.0.0 are documented here. The
entries below are derived from release tags; see the linked PRs for full
detail.

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
