# Elephant Repository

The domain language of a NewsDoc document repository: versioned documents, per-document access control, workflow statuses, a signed archive, and a real-time eventlog. This file is a glossary — it defines what the words mean. For how to build, test, and run the system, see `CLAUDE.md`.

## Documents

**Document**:
A NewsDoc — the structured content the repository versions and serves. A document is identified by a UUID, has a URI, a type, and accrues versions, statuses, and an ACL over its lifetime.
_Avoid_: record, object, asset

**NewsDoc**:
The content format every document conforms to (uuid, type, uri, language, content blocks, links, and meta). "NewsDoc" names the format; "document" names an instance of it.
_Avoid_: payload, blob

**Version**:
An immutable, sequentially numbered snapshot of a document's content and metadata. Version numbers never skip and never reorder.
_Avoid_: revision, snapshot, edit

**URI**:
A namespaced, globally unique identifier (e.g. `article/...`, `user:...`, `unit:...`). Documents are addressed by URI, and principals — users and units — are named by URI in ACLs.
_Avoid_: slug, key, id

**Type**:
A string tag (e.g. `core/article`) that declares which schema and workflow a document follows.
_Avoid_: kind, category, class

**Variant type**:
A document type derived from a declared type by a `#` suffix (e.g. `core/article#timeless`). A variant is a document type in its own right — documents, statuses and workflows key on the full name — but it is declared in a type configuration rather than in a schema, so a schema listing alone never shows one.
_Avoid_: subtype, sub-type, type flavour, specialisation

**Meta**:
The metadata sidecar carried alongside a document's content on each version — arbitrary structured data that is not the content itself.
_Avoid_: extra, props, attributes

**Meta document**:
A document whose purpose is to describe another document rather than stand alone. It points at the document it describes via that document's main document reference.
_Avoid_: sidecar document, attachment document

**Main document**:
The primary document that a meta document describes.
_Avoid_: parent document, owner document

**Meta type**:
A document type registered as usable as a meta document, optionally exclusive — at most one meta document of that type per main document. A separate registration says which main types may carry which meta types.
_Avoid_: sidecar type, annotation type

**Generation**:
The run of versions a document accrues between a create (or a recreate after a delete) and the next delete, identified by the document's nonce. Version 1 of a recreated document belongs to a different generation than the version 1 it replaced. Unqualified "generation" in this repository means a document generation; the schema kind is always spelled **schema generation**.
_Avoid_: incarnation, lifetime, run, epoch

**Nonce**:
The UUID that identifies a document generation. It travels on the eventlog as `document_nonce` and is opaque — nonces minted from v1.9.0 onwards are UUIDv7 and therefore sort in creation order, but only among themselves, so ordering by nonce is never a contract.
_Avoid_: generation id, incarnation id, epoch id

**System state**:
A marker on a document (`deleting`, `restoring`) saying the document is mid-lifecycle and outside normal service. A write refused because of one fails with the `system-lock` error code — that is the system state speaking, not a document lock.
_Avoid_: document state, phase, status

**Subset**:
An expression on a read that selects which parts of a document to return, so a client can fetch what it needs instead of the whole document.
_Avoid_: projection, field mask, partial, filter

**Attached object**:
A binary uploaded separately and attached to a document by referencing its upload ID in an update. Attached objects live in the asset bucket, are not archived, and are moved into the archive bucket only when their document is deleted.
_Avoid_: attachment, asset, file, blob

**Upload**:
The handle for getting an attached object's bytes into place: an upload ID plus a presigned, short-lived S3 PUT URL. The ID is what an update references to attach the object.
_Avoid_: file upload, transfer, staging object

## Identity & access

**User**:
An individual authenticated identity.
_Avoid_: account, login, person

**Unit**:
A collective identity — a desk or team — that permissions and status-setting can be granted to, just like a user.
_Avoid_: group, organisation, team

**ACL**:
A document's access control list: the mapping of users and units to the permissions they hold on that document.
_Avoid_: sharing, permission list, grants table

**Permission**:
A capability granted on a single document through its ACL — Read, Write, MetaWrite, or SetStatus.
_Avoid_: right, privilege

**Scope**:
A capability carried in the caller's auth token that governs which API operations they may attempt across the system. A scope authorises an operation; a permission authorises it on a specific document. Both must be satisfied.
_Avoid_: role, claim, grant

## Status & workflow

**Status**:
A named, versioned state marker set on a document (e.g. `usable`, `withheld`), decoupled from document versions — setting a status does not create a new version, and creating a version does not change a status.
_Avoid_: state, flag, label

**Status head**:
The current status for a given status name on a document — the latest in that name's sequence.
_Avoid_: current status, active status

> Known exception: a status head's `id` is the count of how many times that status has been set, not a version number. The version it points at is a separate field.

**Withheld**:
The status that schedules a document for future publishing; a scheduler promotes it when its publish time arrives.
_Avoid_: scheduled, embargoed, pending

**Cause**:
A field on a status update recording why it was set (e.g. `correction`, `fix`).
_Avoid_: reason, note

**Workflow**:
The configured state machine for a document type that governs which status updates are valid and how a document moves between steps. Workflow is configuration; status is the stored result.
_Avoid_: pipeline, lifecycle, process

**Workflow step**:
A position a document occupies within its type's workflow (e.g. draft, approved, published).
_Avoid_: stage, phase

**Step zero**:
The workflow step a document starts in, and the step it returns to when a new version is created while the document sits at a checkpoint.
_Avoid_: initial step, start state, reset step

**Checkpoint**:
The one status name in a workflow whose being set means the document has reached the workflow's goal — publication, by convention. Reaching it is what makes a subsequent new version reset the document to step zero.
_Avoid_: final step, terminal state, goal, milestone

**Negative checkpoint**:
The step a document moves to when the checkpoint status is set as an unpublish. It counts as being at a checkpoint for the purpose of resetting to step zero.
_Avoid_: unpublished step, rollback step, inverse checkpoint

**Workflow state**:
A document's current position in its workflow: the step it is at, plus the last checkpoint it reached. Workflow state is derived on write and folded onto the event that changed it; it is not a status.
_Avoid_: workflow status, progress, position

**Status rule**:
A constraint — an expression — that a status update must satisfy to be accepted.
_Avoid_: validation, policy, guard

**Access rule**:
A status rule whose violation is reported as a permission error rather than a validation error — the update is refused because the caller may not make it, not because it is malformed.
_Avoid_: permission rule, authorisation rule

## Eventlog & streaming

**Event**:
An immutable record of a single change to a document: a new version, a new status, an ACL change, a delete, or a restore.
_Avoid_: message, notification, change

**Eventlog**:
The append-only, ordered sequence of all events, consumable historically or in real time.
_Avoid_: feed, journal, audit log

**Event outbox**:
The table a mutation writes its event to inside its own transaction, drained asynchronously into the eventlog. The outbox is the staging area; the eventlog is the published record.
_Avoid_: queue, buffer, pending events

**Fan-out**:
The delivery of eventlog events to consumers — SSE, the websocket document stream, and the EventBridge forwarder — each following the log at its own position.
_Avoid_: broadcast, dispatch, publishing

**Subscription**:
A single consumer's live position on the eventlog over a websocket, with its own resume point, event-type filter and rate limit.
_Avoid_: listener, connection, channel

## Archiving & deletion

**Archive**:
The immutable, cryptographically signed store of every event, document version, and status, held in S3.
_Avoid_: backup, cold storage, snapshot store

**Signature chain**:
The chain of ECDSA signatures linking archived items into a tamper-evident merkle tree, so any later alteration is detectable.
_Avoid_: hash chain, audit chain

**Signing key**:
The ECDSA key pair the archive signs with. One key is current at a time, it is valid for a fixed window, and the public halves are published so the chain can be verified without the repository's cooperation.
_Avoid_: archive key, certificate, secret

**Batch archive**:
A zip of consecutive archived events written alongside the individual objects, at 1 000 and 10 000 event granularities, so a verifier can walk the chain without fetching every object.
_Avoid_: bundle, rollup, digest

**Delete**:
Mark a document for removal. A deleted document is archived and can be restored — it is gone from active storage but not yet destroyed.
_Avoid_: remove, trash

**Purge**:
Permanently destroy a deleted document's archived data, keeping only the deletion audit trail. A purge cannot be undone.
_Avoid_: hard delete, erase, wipe

**Restore**:
Reconstruct a deleted document — its versions, statuses, and ACL — from the archive. Possible until the document is purged.
_Avoid_: undelete, recover, rollback

**Delete record**:
The metadata that survives a document's deletion, identifying its archived objects so it can be restored or purged.
_Avoid_: tombstone, deletion log

**Delete manifest**:
The object written into the archive alongside a deleted document's moved data, listing its last version, status heads, ACL and attachments. Once the document row is gone the manifest is the only record of them, and it is the authority a restore reads from — the delete record points at it.
_Avoid_: deletion manifest, index, inventory

## Schema & validation

**Schema**:
The specification defining the valid structure of a document type.
_Avoid_: spec, model, contract

**Schema generation**:
A numbered, immutable set of schema versions activated together, enabling coordinated rollouts. One generation is active at a time.
_Avoid_: schema release, schema batch, schema set

> Always spelled in full: unqualified **generation** means a document generation.

**Exemplar**:
A sample document stored with a schema generation to illustrate valid structure for that generation.
_Avoid_: example, fixture, sample, template

**Deprecation**:
A named construct that schemas still accept but should no longer be used. A deprecation is either enforced, in which case using it fails validation, or unenforced, in which case it is counted and logged and the write succeeds.
_Avoid_: warning, lint, legacy flag

**Type configuration**:
Per-document-type configuration that decides what is derived from a document on write and what variants the type has. It is separate from the schema: a schema says what a document may contain, a type configuration says what the repository does with it.
_Avoid_: type settings, doc config, type metadata

**Time expression**:
An expression in a type configuration that extracts the timespans a document covers into the document's queryable time range.
_Avoid_: date extractor, time selector

**Label expression**:
An expression in a type configuration that extracts a document's labels — free-form tags the repository indexes for listing.
_Avoid_: tag extractor, classifier

**Bounded collection**:
A type whose full membership is enumerable, so every document of that type can be listed rather than only searched.
_Avoid_: finite type, closed set, enumerable type

**Validate**:
Check a caller-supplied document against the active schemas and report the violations, without storing anything.
_Avoid_: check, lint, verify

**Prune**:
Remove from a caller-supplied document the parts the active schemas do not allow, returning the corrected document. Prune is a dry-run correction of content and has nothing to do with **Purge**, which destroys archived data.
_Avoid_: clean, strip, trim, sanitise

## Locks

**Lock**:
A pessimistic hold on a document that blocks competing updates until it is released or its TTL expires.
_Avoid_: reservation, claim

**Lock token**:
The secret issued when a lock is acquired, required to update, extend, or release the locked document.
_Avoid_: lock key, lock id

**Lock exclusivity**:
How much of a document a lock covers — versions only, or also statuses and/or the ACL.
_Avoid_: lock scope, lock level

**Job lock**:
A cluster-wide lease held in the database that elects the single instance allowed to run a given background worker. Unqualified "lock" in this repository means a document lock; the coordination kind is always spelled **job lock**.
_Avoid_: leader election, advisory lock, mutex, singleton lock

## Metrics

**Metric**:
A custom numeric value attached to a document (e.g. a character count), distinct from operational/Prometheus metrics.
_Avoid_: measurement, stat, gauge

**Kind**:
The named class a document metric belongs to, carrying the aggregation that decides how a new measurement combines with the stored one — replacing it, or incrementing it.
_Avoid_: metric type, metric name, category

## Planning

**Deliverable**:
The link between an editorial assignment and a specific document version — the document is what fulfils the assignment.
_Avoid_: output, artifact, attachment

**Planning item**:
A document describing planned editorial work — what is to be covered, and when.
_Avoid_: plan, job, ticket, story

**Assignment**:
A unit of work within a planning item, carrying its own timespan, and the thing a deliverable fulfils.
_Avoid_: task, slot, booking
