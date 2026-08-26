# Permissions

The per-RPC permission matrix: which scopes each method accepts, and which
per-document ACL check it applies on top. Derived by hand from the
`RequireAnyScope` and `accessCheck` calls in `repository/documents_api.go`,
`schema_api.go`, `workflow_api.go` and `metric_api.go`.

| Document | What it settles |
|---|---|
| [README](../README.md) | Orientation and the working reference: what the repository holds, how to build and run it, and what every configuration flag does. |
| [architecture.md](architecture.md) | How the service is built, including [the scope vocabulary](architecture.md#scopes) and what each scope grants. |
| [ops.md](ops.md) | The operator's-eye view, including [the security summary](ops.md#security). |
| [observability.md](observability.md) | Every metric the service exports and what a change in it means. |
| **permissions.md** (this document) | The per-RPC matrix below. |

[architecture.md](architecture.md#scopes) is the authority on what each scope
*means*; this document only says which methods accept which ones. It says
nothing about the socket API's own method set, which authorises against the
same scopes through the session's JWT.

> Nothing verifies this table against the code. It has been out of date before —
> if a method's behaviour matters, read the handler. See
> [Pending work](../README.md#pending-work).

## How a call is authorised

Three gates, in order:

1. **A valid token.** The auth middleware rejects a request with a missing or
   invalid `Authorization` header with a 401 before the handler runs. Only
   `GET /signing-keys` and `GET /websocket/:token` bypass it.
2. **The scope check.** `RequireAnyScope` requires the caller to hold at least
   one of the listed scopes; a token without a matching scope is
   `permission_denied`.
3. **The ACL check**, for document operations only. `accessCheck` requires the
   caller's subject or one of its units to hold the needed permission on the
   document.

**The ACL check has scope-level bypasses, and they are the whole access-control
story for anything holding them:**

* `doc_admin` bypasses every ACL check, for every permission.
* `doc_read_all` bypasses the check when only `Read` is needed.
* `meta_doc_write_all` bypasses the check when `MetaWrite` is needed — and only
  then. `accessCheck` explicitly re-requires `doc_write` or `doc_admin` when the
  requested permissions include a plain `Write`, so the scope cannot be used to
  create ordinary documents.

Some scopes take subscopes: `metrics_write:word_count` grants writes to that
one metric kind.

## Methods with no scope check

Only one: `Documents.Evict`, which is unimplemented and returns
`unimplemented` before looking at anything.

**Every new method still needs its own scope check.** The middleware guarantees a
valid token, not that the caller may do what they asked — it knows nothing about
which scope a method requires. An omitted `RequireAnyScope` leaves a method open
to any authenticated caller.

## Documents

| Method | Scopes (any of) | ACL check |
|---|---|---|
"Rejects" means the call fails if the ACL check fails. "Filters" means the
result set is reduced to the documents the caller may read, and the call
succeeds either way — a client cannot distinguish "does not exist" from "not
shared with me".

| Method | Scopes (any of) | ACL check |
|---|---|---|
| `Get` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects. Acquiring a lock via the `Lock` field additionally requires Write. |
| `GetMeta` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects |
| `GetHistory` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects |
| `GetStatus` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects |
| `GetStatusHistory` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects |
| `GetNilStatuses` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects |
| `GetDeliverableInfo` | `doc_read`, `doc_read_all`, `doc_admin` | Read, rejects |
| `BulkGet` | `doc_read`, `doc_read_all`, `doc_admin` | Read, filters |
| `GetMatching` | `doc_read`, `doc_read_all`, `doc_admin` | Read, filters |
| `GetStatusOverview` | `doc_read`, `doc_read_all`, `doc_admin` | Read, filters |
| `BulkGetDeliverableInfo` | `doc_read`, `doc_read_all`, `doc_admin` | Read, filters |
| `GetAttachments` | `doc_read`, `doc_read_all`, `doc_admin` | Read, filters |
| `GetSocketToken` | `doc_read`, `doc_read_all`, `doc_admin` | — Scope only; per-document checks happen on the socket. |
| `GetPermissions` | `doc_read`, `doc_read_all`, `doc_write`, `doc_delete`, `doc_admin` | — Any client with a document scope may read permissions. |
| `Update` | `doc_write`, `doc_admin`, `meta_doc_write_all` | Write, rejects — or MetaWrite for a meta document, which also checks the main document. Import directives additionally require `doc_import` or `doc_admin`. |
| `BulkUpdate` | `doc_write`, `doc_admin`, `meta_doc_write_all` | As `Update`, per document |
| `Lock` | `doc_write`, `doc_delete`, `doc_admin` | Write, rejects |
| `ExtendLock` | `doc_write`, `doc_delete`, `doc_admin` | Write, rejects |
| `Unlock` | `doc_write`, `doc_delete`, `doc_admin` | Write, rejects |
| `Delete` | `doc_delete`, `doc_admin` | Write, rejects |
| `ListDeleted` | `doc_restore`, `doc_purge`, `doc_admin` | — |
| `Restore` | `doc_restore`, `doc_admin` | **None** |
| `Purge` | `doc_purge`, `doc_admin` | **None** |
| `CreateUpload` | `asset_upload`, `doc_admin` | — |
| `GetWithheld` | `doc_admin` | — |
| `Eventlog` | `eventlog_read`, `doc_admin` | **None** |
| `CompactedEventlog` | `eventlog_read`, `doc_admin` | **None** |
| `Validate` | `doc_write`, `doc_admin`, `meta_doc_write_all` | — Nothing is read from storage; the caller supplies the document. |
| `Prune` | `doc_write`, `doc_admin`, `meta_doc_write_all` | — Nothing is read from storage; the caller supplies the document. |
| `Evict` | none | Unimplemented. |

Three of those have no per-document check at all, which is worth being explicit
about:

* **`Restore` and `Purge` are scope-only.** The document is deleted, so there is
  no ACL left to check against — `doc_restore` and `doc_purge` therefore act on
  *any* deleted document regardless of who could see it when it existed.
  `Restore` does take an ACL to apply to the restored document. Treat both
  scopes as administrative.
* **The eventlog is not ACL-filtered.** `eventlog_read` grants the whole log,
  including the UUIDs, types, statuses and ACLs of documents the caller cannot
  read. It does not include document bodies, but it is not a scoped view of the
  repository either.

## Schemas

| Method | Scopes (any of) |
|---|---|
| `Get` | `schema_read`, `schema_admin` |
| `GetAllActive` | `schema_read`, `schema_admin` |
| `ListGenerations` | `schema_read`, `schema_admin` |
| `GetExemplars` | `schema_read`, `schema_admin` |
| `GetTypeConfiguration` | `schema_admin` |
| `ConfigureType` | `schema_admin` |
| `RegisterGeneration` | `schema_admin` |
| `SetActive` | `schema_admin` |
| `RegisterMetaType` | `schema_admin` |
| `RegisterMetaTypeUse` | `schema_admin` |
| `GetDeprecations` | `schema_admin` |
| `UpdateDeprecation` | `schema_admin` |
| `GetDocumentTypes` | `schema_read`, `schema_admin` |
| `GetMetaTypes` | `schema_read`, `schema_admin` |
| `ListActive` | `schema_read`, `schema_admin` |

`GetTypeConfiguration` requires `schema_admin` rather than `schema_read`, unlike
every other read in this service.

`Validate` and `Prune` live on the `Documents` service but read the schemas, so
a client that only validates still needs a *write* scope rather than
`schema_read`. That asymmetry is deliberate — they are dry runs of the write
path — but it is easy to trip over when granting scopes.

## Workflows

| Method | Scopes (any of) |
|---|---|
| `GetStatuses` | `doc_read`, `workflow_admin` |
| `GetWorkflow` | `doc_read`, `workflow_admin` |
| `GetStatusRules` | `workflow_admin` |
| `CreateStatusRule` | `workflow_admin` |
| `DeleteStatusRule` | `workflow_admin` |
| `UpdateStatus` | `workflow_admin` |
| `SetWorkflow` | `workflow_admin` |
| `DeleteWorkflow` | `workflow_admin` |

## Metrics

Document metrics, not Prometheus metrics.

| Method | Scopes (any of) |
|---|---|
| `GetMetrics` | `metrics_read`, `metrics_admin` |
| `GetKinds` | `metrics_read`, `metrics_write`, `metrics_admin` |
| `RegisterMetric` | `metrics_write`, `metrics_write:<kind>`, `metrics_admin` |
| `RegisterKind` | `metrics_admin` |
| `DeleteKind` | `metrics_admin` |

`RegisterMetric` accepts the `metrics_write:<kind>` subscope for the kind named
in the request, so a client can be granted writes to one metric kind without
being able to write any other.

## Streaming endpoints

| Endpoint | Scopes (any of) |
|---|---|
| `GET /sse` | A valid token, then `eventlog_read` or `doc_admin`. The token may be passed as a `token` query parameter as well as a bearer header — it is copied into the header before the middleware runs. |
| `GET /websocket/:token` | A socket token from `Documents.GetSocketToken`, signed with the server's socket key. Bypasses the auth middleware; the session then authenticates with a JWT, and per-document reads honour `doc_read_all`. |
| `GET /signing-keys` | None. Public by design — it is what makes independent archive verification possible. |
