-- name: GetDocumentForUpdate :one
SELECT uri, current_version FROM document
WHERE uuid = $1
FOR UPDATE;

-- name: GetDocumentHeads :many
SELECT name, id
FROM status_heads
WHERE uuid = $1;

-- name: GetDocumentACL :many
SELECT uuid, uri, permissions FROM acl WHERE uuid = $1;

-- name: GetStatuses :many
SELECT uuid, name, id, version, created, creator_uri, meta
FROM document_status
WHERE uuid = $1 AND name = $2 AND ($3 = 0 OR id < $3)
ORDER BY id DESC
LIMIT $4;

-- name: GetVersions :many
SELECT uuid, name, id, version, created, creator_uri, meta
FROM document_status
WHERE uuid = $1 AND name = $2 AND ($3 = 0 OR id < $3)
ORDER BY id DESC
LIMIT $4;

-- name: AcquireTXLock :exec
SELECT pg_advisory_xact_lock(@id::bigint);

-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);

-- name: CreateVersion :exec
select create_version(
       @uuid::uuid, @version::bigint, @created::timestamptz,
       @creator_uri::text, @meta::jsonb, @document_data::jsonb
);

-- name: CreateStatus :exec
select create_status(
       @uuid::uuid, @name::varchar(32), @id::bigint, @version::bigint,
       @created::timestamptz, @creator_uri::text, @meta::jsonb
);
