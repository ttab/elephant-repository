-- name: GetDocumentForUpdate :one
SELECT uri, current_version, deleting FROM document
WHERE uuid = $1
FOR UPDATE;

-- name: GetDocumentHeads :many
SELECT name, id
FROM status_heads
WHERE uuid = $1;

-- name: GetFullDocumentHeads :many
SELECT s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
       s.archived, s.signature
FROM status_heads AS h
     INNER JOIN document_status AS s ON
           s.uuid = h.uuid AND s.name = h.name AND s.id = h.id
WHERE h.uuid = $1;

-- name: GetDocumentUnarchivedCount :one
SELECT SUM(num) FROM (
       SELECT COUNT(*) as num
              FROM document_status AS s
              WHERE s.uuid = @uuid AND s.archived = false
       UNION
       SELECT COUNT(*) as num
              FROM document_version AS v
              WHERE v.uuid = @uuid AND v.archived = false
) AS unarchived;

-- name: GetDocumentACL :many
SELECT uuid, uri, permissions FROM acl WHERE uuid = $1;

-- name: GetStatuses :many
SELECT uuid, name, id, version, created, creator_uri, meta
FROM document_status
WHERE uuid = $1 AND name = $2 AND ($3 = 0 OR id < $3)
ORDER BY id DESC
LIMIT $4;

-- name: GetVersions :many
SELECT version, created, creator_uri, meta, archived
FROM document_version
WHERE uuid = @uuid AND (@before::bigint = 0 OR version < @before::bigint)
ORDER BY version DESC
LIMIT @count;

-- name: GetVersion :one
SELECT created, creator_uri, meta, archived
FROM document_version
WHERE uuid = @UUID AND version = @version;

-- name: GetDocumentInfo :one
SELECT
        uuid, uri, created, creator_uri, updated, updater_uri, current_version,
        deleting
FROM document
WHERE uuid = $1;

-- name: GetDocumentData :one
SELECT v.document_data
FROM document as d
     INNER JOIN document_version AS v ON
           v.uuid = d.uuid And v.version = d.current_version
WHERE d.uuid = $1;

-- name: GetDocumentVersionData :one
SELECT document_data
FROM document_version
WHERE uuid = $1 AND version = $2;

-- name: AcquireTXLock :exec
SELECT pg_advisory_xact_lock(@id::bigint);

-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);

-- name: CreateVersion :exec
SELECT create_version(
       @uuid::uuid, @version::bigint, @created::timestamptz,
       @creator_uri::text, @meta::jsonb, @document_data::jsonb
);

-- name: CreateStatus :exec
SELECT create_status(
       @uuid::uuid, @name::varchar(32), @id::bigint, @version::bigint,
       @created::timestamptz, @creator_uri::text, @meta::jsonb
);

-- name: DeleteDocument :exec
SELECT delete_document(
       @uuid::uuid, @uri::text, @record_id::bigint
);

-- name: InsertDeleteRecord :one
INSERT INTO delete_record(
       uuid, uri, version, created, creator_uri, meta
) VALUES(
       @uuid, @uri, @version, @created, @creator_uri, @meta
) RETURNING id;

-- name: GetDocumentStatusForArchiving :one
SELECT
        s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
        p.signature AS parent_signature, v.signature AS version_signature
FROM document_status AS s
     INNER JOIN document_version AS v
           ON v.uuid = s.uuid
              AND v.version = s.version
              AND v.signature IS NOT NULL
     LEFT JOIN document_status AS p
          ON p.uuid = s.uuid AND p.name = s.name AND p.id = s.id-1
WHERE s.archived = false
AND (s.id = 1 OR p.archived = true)
ORDER BY s.created
FOR UPDATE OF s SKIP LOCKED
LIMIT 1;

-- name: GetDocumentVersionForArchiving :one
SELECT
        v.uuid, v.version, v.created, v.creator_uri, v.meta, v.document_data,
        p.signature AS parent_signature
FROM document_version AS v
     LEFT JOIN document_version AS p
          ON p.uuid = v.uuid AND p.version = v.version-1
WHERE v.archived = false
AND (v.version = 1 OR p.archived = true)
ORDER BY v.created
FOR UPDATE OF v SKIP LOCKED
LIMIT 1;

-- name: GetDocumentForDeletion :one
SELECT uuid, current_version AS delete_record_id FROM document
WHERE deleting = true
ORDER BY created
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: FinaliseDelete :execrows
DELETE FROM document
WHERE uuid = @uuid AND deleting = true;

-- name: SetDocumentVersionAsArchived :exec
UPDATE document_version
SET archived = true, signature = @signature::text
WHERE uuid = @uuid AND version = @version;

-- name: SetDocumentStatusAsArchived :exec
UPDATE document_status
SET archived = true, signature = @signature::text
WHERE uuid = @uuid AND id = @id;

-- name: GetSigningKeys :many
SELECT kid, spec FROM signing_keys;

-- name: InsertSigningKey :exec
INSERT INTO signing_keys(kid, spec) VALUES(@kid, @spec);
