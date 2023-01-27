-- name: InsertDocumentStatus :exec
INSERT INTO document_status(uuid, name, id, hash, created, creator_uri, meta)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: GetCurrentVersion :one
SELECT
        v.uuid, v.version, v.hash, v.title, v.type, v.language,
        v.created, v.creator_uri, v.meta, v.document_data, v.archived
FROM document AS d
     INNER JOIN document_version AS v ON
           v.uuid = d.uuid AND v.version = d.current_version
WHERE d.uuid = $1;

-- name: GetDocumentACL :many
SELECT uuid, uri, permissions FROM acl WHERE uuid = $1;

-- name: GetStatuses :many
SELECT uuid, name, id, version, hash, created, creator_uri, meta
FROM document_status
WHERE uuid = $1 AND name = $2 AND ($3 = 0 OR id < $3)
ORDER BY id DESC
LIMIT $4;


-- name: GetVersions :many
SELECT uuid, version, hash, title, type, language, created, creator_uri, meta
FROM document_status
WHERE uuid = $1 AND name = $2 AND ($3 = 0 OR id < $3)
ORDER BY id DESC
LIMIT $4;
