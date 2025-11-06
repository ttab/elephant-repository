-- name: GetDocumentForUpdate :one
SELECT d.uri, d.type, d.current_version, d.main_doc, d.language, d.system_state,
       d.nonce AS nonce,
       l.uuid as lock_uuid, l.uri as lock_uri, l.created as lock_created,
       l.expires as lock_expires, l.app as lock_app, l.comment as lock_comment,
       l.token as lock_token
FROM document as d
LEFT JOIN document_lock as l ON d.uuid = l.uuid AND l.expires > @now
WHERE d.uuid = $1
FOR UPDATE OF d;

-- name: GetTypeOfDocument :one
SELECT type
FROM document
WHERE uuid = @uuid;

-- name: GetDocumentHeads :many
SELECT name, current_id
FROM status_heads
WHERE uuid = $1;

-- name: GetFullDocumentHeads :many
SELECT s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
       s.archived, s.signature, s.meta_doc_version, h.language
FROM status_heads AS h
     INNER JOIN document_status AS s ON
           s.uuid = h.uuid AND s.name = h.name AND s.id = h.current_id
WHERE h.uuid = $1;

-- name: GetDocumentUnarchivedCount :one
SELECT unarchived FROM document_archive_counter
WHERE uuid = @uuid;

-- name: UpdateDocumentUnarchivedCount :one
INSERT INTO document_archive_counter AS dac (uuid, unarchived)
VALUES (@uuid, GREATEST(0, @delta::int))
ON CONFLICT (uuid) DO UPDATE
   SET unarchived = GREATEST(0, dac.unarchived + @delta::int)
RETURNING unarchived;

-- name: DeleteDocumentUnarchivedCounter :exec
DELETE FROM document_archive_counter
WHERE uuid = @uuid;

-- name: GetDocumentACL :many
SELECT uuid, uri, permissions FROM acl WHERE uuid = $1;

-- name: GetCurrentDocumentVersions :many
SELECT d.uuid, d.current_version, d.updated,
       d.creator_uri, d.updater_uri,
       w.step AS workflow_step, w.checkpoint AS workflow_checkpoint
FROM document AS d
     LEFT OUTER JOIN workflow_state AS w ON w.uuid = d.uuid
WHERE d.uuid = ANY(@uuids::uuid[]);

-- name: GetMultipleStatusHeads :many
SELECT h.uuid, h.name, h.current_id, h.updated, h.updater_uri, s.version,
       s.meta_doc_version,
       CASE WHEN @get_meta::bool THEN s.meta ELSE NULL::jsonb END AS meta
FROM status_heads AS h
     INNER JOIN document_status AS s
           ON s.uuid = h.uuid AND s.name = h.name AND s.id = h.current_id
WHERE h.uuid = ANY(@uuids::uuid[])
AND h.name = ANY(@statuses::text[]);

-- name: GetVersions :many
SELECT version, created, creator_uri, meta, archived
FROM document_version
WHERE uuid = @uuid AND (@before::bigint = 0 OR version < @before::bigint)
ORDER BY version DESC
LIMIT sqlc.arg(count)::bigint;

-- name: GetStatusesForVersions :many
SELECT uuid, name, id, version, created, creator_uri, meta, meta_doc_version
FROM document_status
WHERE uuid = @uuid AND version = ANY(@versions::bigint[])
ORDER BY version DESC, name, id DESC;

-- name: GetNilStatuses :many
SELECT uuid, name, id, version, created, creator_uri, meta, meta_doc_version
FROM document_status
WHERE uuid = @uuid
      AND (@names::text[] IS NULL OR name = ANY(@names))
      AND version = -1
ORDER BY name ASC, id DESC;

-- name: GetVersion :one
SELECT created, creator_uri, meta, archived
FROM document_version
WHERE uuid = @UUID AND version = @version;

-- name: GetFullVersion :one
SELECT created, creator_uri, meta, document_data, archived, signature
FROM document_version
WHERE uuid = @UUID AND version = @version;

-- name: GetDocumentRow :one
SELECT uuid, uri, type, created, creator_uri, updated, updater_uri,
       current_version, main_doc, language, system_state, nonce
FROM document
WHERE uuid = @uuid;

-- name: GetDocumentInfo :one
SELECT
        d.uuid, d.uri, d.created, d.creator_uri, d.updated, d.updater_uri, d.current_version,
        d.system_state, d.main_doc, d.nonce, l.uuid as lock_uuid, l.uri as lock_uri,
        l.created as lock_created, l.expires as lock_expires, l.app as lock_app,
        l.comment as lock_comment, l.token as lock_token,
        ws.step as workflow_state, ws.checkpoint as workflow_checkpoint
FROM document as d 
LEFT JOIN document_lock as l ON d.uuid = l.uuid AND l.expires > @now
LEFT JOIN workflow_state AS ws ON ws.uuid = d.uuid
WHERE d.uuid = @uuid;

-- name: BulkGetDocumentInfo :many
SELECT
        d.uuid, d.uri, d.created, d.creator_uri, d.updated, d.updater_uri, d.current_version,
        d.system_state, d.main_doc, d.nonce, l.uuid as lock_uuid, l.uri as lock_uri,
        l.created as lock_created, l.expires as lock_expires, l.app as lock_app,
        l.comment as lock_comment, l.token as lock_token,
        ws.step as workflow_state, ws.checkpoint as workflow_checkpoint
FROM document as d
LEFT JOIN document_lock as l ON d.uuid = l.uuid AND l.expires > @now
LEFT JOIN workflow_state AS ws ON ws.uuid = d.uuid
WHERE d.uuid = ANY(@uuids::uuid[]);

-- name: BulkGetFullDocumentHeads :many
SELECT s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
       s.archived, s.signature, s.meta_doc_version, h.language
FROM status_heads AS h
     INNER JOIN document_status AS s ON
           s.uuid = h.uuid AND s.name = h.name AND s.id = h.current_id
WHERE h.uuid = ANY(@uuids::uuid[]);


-- name: BulkGetAttachments :many
SELECT document, name, version FROM attached_object_current
WHERE document = ANY(@documents::uuid[])
      AND deleted = false;

-- name: BulkGetDocumentACL :many
SELECT uuid, uri, permissions
FROM acl
WHERE uuid = ANY(@uuids::uuid[]);

-- name: GetDocumentData :one
SELECT v.document_data, v.version
FROM document as d
     INNER JOIN document_version AS v ON
           v.uuid = d.uuid And v.version = d.current_version
WHERE d.uuid = $1;

-- name: GetDocumentVersionData :one
SELECT document_data
FROM document_version
WHERE uuid = $1 AND version = $2;

-- name: BulkGetDocumentData :many
WITH refs AS (
     SELECT unnest(@uuids::uuid[]) AS uuid,
            unnest(@versions::bigint[]) AS version
)
SELECT v.uuid, v.version, v.document_data
FROM refs AS r
     INNER JOIN document as d ON d.uuid = r.uuid
     INNER JOIN document_version AS v ON
           v.uuid = d.uuid AND (
                  (r.version = 0 AND v.version = d.current_version)
                  OR v.version = r.version
           );

-- name: AcquireTXLock :exec
SELECT pg_advisory_xact_lock(@id::bigint);

-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);

-- name: InsertDocument :exec
INSERT INTO document(
       uuid, uri, type,
       created, creator_uri, updated, updater_uri, current_version,
       main_doc, language, system_state, nonce
) VALUES (
       @uuid, @uri, @type,
       @created, @creator_uri, @created, @creator_uri, @version,
       @main_doc, @language, @system_state, @nonce
);

-- name: ReadForRestore :one
SELECT system_state FROM document
WHERE uuid = @uuid;

-- name: ClearSystemState :exec
UPDATE document SET system_state = NULL
WHERE uuid = @uuid AND NOT system_state IS NULL;

-- name: CheckForPendingPurge :one
SELECT EXISTS (
       SELECT 1 FROM purge_request
       WHERE delete_record_id = @delete_record_id
       AND finished IS NULL
);

-- name: InsertRestoreRequest :exec
INSERT INTO restore_request(
       uuid, delete_record_id, created, creator, spec
) VALUES(
       @uuid, @delete_record_id, @created, @creator, @spec
);

-- name: GetNextRestoreRequest :one
SELECT r.id, r.uuid, d.nonce, r.delete_record_id, r.created, r.creator, r.spec
FROM restore_request AS r
     INNER JOIN delete_record AS dr
           ON dr.id = r.delete_record_id
     INNER JOIN document AS d
           ON d.uuid = r.uuid AND d.system_state = 'restoring'
WHERE r.finished IS NULL AND dr.purged IS NULL
ORDER BY r.id ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: GetInvalidRestoreRequests :many
SELECT r.id
FROM restore_request AS r
     INNER JOIN delete_record AS dr
           ON dr.id = r.delete_record_id
WHERE r.finished IS NULL AND dr.purged IS NOT NULL
ORDER BY r.id ASC
FOR UPDATE SKIP LOCKED;

-- name: DropInvalidRestoreRequests :exec
DELETE FROM restore_request AS rr
WHERE rr.finished IS NULL
AND rr.delete_record_id = ANY(
    SELECT dr.id FROM delete_record AS dr
    WHERE dr.id = rr.delete_record_id
    AND dr.purged IS NOT NULL
);

-- name: FinishRestoreRequest :exec
UPDATE restore_request
SET finished = @finished
WHERE id = @id;

-- name: InsertPurgeRequest :exec
INSERT INTO purge_request(
       uuid, delete_record_id, created, creator
) VALUES(
       @uuid, @delete_record_id, @created, @creator
);

-- name: GetNextPurgeRequest :one
SELECT p.id, p.uuid, p.delete_record_id, p.created
FROM purge_request AS p
     INNER JOIN delete_record AS dr
           ON dr.id = p.delete_record_id
WHERE p.finished IS NULL
      AND dr.purged IS NULL
      AND dr.finalised IS NOT NULL
ORDER BY p.id ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: DropRedundantPurgeRequests :exec
DELETE FROM purge_request AS pr
WHERE pr.finished IS NULL
AND pr.delete_record_id = ANY(
    SELECT dr.id FROM delete_record AS dr
    WHERE dr.id = pr.delete_record_id
    AND dr.purged IS NOT NULL
);

-- name: PurgeDeleteRecordDetails :exec
UPDATE delete_record SET
       meta = NULL, acl = NULL, heads = NULL, version = 0,
       language = NULL, 
       purged = @purged_time
WHERE id = @id;

-- name: FinishPurgeRequest :exec
UPDATE purge_request
SET finished = @finished
WHERE id = @id;

-- name: UpsertDocument :exec
INSERT INTO document(
       uuid, uri, type,
       created, creator_uri, updated, updater_uri, current_version,
       main_doc, language, main_doc_type, nonce, time, intrinsic_time
) VALUES (
       @uuid, @uri, @type,
       @created, @creator_uri, @created, @creator_uri, @version,
       @main_doc, @language, @main_doc_type, @nonce, @time, @intrinsic_time
) ON CONFLICT (uuid) DO UPDATE
     SET uri = excluded.uri,
         updated = excluded.created,
         updater_uri = excluded.creator_uri,
         current_version = excluded.current_version,
         language = excluded.language,
         time = COALESCE(excluded.time, document.time),
         intrinsic_time = COALESCE(excluded.intrinsic_time, document.intrinsic_time);

-- name: GetIntrinsicTime :one
SELECT intrinsic_time
FROM document
WHERE uuid = @uuid;

-- name: UpdateDocumentTime :exec
UPDATE document
SET time = @time
WHERE uuid = @uuid;

-- name: CreateDocumentVersion :exec
INSERT INTO document_version(
       uuid, version,
       created, creator_uri, meta, document_data, archived, language
) VALUES (
       @uuid, @version,
       @created, @creator_uri, @meta, @document_data, false, @language
);

-- name: CreateStatusHead :exec
INSERT INTO status_heads(
       uuid, name, type, version, current_id,
       updated, updater_uri, language, system_state
) VALUES (
       @uuid, @name, @type::text, @version::bigint, @id::bigint,
       @created, @creator_uri, @language::text, @system_state
)
ON CONFLICT (uuid, name) DO UPDATE
   SET updated = @created,
       updater_uri = @creator_uri,
       current_id = @id::bigint,
       version = @version::bigint,
       language = @language::text;

-- name: InsertDocumentStatus :exec
INSERT INTO document_status(
       uuid, name, id, version, created,
       creator_uri, meta, meta_doc_version
) VALUES (
       @uuid, @name, @id, @version, @created,
       @creator_uri, @meta, @meta_doc_version::bigint
);

-- name: DeleteDocumentWorkflow :execrows
DELETE FROM workflow WHERE type = @type;

-- name: SetDocumentWorkflow :exec
INSERT INTO workflow(
       type, updated, updater_uri, configuration
) VALUES (
       @type, @updated, @updater_uri, @configuration
) ON CONFLICT(type) DO UPDATE SET
  updated = excluded.updated,
  updater_uri = excluded.updater_uri,
  configuration = excluded.configuration;

-- name: GetDocumentWorkflows :many
SELECT type, updated, updater_uri, configuration
FROM workflow;

-- name: GetDocumentWorkflow :one
SELECT type, updated, updater_uri, configuration
FROM workflow
WHERE type = @type;

-- name: ChangeWorkflowState :exec
INSERT INTO workflow_state(
       uuid, type, language, updated, updater_uri, step, checkpoint,
       status_name, status_id, document_version
) VALUES (
       @uuid, @type, @language, @updated, @updater_uri, @step,
       @checkpoint, @status_name, @status_id, @document_version
) ON CONFLICT(uuid) DO UPDATE SET
  type = excluded.type,
  language = excluded.language,
  updated = excluded.updated,
  updater_uri = excluded.updater_uri,
  step = excluded.step,
  checkpoint = excluded.checkpoint,
  status_name = excluded.status_name,
  status_id = excluded.status_id,
  document_version = excluded.document_version;

-- name: GetWorkflowState :one
SELECT uuid, type, language, updated, updater_uri, step, checkpoint,
       status_name, status_id, document_version
FROM workflow_state
WHERE uuid = @uuid;

-- name: RegisterMetaType :exec
INSERT INTO meta_type(
       meta_type, exclusive_for_meta
) VALUES (
       @meta_type, @exclusive_for_meta
) ON CONFLICT (meta_type) DO UPDATE SET
  exclusive_for_meta = @exclusive_for_meta;

-- name: RegisterMetaTypeUse :exec
INSERT INTO meta_type_use(
       main_type, meta_type
) VALUES (
       @main_type, @meta_type
);

-- name: DropMetaType :exec
DELETE FROM meta_type
WHERE meta_type = @meta_type;

-- name: GetMetaTypeUse :many
SELECT main_type, meta_type
FROM meta_type_use;

-- name: GetMetaTypesWithUse :many
SELECT m.meta_type, u.main_type
       FROM meta_type AS m
       LEFT OUTER JOIN meta_type_use AS u ON u.meta_type = m.meta_type;

-- name: CheckMetaDocumentType :one
SELECT coalesce(meta_type, ''), NOT d.main_doc IS NULL as is_meta_doc
FROM document AS d
     LEFT JOIN meta_type_use AS m ON m.main_type = d.type
WHERE d.uuid = @uuid;

-- name: GetMetaDocVersion :one
SELECT current_version FROM document
WHERE main_doc = @uuid;

-- name: DeleteDocumentEntry :exec
DELETE FROM document WHERE uuid = @uuid;

-- name: InsertDeletionPlaceholder :exec
insert into document(
       uuid, uri, type, created, creator_uri, updated, updater_uri,
       current_version, system_state, nonce
) values (
       @uuid, @uri, '', now(), '', now(), '', @record_id, 'deleting', @nonce
);

-- name: InsertDeleteRecord :one
INSERT INTO delete_record(
       uuid, uri, type, version, created, creator_uri, meta,
       main_doc, language, meta_doc_record, heads, acl, main_doc_type,
       attachments, nonce
) VALUES(
       @uuid, @uri, @type, @version, @created, @creator_uri, @meta,
       @main_doc, @language, @meta_doc_record, @heads, @acl, @main_doc_type,
       @attachments, @nonce
) RETURNING id;

-- name: ListDeleteRecords :many
SELECT id, uuid, uri, type, version, created, creator_uri, meta,
       main_doc, language, meta_doc_record, finalised, purged, attachments
FROM delete_record AS r
WHERE (sqlc.narg('uuid')::uuid IS NULL OR r.uuid = @uuid)
      AND (@before_id::bigint = 0 OR r.id < @before_id)
      AND (sqlc.narg('before_time')::timestamptz IS NULL OR r.created < @before_time)
ORDER BY r.id DESC;

-- name: GetDeleteRecordForUpdate :one
SELECT id, uuid, uri, type, version, created, creator_uri, meta,
       main_doc, language, meta_doc_record, heads, finalised, purged,
       attachments
FROM delete_record
WHERE id = @id AND uuid = @uuid
FOR UPDATE;

-- name: GetVersionLanguage :one
SELECT language FROM document_version
WHERE uuid = @uuid AND version = @version;

-- name: SetEventlogArchiver :exec
INSERT INTO eventlog_archiver(size, position, last_signature)
       VALUES (@size, @position, @signature)
ON CONFLICT (size) DO UPDATE
   SET position = @position,
       last_signature = @signature;

-- name: GetEventlogArchiver :one
SELECT position, last_signature FROM eventlog_archiver
WHERE size = @size;

-- name: GetDocumentStatusForArchiving :one
SELECT
        s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
        s.meta_doc_version, p.signature AS parent_signature, d.nonce
FROM document_status AS s
     INNER JOIN document AS d
           ON d.uuid = s.uuid
     LEFT JOIN document_status AS p
          ON p.uuid = s.uuid AND p.name = s.name AND p.id = s.id-1
WHERE s.uuid = @uuid AND s.name = @name AND s.id = @id;

-- name: GetDocumentVersionForArchiving :one
SELECT
        v.uuid, v.version, v.created, v.creator_uri, v.meta, v.document_data,
        p.signature AS parent_signature, d.main_doc, d.uri, d.type,
        v.language AS language, d.nonce
FROM document_version AS v
     LEFT JOIN document_version AS p
          ON p.uuid = v.uuid AND p.version = v.version-1
     INNER JOIN document AS d
          ON d.uuid = v.uuid
WHERE v.uuid = @uuid AND v.version = @version;

-- name: GetDocumentForDeletion :one
SELECT dr.id, dr.uuid, dr.nonce, dr.heads, dr.acl, dr.version, dr.attachments
FROM delete_record AS dr
WHERE dr.finalised IS NULL
ORDER BY dr.created
FOR UPDATE SKIP LOCKED -- locks both rows
LIMIT 1;

-- name: FinaliseDeleteRecord :exec
UPDATE delete_record SET finalised = @finalised
WHERE uuid = @uuid AND id = @id;

-- name: FinaliseDocumentDelete :execrows
DELETE FROM document
WHERE uuid = @uuid AND system_state = 'deleting';

-- name: SetEventlogItemAsArchived :exec
UPDATE eventlog
SET signature = @signature
WHERE id = @id;

-- name: SetDocumentVersionAsArchived :exec
UPDATE document_version
SET archived = true, signature = @signature::text
WHERE uuid = @uuid AND version = @version;

-- name: SetDocumentStatusAsArchived :exec
UPDATE document_status
SET archived = true, signature = @signature::text
WHERE uuid = @uuid AND name = @name AND id = @id;

-- name: GetSigningKeys :many
SELECT kid, spec FROM signing_keys;

-- name: InsertSigningKey :exec
INSERT INTO signing_keys(kid, spec) VALUES(@kid, @spec);

-- name: ACLUpdate :batchexec
INSERT INTO acl(uuid, uri, permissions)
VALUES (@uuid, @uri, @permissions::text[])
       ON CONFLICT(uuid, uri) DO UPDATE SET
          permissions = @permissions::text[];

-- name: DropACL :exec
DELETE FROM acl WHERE uuid = @uuid AND uri = @uri;

-- name: CheckPermissions :one
SELECT (acl.uri IS NOT NULL) = true AS has_access, d.system_state
FROM document AS d
     LEFT JOIN acl
          ON (acl.uuid = d.uuid OR acl.uuid = d.main_doc)
          AND acl.uri = ANY(@uri::text[])
          AND @permissions::text[] && permissions
WHERE d.uuid = @uuid;

-- name: BulkCheckPermissions :many
SELECT d.uuid
FROM document AS d
     INNER JOIN acl
          ON (acl.uuid = d.uuid OR acl.uuid = d.main_doc)
          AND acl.uri = ANY(@uri::text[])
          AND @permissions::text[] && permissions
WHERE d.uuid = ANY(@uuids::uuid[]);

-- name: SelectDocumentsInTimeRange :many
SELECT d.uuid, d.current_version, d.language
FROM document AS d
WHERE d.time && @range::tstzrange
      AND d.type = @type;

-- name: SelectBoundedDocumentsWithType :many
SELECT d.uuid, d.current_version, d.language
FROM document_type AS dt
     INNER JOIN document AS d
           ON d.type = dt.type
           AND (sqlc.narg('language')::text IS NULL OR d.language = @language)
WHERE dt.type = @type
      AND dt.bounded_collection;

-- name: GetDeliverableTimes :many
SELECT a.full_day, a.publish, a.starts, a.ends, a.start_date, a.end_date, a.timezone
FROM planning_deliverable AS d
INNER JOIN planning_assignment AS a
ON a.uuid = d.assignment
WHERE d.document = @uuid;

-- name: GetDeliverableTimeranges :many
SELECT a.timerange
FROM planning_deliverable AS d
INNER JOIN planning_assignment AS a
ON a.uuid = d.assignment
WHERE d.document = @uuid;

-- name: SetTypeConfiguration :exec
INSERT INTO document_type(type, bounded_collection, configuration)
VALUES (@type, @bounded_collection, @configuration)
ON CONFLICT (type) DO UPDATE
   SET bounded_collection = excluded.bounded_collection,
       configuration = excluded.configuration;

-- name: GetTypeConfiguration :one
SELECT type, bounded_collection, configuration
FROM document_type
WHERE type = @type;

-- name: GetTypeConfigurations :many
SELECT type, bounded_collection, configuration
FROM document_type;

-- name: InsertACLAuditEntry :exec
INSERT INTO acl_audit(
       uuid, type, updated,
       updater_uri, state, language,
       system_state
)
SELECT
       @uuid::uuid, @type, @updated::timestamptz,
       @updater_uri::text, json_agg(l), @language::text,
       @system_state
FROM (
       SELECT uri, permissions
       FROM acl
       WHERE uuid = @uuid::uuid
) AS l;

-- name: GranteesWithPermission :many
SELECT uri
FROM acl
WHERE uuid = @uuid
      AND @permission::text = ANY(permissions);

-- name: RegisterSchema :exec
INSERT INTO document_schema(name, version, spec)
VALUES (@name, @version, @spec);

-- name: ActivateSchema :exec
INSERT INTO active_schemas(name, version)
VALUES (@name, @version)
       ON CONFLICT(name) DO UPDATE SET
          version = @version;

-- name: DeactivateSchema :exec
DELETE FROM active_schemas
WHERE name = @name;

-- name: GetActiveSchema :one
SELECT s.name, s.version, s.spec
FROM active_schemas AS a
     INNER JOIN document_schema AS s
           ON s.name = a.name AND s.version = a.version
WHERE a.name = @name;

-- name: GetSchema :one
SELECT s.name, s.version, s.spec
FROM document_schema AS s
WHERE s.name = @name AND s.version = @version;

-- name: ListActiveSchemas :many
SELECT a.name, a.version
FROM active_schemas AS a;

-- name: GetActiveSchemas :many
SELECT s.name, s.version, s.spec
FROM active_schemas AS a
     INNER JOIN document_schema AS s
           ON s.name = a.name AND s.version = a.version;

-- name: GetSchemaVersions :many
SELECT a.name, a.version
FROM active_schemas AS a;

-- name: GetEnforcedDeprecations :many
SELECT label
FROM deprecation
WHERE enforced = true;

-- name: GetDeprecations :many
SELECT label, enforced
FROM deprecation
ORDER BY label;

-- name: UpdateDeprecation :exec
INSERT INTO deprecation(label, enforced)
VALUES(@label, @enforced)
ON CONFLICT(label) DO UPDATE SET
   enforced = @enforced;

-- name: GetActiveStatuses :many
SELECT type, name
FROM status
WHERE disabled = false
      AND (sqlc.narg(type)::text IS NULL OR type = @type);

-- name: UpdateStatus :exec
INSERT INTO status(type, name, disabled)
VALUES(@type, @name, @disabled)
ON CONFLICT(type, name) DO UPDATE SET
   disabled = @disabled;

-- name: GetStatusRules :many
SELECT type, name, description, access_rule, applies_to, expression
FROM status_rule;

-- name: UpdateStatusRule :exec
INSERT INTO status_rule(
       type, name, description, access_rule, applies_to, expression
) VALUES(
       @type, @name, @description, @access_rule, @applies_to, @expression
) ON CONFLICT(type, name)
  DO UPDATE SET
     description = @description, access_rule = @access_rule,
     applies_to = @applies_to, expression = @expression;

-- name: DeleteStatusRule :exec
DELETE FROM status_rule WHERE type = @type AND name = @name;

-- name: InsertDocumentLock :exec
INSERT INTO document_lock(
  uuid, token, created, expires, uri, app, comment
) VALUES(
  @uuid, @token, @created, @expires, @uri, @app, @comment
);

-- name: UpdateDocumentLock :exec
UPDATE document_lock
SET expires = @expires
WHERE uuid = @uuid;

-- name: DeleteExpiredDocumentLock :exec
DELETE FROM document_lock
WHERE uuid = ANY(@uuids::uuid[])
  AND expires < @cutoff;

-- name: GetExpiredDocumentLocks :many
SELECT d.uuid, l.expires AS lock_expires, l.app
FROM document_lock AS l
       INNER JOIN document AS d ON d.uuid = l.uuid
WHERE l.expires < @cutoff
FOR UPDATE OF d SKIP LOCKED;

-- name: DeleteDocumentLock :execrows
DELETE FROM document_lock
WHERE uuid = @uuid
  AND token = @token;  

-- name: InsertIntoEventLog :exec
INSERT INTO eventlog(
       id, event, uuid, nonce, type, timestamp, updater, version, status, status_id, acl,
       language, old_language, main_doc, system_state, workflow_state, workflow_checkpoint,
       main_doc_type, extra
) VALUES (
       @id, @event, @uuid, @nonce, @type, @timestamp, @updater, @version, @status, @status_id, @acl,
       @language, @old_language, @main_doc, @system_state, @workflow_state, @workflow_checkpoint,
       @main_doc_type, @extra
);

-- name: GetEventlog :many
SELECT id, event, uuid, timestamp, type, version, status, status_id, acl, updater,
       language, old_language, main_doc, system_state,
       workflow_state, workflow_checkpoint, main_doc_type, extra, signature, nonce
FROM eventlog
WHERE id > @after
ORDER BY id ASC
LIMIT sqlc.arg(row_limit);

-- name: GetDocumentLog :many
SELECT e.id, e.event, e.uuid, e.timestamp, e.type, e.version, e.status,
       e.status_id, e.acl, e.updater, e.language, e.old_language, e.main_doc,
       e.system_state, e.workflow_state, e.workflow_checkpoint, e.main_doc_type,
       e.extra, e.signature
FROM eventlog AS e
     LEFT OUTER JOIN document AS d
           ON e.event = 'documen'
              AND d.uuid = e.uuid
              AND e.nonce = d.nonce
              AND e.version = d.current_version
WHERE e.id > @after
      AND (d.uuid IS NOT NULL OR e.event = 'delete')
ORDER BY e.id ASC
LIMIT sqlc.arg(row_limit);

-- name: GetLastEvent :one
SELECT id, event, uuid, timestamp, updater, type, version, status, status_id, acl,
       language, old_language, main_doc, workflow_state, workflow_checkpoint, main_doc_type,
       extra, signature
FROM eventlog
ORDER BY id DESC
LIMIT 1;

-- name: GetLastEventID :one
SELECT id FROM eventlog
ORDER BY id DESC LIMIT 1;

-- name: GetCompactedEventlog :many
SELECT
        w.id, w.event, w.uuid, w.timestamp, w.type, w.version, w.status,
        w.status_id, w.acl, w.updater, w.language, w.old_language, w.main_doc,
        w.system_state, workflow_state, workflow_checkpoint, main_doc_type,
        extra, signature, nonce
FROM (
     SELECT DISTINCT ON (
            e.uuid,
            CASE WHEN e.event = 'delete_document' THEN null ELSE 0 END,
            CASE WHEN NOT e.old_language IS NULL THEN null ELSE 0 END
       ) * FROM eventlog AS e
     WHERE e.id > @after AND e.id <= @until
     AND (sqlc.narg(type)::text IS NULL OR e.type = @type)
     ORDER BY
           e.uuid,
           CASE WHEN e.event = 'delete_document' THEN null ELSE 0 END,
           CASE WHEN NOT e.old_language IS NULL THEN null ELSE 0 END,
           e.id DESC
     ) AS w
ORDER BY w.id ASC
LIMIT sqlc.narg(row_limit) OFFSET sqlc.arg(row_offset);

-- name: ConfigureEventsink :exec
INSERT INTO eventsink(name, configuration) VALUES(@name, @config)
ON CONFLICT (name) DO UPDATE SET
   configuration = @config;

-- name: UpdateEventsinkPosition :exec
UPDATE eventsink SET position = @position WHERE name = @name;

-- name: GetEventsinkPosition :one
SELECT position FROM eventsink WHERE name = @name;

-- name: GetJobLock :one
SELECT holder, touched, iteration
FROM job_lock
WHERE name = $1
FOR UPDATE;

-- name: InsertJobLock :one
INSERT INTO job_lock(name, holder, touched, iteration)
VALUES (@name, @holder, now(), 1)
RETURNING iteration;

-- name: PingJobLock :execrows
UPDATE job_lock
SET touched = now(),
    iteration = iteration + 1
WHERE name = @name
      AND holder = @holder
      AND iteration = @iteration;

-- name: StealJobLock :execrows
UPDATE job_lock
SET holder = @new_holder,
    touched = now(),
    iteration = iteration + 1
WHERE name = @name
      AND holder = @previous_holder
      AND iteration = @iteration;

-- name: ReleaseJobLock :execrows
DELETE FROM job_lock
WHERE name = @name
      AND holder = @holder;

-- name: GetStatusVersions :many
SELECT id, version, created, creator_uri, meta, meta_doc_version
FROM document_status
WHERE uuid = @uuid AND name = @name
      AND (@before::bigint = 0 OR id < @before::bigint)
ORDER BY id DESC
LIMIT @count;

-- name: GetStatus :one
SELECT id, version, created, creator_uri, meta, meta_doc_version
FROM document_status
WHERE uuid = @uuid AND name = @name
      AND (@id::bigint = 0 OR id = @id::bigint)
ORDER BY id DESC
LIMIT 1;

-- name: RegisterMetricKind :exec
INSERT INTO metric_kind(name, aggregation)
VALUES (@name, @aggregation)
ON CONFLICT (name) DO UPDATE
   SET aggregation = excluded.aggregation;

-- name: DeleteMetricKind :exec
DELETE FROM metric_kind
WHERE name = @name;

-- name: GetMetricKind :one
SELECT name, aggregation
FROM metric_kind 
WHERE name = @name;

-- name: GetMetricKinds :many
SELECT name, aggregation
FROM metric_kind 
ORDER BY name;

-- name: GetMetrics :many
SELECT uuid, kind, label, value
FROM metric
WHERE uuid = ANY(@uuid::uuid[])
      AND (@kind::text[] IS NULL OR kind = ANY(@kind::text[]));

-- name: RegisterOrReplaceMetric :exec
INSERT INTO metric(uuid, kind, label, value)
VALUES (@uuid, @kind, @label, @value)
ON CONFLICT ON CONSTRAINT metric_pkey DO UPDATE 
SET value = @value;

-- name: RegisterOrIncrementMetric :exec
INSERT INTO metric(uuid, kind, label, value)
VALUES (@uuid, @kind, @label, @value)
ON CONFLICT ON CONSTRAINT metric_pkey DO UPDATE 
SET value = metric.value + @value;

-- name: GetPlanningItem :one
SELECT
        uuid, version, title, description, public, tentative,
        start_date, end_date, priority, event
FROM planning_item
WHERE uuid = @uuid;

-- name: SetPlanningItem :exec
INSERT INTO planning_item(
        uuid, version, title, description, tentative,
        start_date, end_date, priority, event
) VALUES (
        @uuid, @version, @title, @description, @tentative,
        @start_date, @end_date, @priority, @event
)
ON CONFLICT ON CONSTRAINT planning_item_pkey DO UPDATE
SET
   version = @version, title = @title, description = @description,
   public = @public, tentative = @tentative, start_date = @start_date,
   end_date = @end_date, priority = @priority, event = @event;

-- name: SetPlanningItemDeliverable :exec
INSERT INTO planning_deliverable(
       assignment, document, version
) VALUES(
       @assignment, @document, @version
)
ON CONFLICT ON CONSTRAINT planning_deliverable_pkey DO UPDATE
SET
   version = @version;

-- name: GetPlanningAssignment :one
SELECT uuid, version, planning_item, status, publish, publish_slot,
       starts, ends, start_date, end_date, full_day, public, kind, description,
       timezone, timerange
FROM planning_assignment
WHERE uuid = @uuid;

-- name: SetPlanningAssignment :exec
INSERT INTO planning_assignment(
       uuid, version, planning_item, status, publish, publish_slot,
       starts, ends, start_date, end_date, full_day, public, kind, description,
       timezone, timerange
) VALUES (
       @uuid, @version, @planning_item, @status, @publish, @publish_slot,
       @starts, @ends, @start_date, @end_date, @full_day, @public, @kind,
       @description, @timezone, @timerange
)
ON CONFLICT ON CONSTRAINT planning_assignment_pkey DO UPDATE
SET
   version = excluded.version,
   planning_item = excluded.planning_item,
   status = excluded.status,
   publish = excluded.publish,
   publish_slot = excluded.publish_slot,
   starts = excluded.starts,
   ends = excluded.ends,
   start_date = excluded.start_date,
   end_date = excluded.end_date,
   full_day = excluded.full_day,
   public = excluded.public,
   kind = excluded.kind,
   description = excluded.description,
   timezone = excluded.timezone,
   timerange = excluded.timerange;

-- name: GetPlanningAssignments :many
SELECT uuid, version, planning_item, status, publish, publish_slot,
       starts, ends, start_date, end_date, full_day, public, kind, description
FROM planning_assignment
WHERE planning_item = @planning_item;

-- name: SetPlanningAssignee :exec
INSERT INTO planning_assignee(
       assignment, assignee, version, role
) VALUES (
       @assignment, @assignee, @version, @role
)
ON CONFLICT ON CONSTRAINT planning_assignee_pkey DO UPDATE
SET version = @version, role = @role;

-- name: DeletePlanningItem :exec
DELETE FROM planning_item WHERE uuid = @uuid;

-- name: CleanUpAssignments :exec
DELETE FROM planning_assignment
WHERE planning_item = @planning_item AND version != @version;

-- name: CleanUpAssignees :exec
DELETE FROM planning_assignee
WHERE assignment = @assignment AND version != @version;

-- name: CleanUpDeliverables :exec
DELETE FROM planning_deliverable
WHERE assignment = @assignment AND version != @version;

-- name: GetDeliverableInfo :one
SELECT 
       pa.planning_item AS planning_uuid,
       pd.assignment AS assignment_uuid,
       pi.event AS event_uuid
FROM planning_deliverable pd
     JOIN planning_assignment pa
          ON pd.assignment = pa.uuid
     JOIN planning_item pi
          ON pa.planning_item = pi.uuid
WHERE pd.document = @uuid;

-- name: CreateUpload :exec
INSERT INTO upload(id, created_at, created_by, meta)
VALUES (@id, @created_at, @created_by, @meta);

-- name: GetUpload :one
SELECT id, created_at, created_by, meta
FROM upload WHERE id = @id;

-- name: GetAttachedObject :one
SELECT
        o.document,
        o.name,
        o.version,
        o.object_version,
        o.attached_at,
        o.created_by,
        o.created_at,
        o.meta,
        c.deleted
FROM attached_object_current AS c
     INNER JOIN attached_object AS o ON
           o.document = c.document
           AND o.name = c.name
           AND o.version = c.version
WHERE c.document = @document
      AND c.name = @name;

-- name: AddAttachedObject :exec
INSERT INTO attached_object(
       document, name, version, object_version, attached_at,
       created_by, created_at, meta
) VALUES (
       @document, @name, @version, @object_version, @attached_at,
       @created_by, @created_at, @meta
);

-- name: ChangeAttachedObjectVersion :exec
UPDATE attached_object SET object_version = @object_version
WHERE document = @document
      AND name = @name
      AND version = @version;

-- name: SetCurrentAttachedObject :exec
INSERT INTO attached_object_current(
       document, name, version, deleted
) VALUES (
       @document, @name, @version, @deleted
) ON CONFLICT (document, name) DO UPDATE SET
       version = excluded.version,
       deleted = excluded.deleted;

-- name: GetAttachments :many
SELECT name, version FROM attached_object_current
WHERE document = @document
      AND deleted = false;

-- name: GetDocumentAttachmentDetails :many
SELECT
        o.document,
        o.name,
        o.version,
        o.object_version,
        o.attached_at,
        o.created_by,
        o.created_at,
        o.meta
FROM attached_object_current AS c
     INNER JOIN attached_object AS o ON
           o.document = c.document
           AND o.name = c.name
           AND o.version = c.version
WHERE c.document = @document
      AND c.deleted = false;

-- name: GetAttachmentsForDocuments :many
SELECT
        o.document,
        o.name,
        o.version,
        o.meta
FROM attached_object_current AS c
     INNER JOIN attached_object AS o ON
           o.document = c.document
           AND o.name = c.name
           AND o.version = c.version
WHERE c.document = ANY(@documents::uuid[])
      AND c.name = @name
      AND c.deleted = false;

-- name: GetScheduled :many
SELECT
        ws.uuid,
        ws.type,
        s.id AS status_id,
        s.version AS document_version,
        pa.uuid AS assignment,
        pa.planning_item,
        pa.publish AS publish,
        s.creator_uri
FROM workflow_state AS ws
     INNER JOIN planning_deliverable AS pd
           ON pd.document = ws.uuid
     INNER JOIN planning_assignment AS pa
           ON pa.uuid = pd.assignment
     INNER JOIN status_heads AS sh
           ON sh.uuid = ws.uuid
           AND sh.name = 'withheld'
     INNER JOIN document_status AS s
           ON s.uuid = sh.uuid
           AND s.name = sh.name
           AND s.id = sh.current_id
WHERE ws.step = 'withheld'
      AND (
          @not_source::text[] IS NULL
          OR s.meta->>'source' IS NULL
          OR s.meta->>'source' != ANY(@not_source)
      )
      AND pa.publish > @after
ORDER BY pa.publish ASC
LIMIT 10;

-- name: GetDelayedScheduled :many
SELECT
        ws.uuid,
        ws.type,
        s.id AS status_id,
        s.version AS document_version,
        pa.uuid AS assignment,
        pa.planning_item,
        pa.publish AS publish,
        s.creator_uri
FROM workflow_state AS ws
     INNER JOIN planning_deliverable AS pd
           ON pd.document = ws.uuid
     INNER JOIN planning_assignment AS pa
           ON pa.uuid = pd.assignment
     INNER JOIN status_heads AS sh
           ON sh.uuid = ws.uuid
           AND sh.name = 'withheld'
     INNER JOIN document_status AS s
           ON s.uuid = sh.uuid
           AND s.name = sh.name
           AND s.id = sh.current_id
WHERE ws.step = 'withheld'
      AND (
          @not_source::text[] IS NULL
          OR s.meta->>'source' IS NULL
          OR s.meta->>'source' != ANY(@not_source)
      )
      AND pa.publish < @before
      AND pa.publish > @cutoff;

-- name: GetDelayedScheduledCount :one
SELECT COUNT(*)
FROM workflow_state AS ws
     INNER JOIN planning_deliverable AS pd
           ON pd.document = ws.uuid
     INNER JOIN planning_assignment AS pa
           ON pa.uuid = pd.assignment
     INNER JOIN status_heads AS sh
           ON sh.uuid = ws.uuid
           AND sh.name = 'withheld'
     INNER JOIN document_status AS s
           ON s.uuid = sh.uuid
           AND s.name = sh.name
           AND s.id = sh.current_id
WHERE ws.step = 'withheld'
      AND (
          @not_source::text[] IS NULL
          OR s.meta->>'source' IS NULL
          OR s.meta->>'source' != ANY(@not_source)
      )
      AND pa.publish < @before
      AND pa.publish > @cutoff;

-- name: AddEventToOutbox :one
INSERT INTO event_outbox_item(event)
VALUES(@event)
RETURNING id;

-- name: DeleteOutboxEvent :exec
DELETE FROM event_outbox_item WHERE id = @id;

-- name: ReadEventOutbox :many
SELECT id, event FROM event_outbox_item
ORDER BY id ASC
LIMIT sqlc.arg(count)::bigint;

-- name: LockConfigTable :exec
LOCK TABLE system_config IN ACCESS EXCLUSIVE MODE;

-- name: SetSystemConfig :exec
INSERT INTO system_config(name, value)
       VALUES (@name, @value)
ON CONFLICT (name) DO UPDATE SET
   value = excluded.value;

-- name: GetSystemConfig :one
SELECT value FROM system_config WHERE name = @name;
