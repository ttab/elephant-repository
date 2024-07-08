-- name: GetDocumentForUpdate :one
SELECT d.uri, d.type, d.current_version, d.main_doc, d.language, d.system_state,
       l.uuid as lock_uuid, l.uri as lock_uri, l.created as lock_created,
       l.expires as lock_expires, l.app as lock_app, l.comment as lock_comment,
       l.token as lock_token
FROM document as d
LEFT JOIN document_lock as l ON d.uuid = l.uuid AND l.expires > @now
WHERE d.uuid = $1
FOR UPDATE OF d;

-- name: GetDocumentHeads :many
SELECT name, current_id
FROM status_heads
WHERE uuid = $1;

-- name: GetFullDocumentHeads :many
SELECT s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
       s.archived, s.signature, s.meta_doc_version
FROM status_heads AS h
     INNER JOIN document_status AS s ON
           s.uuid = h.uuid AND s.name = h.name AND s.id = h.current_id
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

-- name: GetCurrentDocumentVersions :many
SELECT uuid, current_version, updated
FROM document
WHERE uuid = ANY(@uuids::uuid[]);

-- name: GetMultipleStatusHeads :many
SELECT h.uuid, h.name, h.current_id, h.updated, h.updater_uri, s.version,
       s.meta_doc_version,
       CASE WHEN @get_meta::bool THEN s.meta ELSE NULL::jsonb END AS meta
FROM status_heads AS h
     INNER JOIN document_status AS s
           ON s.uuid = h.uuid AND s.name = h.name AND s.id = h.current_id
WHERE h.uuid = ANY(@uuids::uuid[])
AND h.name = ANY(@statuses::text[]);

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

-- name: GetFullVersion :one
SELECT created, creator_uri, meta, document_data, archived, signature
FROM document_version
WHERE uuid = @UUID AND version = @version;

-- name: GetDocumentRow :one
SELECT uuid, uri, type, created, creator_uri, updated, updater_uri,
       current_version, main_doc, language, system_state
FROM document
WHERE uuid = @uuid;

-- name: GetDocumentInfo :one
SELECT
        d.uuid, d.uri, d.created, creator_uri, updated, updater_uri, current_version,
        system_state, main_doc, l.uuid as lock_uuid, l.uri as lock_uri,
        l.created as lock_created, l.expires as lock_expires, l.app as lock_app,
        l.comment as lock_comment, l.token as lock_token
FROM document as d 
LEFT JOIN document_lock as l ON d.uuid = l.uuid AND l.expires > @now
WHERE d.uuid = @uuid;

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

-- name: AcquireTXLock :exec
SELECT pg_advisory_xact_lock(@id::bigint);

-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);

-- name: InsertDocument :exec
INSERT INTO document(
       uuid, uri, type,
       created, creator_uri, updated, updater_uri, current_version,
       main_doc, language, system_state
) VALUES (
       @uuid, @uri, @type,
       @created, @creator_uri, @created, @creator_uri, @version,
       @main_doc, @language, @system_state
);

-- name: ReadForRestore :one
SELECT system_state FROM document
WHERE uuid = @uuid;

-- name: ClearSystemState :exec
UPDATE document SET system_state = NULL
WHERE uuid = @uuid AND NOT system_state IS NULL;

-- name: InsertRestoreRequest :exec
INSERT INTO restore_request(
       uuid, delete_record_id, created, creator, spec
) VALUES(
       @uuid, @delete_record_id, @created, @creator, @spec
);

-- name: GetNextRestoreRequest :one
SELECT id, uuid, delete_record_id, created, creator, spec
FROM restore_request
WHERE finished IS NULL
ORDER BY id ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: FinishRestoreRequest :exec
UPDATE restore_request
SET finished = @finished
WHERE id = @id;

-- name: UpsertDocument :exec
INSERT INTO document(
       uuid, uri, type,
       created, creator_uri, updated, updater_uri, current_version,
       main_doc, language
) VALUES (
       @uuid, @uri, @type,
       @created, @creator_uri, @created, @creator_uri, @version,
       @main_doc, @language
) ON CONFLICT (uuid) DO UPDATE
     SET uri = @uri,
         updated = @created,
         updater_uri = @creator_uri,
         current_version = @version,
         language = @language;

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
       current_version, system_state
) values (
       @uuid, @uri, '', now(), '', now(), '', @record_id, 'deleting'
);

-- name: InsertDeleteRecord :one
INSERT INTO delete_record(
       uuid, uri, type, version, created, creator_uri, meta,
       main_doc, language, meta_doc_record, heads, acl, current_version
) VALUES(
       @uuid, @uri, @type, @version, @created, @creator_uri, @meta,
       @main_doc, @language, @meta_doc_record, @heads, @acl, @current_version
) RETURNING id;

-- name: ListDeleteRecords :many
SELECT id, uuid, uri, type, version, created, creator_uri, meta,
       main_doc, language, meta_doc_record
FROM delete_record AS r
WHERE (sqlc.narg('uuid')::uuid IS NULL OR r.uuid = @uuid)
      AND (@before_id::bigint = 0 OR r.id < @before_id)
      AND (sqlc.narg('before_time')::timestamptz IS NULL OR r.created < @before_time)
ORDER BY r.id DESC;

-- name: GetDeleteRecord :one
SELECT id, uuid, uri, type, version, created, creator_uri, meta,
       main_doc, language, meta_doc_record, heads
FROM delete_record
WHERE id = @id AND uuid = @uuid;

-- name: GetVersionLanguage :one
SELECT language FROM document_version
WHERE uuid = @uuid AND version = @version;

-- name: GetDocumentStatusForArchiving :one
SELECT
        s.uuid, s.name, s.id, s.version, s.created, s.creator_uri, s.meta,
        p.signature AS parent_signature, v.signature AS version_signature,
        d.type, v.language, s.meta_doc_version
FROM document_status AS s
     INNER JOIN document AS d
           ON d.uuid = s.uuid
     LEFT JOIN document_version AS v
           ON v.uuid = s.uuid
              AND v.version = s.version
     LEFT JOIN document_status AS p
          ON p.uuid = s.uuid AND p.name = s.name AND p.id = s.id-1
WHERE s.archived = false
-- Any parent has to have been archived.
AND (s.id = 1 OR p.archived = true)
-- Accept null signature for the referenced version if we have a nil version.
AND (s.version = -1 OR v.signature IS NOT NULL)
ORDER BY s.created
FOR UPDATE OF s SKIP LOCKED
LIMIT 1;

-- name: GetDocumentVersionForArchiving :one
SELECT
        v.uuid, v.version, v.created, v.creator_uri, v.meta, v.document_data,
        p.signature AS parent_signature, d.main_doc, d.uri, d.type,
        (v.document_data->>'language')::text AS language
FROM document_version AS v
     LEFT JOIN document_version AS p
          ON p.uuid = v.uuid AND p.version = v.version-1
     INNER JOIN document AS d
          ON d.uuid = v.uuid
WHERE v.archived = false
AND (v.version = 1 OR p.archived = true)
ORDER BY v.created
FOR UPDATE OF v SKIP LOCKED
LIMIT 1;

-- name: GetDocumentForDeletion :one
SELECT dr.id, dr.uuid, dr.heads, dr.acl, dr.current_version
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
SELECT name
FROM status
WHERE disabled = false;

-- name: UpdateStatus :exec
INSERT INTO status(name, disabled)
VALUES(@name, @disabled)
ON CONFLICT(name) DO UPDATE SET
   disabled = @disabled;

-- name: GetStatusRules :many
SELECT name, description, access_rule, applies_to, for_types, expression
FROM status_rule;

-- name: UpdateStatusRule :exec
INSERT INTO status_rule(
       name, description, access_rule, applies_to, for_types, expression
) VALUES(
       @name, @description, @access_rule, @applies_to, @for_types, @expression
) ON CONFLICT(name)
  DO UPDATE SET
     description = @description, access_rule = @access_rule,
     applies_to = @applies_to, for_types = @for_types, expression = @expression;

-- name: DeleteStatusRule :exec
DELETE FROM status_rule WHERE name = $1;

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

-- name: UpdateReport :exec
INSERT INTO report(
       name, enabled, next_execution, spec
) VALUES (
       @name, @enabled, @next_execution, @spec
) ON CONFLICT (name) DO UPDATE SET
  enabled = @enabled,
  next_execution = @next_execution,
  spec = @spec;

-- name: ListReports :many
SELECT name, spec
FROM report
ORDER BY name;

-- name: GetReport :one
SELECT name, enabled, next_execution, spec
FROM report
WHERE name = $1;

-- name: GetDueReport :one
SELECT name, enabled, next_execution, spec
FROM report
WHERE enabled AND next_execution < now()
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- name: DeleteReport :exec
DELETE FROM report
WHERE name = @name;

-- name: SetNextReportExecution :exec
UPDATE report
SET next_execution = @next_execution
WHERE name = @name;

-- name: GetNextReportDueTime :one
SELECT MIN(next_execution)::timestamptz
FROM report
WHERE enabled;

-- name: InsertIntoEventLog :one
INSERT INTO eventlog(
       event, uuid, type, timestamp, updater, version, status, status_id, acl,
       language, old_language, main_doc, system_state
) VALUES (
       @event, @uuid, @type, @timestamp, @updater, @version, @status, @status_id, @acl,
       @language, @old_language, @main_doc, @system_state
) RETURNING id;

-- name: GetEventlog :many
SELECT id, event, uuid, timestamp, type, version, status, status_id, acl, updater,
       language, old_language, main_doc, system_state
FROM eventlog
WHERE id > @after
ORDER BY id ASC
LIMIT sqlc.arg(row_limit);

-- name: GetLastEvent :one
SELECT id, event, uuid, timestamp, updater, type, version, status, status_id, acl,
       language, old_language, main_doc
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
        w.system_state
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
SELECT id, version, created, creator_uri, meta
FROM document_status
WHERE uuid = @uuid AND name = @name
      AND (@before::bigint = 0 OR id < @before::bigint)
ORDER BY id DESC
LIMIT @count;

-- name: RegisterMetricKind :exec
INSERT INTO metric_kind(name, aggregation)
VALUES (@name, @aggregation);

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
        uuid, version, title, description, public, tentative,
        start_date, end_date, priority, event
) VALUES (
        @uuid, @version, @title, @description, @public, @tentative,
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
       starts, ends, start_date, end_date, full_day, public, kind, description
FROM planning_assignment
WHERE uuid = @uuid;

-- name: SetPlanningAssignment :exec
INSERT INTO planning_assignment(
       uuid, version, planning_item, status, publish, publish_slot,
       starts, ends, start_date, end_date, full_day, public, kind, description
) VALUES (
       @uuid, @version, @planning_item, @status, @publish, @publish_slot,
       @starts, @ends, @start_date, @end_date, @full_day, @public, @kind,
       @description
)
ON CONFLICT ON CONSTRAINT planning_assignment_pkey DO UPDATE
SET
   version = @version, planning_item = @planning_item, status = @status,
   publish = @publish, publish_slot = @publish_slot, starts = @starts,
   ends = @ends, start_date = @start_date, end_date = @end_date,
   full_day = @full_day, public = @public, kind = @kind,
   description = @description;

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
