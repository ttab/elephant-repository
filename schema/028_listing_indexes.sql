---- tern: disable-tx ----

-- Indexes for Documents.ListDocuments, one per selector kind, each giving a
-- keyset range scan over uuid within a type (and a status or checkpoint).
--
-- Built concurrently so that writes to document, status_heads and
-- workflow_state aren't blocked during the build. Without a transaction a
-- failed build leaves an INVALID index behind that IF NOT EXISTS then skips,
-- so an invalid index has to be dropped by hand before migrating again.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_document_type_uuid
ON document(type, uuid);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_status_heads_type_name_uuid
ON status_heads(type, name, uuid);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_state_type_checkpoint_uuid
ON workflow_state(type, checkpoint, uuid);

---- create above / drop below ----

-- tern reads the annotation separately for each direction, so the down
-- migration needs its own to drop concurrently.
---- tern: disable-tx ----

DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_state_type_checkpoint_uuid;
DROP INDEX CONCURRENTLY IF EXISTS idx_status_heads_type_name_uuid;
DROP INDEX CONCURRENTLY IF EXISTS idx_document_type_uuid;
