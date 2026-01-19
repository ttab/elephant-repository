---- tern: disable-tx ----

-- NB! Run this migration AFTER deploying v1.4.0 otherwise all creates and ACL
-- updates will fail.

-- Not used anymore
DROP TABLE IF EXISTS acl_audit;

-- Index the planning_item foreign key to prevent table scans on cascading
-- deletes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_planning_assignment_planning_item
ON planning_assignment(planning_item);

-- Clean up any remaining eventlog publication.
DROP PUBLICATION IF EXISTS eventlog;

-- Remove remaining stored procedures.
DROP FUNCTION IF EXISTS
     create_status(uuid, character varying, bigint, bigint, timestamptz, text, jsonb),
     create_status(uuid, character varying, bigint, bigint, text, timestamptz, text, jsonb),
     create_version(uuid, bigint, timestamptz, text, jsonb, jsonb);
