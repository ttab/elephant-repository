---- tern: disable-tx ----

-- NB! Run this migration AFTER deploying v1.4.0 otherwise all creates and ACL
-- updates will fail.

-- Not used anymore
DROP TABLE acl_audit;

-- Index the planning_item foreign key to prevent table scans on cascading
-- deletes.
CREATE INDEX CONCURRENTLY idx_planning_assignment_planning_item
ON planning_assignment(planning_item);

-- Clean up any remaining eventlog publication.
DROP PUBLICATION IF EXISTS eventlog;
