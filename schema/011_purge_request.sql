CREATE TABLE purge_request(
       id bigint generated always as identity primary key,
       uuid uuid NOT NULL,
       delete_record_id bigint NOT NULL,
       created timestamptz NOT NULL,
       creator text NOT NULL,
       finished timestamptz,
       FOREIGN KEY (delete_record_id) REFERENCES delete_record(id)
               ON DELETE RESTRICT
);

CREATE INDEX purges_to_perform
ON purge_request (id ASC)
WHERE finished IS NULL;

CREATE INDEX pending_purges
ON purge_request (delete_record_id ASC)
WHERE finished IS NULL;

ALTER TABLE delete_record
      ADD COLUMN purged timestamptz,
      -- current_version was redundant, we already had a version column.
      DROP COLUMN IF EXISTS current_version;

-- GetNextRestoreRequest was changed to order by id instead of created.
DROP INDEX restores_to_perform;

CREATE INDEX restores_to_perform
ON restore_request (id ASC)
WHERE finished IS NULL;

---- create above / drop below ----

ALTER TABLE delete_record
      DROP COLUMN IF EXISTS purged;

DROP TABLE IF EXISTS purge_request;

DROP INDEX restores_to_perform;

CREATE INDEX restores_to_perform
ON restore_request (created ASC)
WHERE finished IS NULL;
