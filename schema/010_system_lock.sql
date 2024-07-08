-- This update is breaking compat with previous versions of the repo as it does
-- a hard switch from deleting to system state. In a situation where we had
-- anything running in production the correct way to do this would be to support
-- both in parallell for one release, and then remove deleting in the next, not
-- worth spending time on now though.

ALTER TABLE document
      ADD COLUMN system_state text,
      DROP COLUMN deleting;

ALTER TABLE document_version
      ADD COLUMN language text;

ALTER TABLE eventlog
      ADD COLUMN system_state text;

ALTER TABLE status_heads
      ADD COLUMN system_state text;

ALTER TABLE acl_audit
      ADD COLUMN system_state text;

ALTER TABLE delete_record
      ADD COLUMN meta_doc_record bigint,
      ADD COLUMN finalised timestamptz,
      ADD COLUMN acl jsonb,
      ADD COLUMN heads jsonb,
      ADD COLUMN current_version bigint;

CREATE INDEX deletes_to_finalise
ON delete_record (created)
WHERE finalised IS NULL;

DROP FUNCTION delete_document(uuid, text, bigint);

CREATE TABLE restore_request(
       id bigint generated always as identity primary key,
       uuid uuid NOT NULL,
       delete_record_id bigint NOT NULL,
       created timestamptz NOT NULL,
       creator text NOT NULL,
       spec jsonb NOT NULL,
       finished timestamptz,
       FOREIGN KEY (delete_record_id) REFERENCES delete_record(id)
               ON DELETE RESTRICT
);

CREATE INDEX restores_to_perform
ON restore_request (created ASC)
WHERE finished IS NULL;

---- create above / drop below ----

ALTER TABLE document
      DROP COLUMN IF EXISTS system_state,
      ADD COLUMN IF NOT EXISTS deleting bool not null default false;

ALTER TABLE document_version
      DROP COLUMN IF EXISTS language;

ALTER TABLE delete_record
      DROP COLUMN IF EXISTS meta_doc_record,
      DROP COLUMN IF EXISTS finalised,
      DROP COLUMN IF EXISTS acl,
      DROP COLUMN IF EXISTS heads,
      DROP COLUMN IF EXISTS current_version;

ALTER TABLE status_heads
      DROP COLUMN IF EXISTS system_state;

ALTER TABLE eventlog
      DROP COLUMN IF EXISTS system_state;

ALTER TABLE acl_audit
      DROP COLUMN IF EXISTS system_state;

DROP TABLE IF EXISTS restore_request;

create function delete_document
(
        in uuid uuid,
        in uri text,
        in record_id bigint
)
returns void
language sql
as $$
   delete from document where uuid = delete_document.uuid;

   insert into document(
          uuid, uri, type, created, creator_uri, updated, updater_uri,
          current_version, deleting
   ) values (
     uuid, uri, '', now(), '', now(), '', record_id, true
   );
$$;



