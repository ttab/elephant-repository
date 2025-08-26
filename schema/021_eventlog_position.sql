-- This is a really heavy migration that should be done in a maintenance
-- window. Ripping off the bandaid before we have customers using the repo.
--
-- We also take advantage of the fact that no documents in the production
-- environment that have been deleted have been recreated, other the eventlog
-- nonce updates wouldn't be safe.

ALTER TABLE document
      ADD COLUMN nonce uuid NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE delete_record
      ADD COLUMN nonce uuid NOT NULL DEFAULT gen_random_uuid();

-- Remove the default random UUID for nonces, we want them to be explicitly set
-- in the future.
ALTER TABLE document
      ALTER COLUMN nonce DROP DEFAULT;

ALTER TABLE delete_record
      ALTER COLUMN nonce DROP DEFAULT;

ALTER TABLE eventlog
      ADD COLUMN signature text,
      ADD COLUMN nonce uuid;

LOCK TABLE eventlog IN EXCLUSIVE MODE;

-- Backfill the eventlog with the generated nonces.

UPDATE eventlog
       SET nonce = d.nonce
FROM document AS d
     WHERE d.uuid = eventlog.uuid;

UPDATE eventlog
       SET nonce = dr.nonce
FROM delete_record AS dr
     WHERE dr.uuid = eventlog.uuid;

-- Make the eventlog nonce required in the future.
ALTER TABLE eventlog
      ALTER COLUMN nonce SET NOT NULL;

CREATE TABLE IF NOT EXISTS eventlog_archiver(
       size bigint PRIMARY KEY,
       position bigint NOT NULL,
       last_signature text NOT NULL
);


-- The archive counter will be incremented for every event that affects a
-- document, and decreased when the corresponding event is archived.
CREATE TABLE IF NOT EXISTS document_archive_counter(
       uuid uuid PRIMARY KEY,
       unarchived int NOT NULL,
       -- Drop on delete, this is safe as document deletes aren't allowed to
       -- proceed until the archive counter reaches zero.
       foreign key(uuid) references document(uuid)
               on delete cascade
);

---- create above / drop below ----

DROP TABLE IF EXISTS document_archive_counter;

DROP TABLE IF EXISTS eventlog_archiver;

ALTER TABLE eventlog
      DROP COLUMN signature,
      DROP COLUMN nonce;

ALTER TABLE delete_record
      DROP COLUMN nonce;

ALTER TABLE document
      DROP COLUMN nonce;
