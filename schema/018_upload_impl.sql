-- Altering large tables is expensive, let's add a json blob where we can stash
-- event information without having to create a dedicated column.
ALTER TABLE eventlog
      ADD COLUMN extra jsonb;

ALTER TABLE upload
      DROP COLUMN name;

ALTER TABLE delete_record
      ADD COLUMN attachments jsonb;

-- Restart with the attached object schema
DROP TABLE IF EXISTS attached_object;

CREATE TABLE IF NOT EXISTS attached_object(
       document uuid NOT NULL,
       name text NOT NULL,
       -- Version of the asset.
       version bigint NOT NULL,
       -- ObjectVersion as returned by the object store.
       object_version text NOT NULL,
       -- AttachedAt document version, this is the version of the document that
       -- attached this version of the object.
       attached_at bigint NOT NULL,
       created_by text NOT NULL,
       created_at timestamptz NOT NULL,
       meta jsonb NOT NULL,
       PRIMARY KEY(document, name, version),
       FOREIGN KEY(document) REFERENCES document(uuid)
               ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS attached_object_current(
       document uuid NOT NULL,
       name text NOT NULL,
       version bigint NOT NULL,
       deleted bool NOT NULL,
       PRIMARY KEY(document, name),
       FOREIGN KEY(document) REFERENCES document(uuid)
               ON DELETE CASCADE,
       FOREIGN KEY(document, name, version)
               REFERENCES attached_object(document, name, version)
               ON DELETE RESTRICT
);

---- create above / drop below ----

DROP TABLE IF EXISTS attached_object_current;
DROP TABLE IF EXISTS attached_object;

CREATE TABLE IF NOT EXISTS attached_object(
       document uuid NOT NULL,
       name text NOT NULL,
       created_by text NOT NULL,
       created_at timestamptz NOT NULL,
       meta jsonb NOT NULL,
       PRIMARY KEY(document, name)
);

ALTER TABLE upload
      ADD COLUMN name text NOT NULL;

ALTER TABLE eventlog
      DROP COLUMN extra;

ALTER TABLE delete_record
      DROP COLUMN attachments;
