CREATE TABLE IF NOT EXISTS workflow_state(
       uuid uuid PRIMARY KEY,
       type text NOT NULL,
       language text NOT NULL,
       updated timestamptz NOT NULL,
       updater_uri text NOT NULL,
       step text NOT NULL,
       checkpoint text NOT NULL,
       document_version bigint NOT NULL,
       -- Record the status that changed the state, mostly for book keeping.
       status_name text,
       status_id bigint,
       FOREIGN KEY(uuid) REFERENCES document(uuid)
               ON DELETE CASCADE
);

ALTER TABLE workflow_state REPLICA IDENTITY FULL;
ALTER PUBLICATION eventlog ADD TABLE ONLY workflow_state;

CREATE TABLE IF NOT EXISTS workflow(
       type text PRIMARY KEY,
       updated timestamptz NOT NULl,
       updater_uri text NOT NULL,
       configuration jsonb NOT NULL
);

ALTER TABLE eventlog
      ADD COLUMN workflow_state text,
      ADD COLUMN workflow_checkpoint text;

---- create above / drop below ----

ALTER PUBLICATION eventlog DROP TABLE ONLY workflow_state;

DROP TABLE IF EXISTS workflow_state;
DROP TABLE IF EXISTS workflow;

ALTER TABLE eventlog
      DROP COLUMN workflow_state,
      DROP COLUMN workflow_checkpoint;
