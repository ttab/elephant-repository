-- Old document_link was about referential integrity, was never used.
DROP TABLE IF EXISTS document_link;

CREATE TABLE document_type(
       type text PRIMARY KEY,
       bounded_collection bool NOT NULL DEFAULT false,
       configuration jsonb NOT NULL
);

ALTER TABLE document
      ADD COLUMN time tstzmultirange,
      ADD COLUMN labels text[];

CREATE INDEX idx_doc_time ON document USING GIST (time);
CREATE INDEX idx_doc_labels ON document USING GIN (labels);

-- Document version contains the time ranges and labels that are intrinsic to
-- the document contents . The document equivalents can be merged with extrinsic
-- data.
ALTER TABLE document_version
      ADD COLUMN time tstzmultirange,
      ADD COLUMN labels text[];

ALTER TABLE planning_assignment
      ADD COLUMN timezone text,
      ADD COLUMN timerange tstzrange;

CREATE TABLE system_config(
       name text PRIMARY KEY,
       value bytea NOT NULL
);

---- create above / drop below ----

-- We don't care about restoring old document_links, was never used.

DROP TABLE IF EXISTS document_type;

ALTER TABLE document
      DROP COLUMN time,
      DROP COLUMN labels;

ALTER TABLE document_version
      DROP COLUMN time,
      DROP COLUMN labels;

ALTER TABLE planning_assignment
      DROP COLUMN timezone,
      DROP COLUMN timerange;

DROP TABLE system_config;
