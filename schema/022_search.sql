-- Old document_link was about referential integrity, was never used.
DROP TABLE IF EXISTS document_link;

CREATE TABLE document_type(
       type text PRIMARY KEY,
       bounded_collection bool NOT NULL DEFAULT false,
       configuration jsonb NOT NULL
);

ALTER TABLE document
      ADD COLUMN time tstzmultirange;

CREATE INDEX idx_doc_time ON document USING GIST (time);

---- create above / drop below ----

-- We don't care about restoring old document_links, was never used.

DROP TABLE IF EXISTS document_type;
ALTER TABLE document
      DROP COLUMN time;
