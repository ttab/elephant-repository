ALTER TABLE document
      ADD COLUMN main_doc uuid,
      ADD COLUMN language text,
      ADD CONSTRAINT fk_main_doc
          FOREIGN KEY (main_doc)
          REFERENCES document(uuid)
          ON DELETE RESTRICT;

ALTER TABLE document_status
      ADD COLUMN meta_doc_version bigint;

ALTER TABLE status_heads
      ADD COLUMN language text;

ALTER TABLE delete_record
      ADD COLUMN main_doc uuid,
      ADD COLUMN language text;

ALTER TABLE acl_audit
      ADD COLUMN language text;

ALTER TABLE eventlog
      ADD COLUMN main_doc uuid,
      ADD COLUMN language text,
      ADD COLUMN old_language text;

CREATE TABLE meta_type(
       meta_type text PRIMARY KEY,
       exclusive_for_meta bool NOT NULL
);

CREATE TABLE meta_type_use(
       main_type text PRIMARY KEY,
       meta_type text NOT NULL,
       FOREIGN KEY (meta_type)
               REFERENCES meta_type(meta_type)
               ON DELETE RESTRICT
);

---- create above / drop below ----

ALTER TABLE document
      DROP COLUMN main_doc,
      DROP COLUMN language;

ALTER TABLE status_heads
      DROP COLUMN language;

ALTER TABLE delete_record
      DROP COLUMN main_doc,
      DROP COLUMN language;

ALTER TABLE acl_audit
      DROP COLUMN language;

ALTER TABLE eventlog
      DROP COLUMN main_doc,
      DROP COLUMN language,
      DROP COLUMN old_language;

ALTER TABLE document_status
      DROP COLUMN meta_doc_version;

DROP TABLE meta_type_use;
DROP TABLE meta_type;
