---- tern: disable-tx ----

-- Create index concurrently so that we don't block writes to the document
-- table.
CREATE INDEX CONCURRENTLY idx_doc_type ON document(type, language);

---- create above / drop below ----

DROP INDEX idx_doc_type;
