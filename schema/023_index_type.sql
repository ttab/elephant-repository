---- tern: disable-tx ----

CREATE INDEX CONCURRENTLY idx_doc_type ON document(type, language);

---- create above / drop below ----

DROP INDEX idx_doc_type;
