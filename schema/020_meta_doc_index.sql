---- tern: disable-tx ----
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_main_doc_id ON document(main_doc);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_main_doc_id;
