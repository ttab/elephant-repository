CREATE TABLE IF NOT EXISTS eventlog_archiver(
       size bigint PRIMARY KEY,
       position bigint NOT NULL,
       last_signature text NOT NULL
);

---- create above / drop below ----

DROP TABLE IF EXISTS eventlog_archiver;
