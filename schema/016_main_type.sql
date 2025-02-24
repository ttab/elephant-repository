ALTER TABLE document
      ADD COLUMN main_doc_type text;

ALTER TABLE delete_record
      ADD COLUMN main_doc_type text;

ALTER TABLE eventlog
      ADD COLUMN main_doc_type text;

CREATE OR REPLACE function sequential_eventlog()
RETURNS TRIGGER AS $$
DECLARE
last_id bigint;
BEGIN
    SELECT COALESCE(MAX(id), 0) INTO last_id FROM eventlog;

    if NEW.id != last_id+1 then
        raise exception 'the eventlog id must be sequential';
    end if;

    return NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequential_eventlog
BEFORE INSERT ON eventlog
FOR EACH row
    EXECUTE PROCEDURE sequential_eventlog();

---- create above / drop below ----

ALTER TABLE document
      DROP COLUMN main_doc_type;

ALTER TABLE delete_record
      DROP COLUMN main_doc_type;

ALTER TABLE eventlog
      DROP COLUMN main_doc_type;

DROP TRIGGER sequential_eventlog ON eventlog;
DROP FUNCTION sequential_eventlog();
