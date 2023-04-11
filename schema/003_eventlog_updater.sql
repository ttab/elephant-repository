ALTER TABLE eventlog
      ADD COLUMN IF NOT EXISTS
          updater text;

---- create above / drop below ----

ALTER TABLE eventlog
      DROP COLUMN IF EXISTS updater;
