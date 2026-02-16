ALTER TABLE signing_keys ADD COLUMN archived boolean NOT NULL DEFAULT false;

---- create above / drop below ----

ALTER TABLE signing_keys DROP COLUMN archived;
