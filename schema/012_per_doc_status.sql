CREATE TABLE status_new(
       type text NOT NULL,
       name text NOT NULL,
       disabled bool NOT NULL DEFAULT false,
       PRIMARY KEY(type, name)
);

-- Generate new statuses for all types using the cross product between current
-- statuses and the available document types.
WITH types AS (
     SELECT DISTINCT type FROM document
)
INSERT INTO status_new(type, name, disabled)
SELECT t.type, s.name, s.disabled
FROM status AS s
     CROSS JOIN types AS t;

DROP TABLE status;
ALTER TABLE status_new RENAME TO status;
ALTER TABLE status RENAME CONSTRAINT status_new_pkey TO status_pkey;

---- create above / drop below ----

CREATE TABLE status_old(
       name text NOT NULL PRIMARY KEY,
       disabled bool NOT NULL DEFAULT false
);

-- Recreate old statuses, all usages of a status must be disabled for it to be
-- disabled as a whole.
INSERT INTO status_old(name, disabled)
SELECT name, BOOL_AND(disabled) FROM status GROUP BY name;

DROP TABLE status;
ALTER TABLE status_old RENAME TO status;
ALTER TABLE status RENAME CONSTRAINT status_old_pkey TO status_pkey;
