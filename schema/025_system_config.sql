CREATE TABLE system_config(
       name text PRIMARY KEY,
       value jsonb NOT NULL
);

---- create above / drop below ----

DROP TABLE system_config;
