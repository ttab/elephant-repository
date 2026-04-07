CREATE TYPE schema_generation_status AS ENUM (
        'active',
        'pending',
        'deactivated'
);

CREATE TABLE schema_generation(
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        identity_hash text UNIQUE NOT NULL,
        status schema_generation_status NOT NULL DEFAULT 'deactivated',
        created timestamptz NOT NULL,
        activated timestamptz,
        deactivated timestamptz
);

CREATE TABLE schema_generation_schema(
        generation_id bigint NOT NULL REFERENCES schema_generation(id),
        name text NOT NULL,
        version text NOT NULL,
        PRIMARY KEY (generation_id, name),
        FOREIGN KEY (name, version) REFERENCES document_schema(name, version)
);

CREATE TABLE schema_exemplar(
        name text NOT NULL,
        version text NOT NULL,
        doc_type text NOT NULL,
        document jsonb NOT NULL,
        PRIMARY KEY (name, version)
);

CREATE TABLE schema_generation_exemplar(
        generation_id bigint NOT NULL REFERENCES schema_generation(id),
        name text NOT NULL,
        version text NOT NULL,
        PRIMARY KEY (generation_id, name),
        FOREIGN KEY (name, version) REFERENCES schema_exemplar(name, version)
);

CREATE TABLE schema_generation_event(
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        generation_id bigint NOT NULL REFERENCES schema_generation(id),
        event text NOT NULL,
        timestamp timestamptz NOT NULL,
        signature text
);

CREATE TABLE schema_generation_archiver(
        id boolean PRIMARY KEY DEFAULT true,
        position bigint NOT NULL DEFAULT 0,
        last_signature text NOT NULL DEFAULT '',
        CONSTRAINT single_row CHECK (id)
);

ALTER TABLE document_version
      ADD COLUMN schema_generation bigint NOT NULL DEFAULT 0;

---- create above / drop below ----

ALTER TABLE document_version
      DROP COLUMN schema_generation;

DROP TABLE schema_generation_archiver;
DROP TABLE schema_generation_event;
DROP TABLE schema_generation_exemplar;
DROP TABLE schema_exemplar;
DROP TABLE schema_generation_schema;
DROP TABLE schema_generation;

DROP TYPE schema_generation_status;
