ALTER TABLE document
      ADD COLUMN main_doc uuid
      ADD COLUMN language text
      ADD CONSTRAINT fk_main_doc
          FOREIGN KEY (main_doc)
          REFERENCES document(uuid)
          ON DELETE RESTRICT;

ALTER TABLE status_heads
      ADD COLUMN main_doc uuid
      ADD COLUMN language text

create function create_version
(
        in uuid uuid,
        in version bigint,
        in created timestamptz,
        in creator_uri text,
        in meta jsonb,
        in document_data jsonb,
        in meta_parent text
)
returns void
language sql
as $$
   insert into document(
               uuid, uri, type,
               created, creator_uri, updated, updater_uri, current_version,
               main_doc
          )
          values(
               uuid, document_data->>'uri', document_data->>'type',
               created, creator_uri, created, creator_uri, version,
               main_doc
          )
          on conflict (uuid) do update
             set uri = create_version.document_data->>'uri',
                 updated = create_version.created,
                 updater_uri = create_version.creator_uri,
                 current_version = version,;

   insert into document_version(
               uuid, version,
               created, creator_uri, meta, document_data, archived
          )
          values(
               uuid, version,
               created, creator_uri, meta, document_data, false
          );
$$;

---- create above / drop below ----

ALTER TABLE document
      DROP COLUMN meta_parent;
