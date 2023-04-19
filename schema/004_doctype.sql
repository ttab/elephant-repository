ALTER TABLE status_heads
      ADD COLUMN IF NOT EXISTS
          type text,
      ADD COLUMN IF NOT EXISTS
          version bigint;

ALTER TABLE acl_audit
      ADD COLUMN IF NOT EXISTS
          type text;

create or replace function create_status
(
        in uuid uuid,
        in name varchar(32),
        in current_id bigint,
        in version bigint,
        in type text,
        in created timestamptz,
        in creator_uri text,
        in meta jsonb
)
returns void
language sql
as $$
   insert into status_heads(
               uuid, name, type, version, current_id, updated, updater_uri
          )
          values(
               uuid, name, type, version, current_id, created, creator_uri
          )
          on conflict (uuid, name) do update
             set updated = create_status.created,
                 updater_uri = create_status.creator_uri,
                 current_id = create_status.current_id,
                 version = create_status.version;

   insert into document_status(
               uuid, name, id, version, created, creator_uri, meta
          )
          values(
               uuid, name, current_id, version, created, creator_uri, meta
          );
$$;

---- create above / drop below ----

ALTER TABLE status_heads
      DROP COLUMN IF EXISTS type,
      DROP COLUMN IF EXISTS version;

ALTER TABLE acl_audit
      DROP COLUMN IF EXISTS type;

DROP FUNCTION IF EXISTS create_status(
     uuid, varchar(32), bigint, bigint, text, timestamptz, text, jsonb);
