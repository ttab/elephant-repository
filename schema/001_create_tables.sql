-- Set up basic tables

create table document(
       uuid uuid primary key,
       uri text unique not null,
       type text not null,
       created timestamptz not null,
       creator_uri text not null,
       updated timestamptz not null,
       updater_uri text not null,
       current_version bigint not null,
       deleting bool not null default false
);

ALTER TABLE document REPLICA IDENTITY FULL;

CREATE INDEX document_deleting
ON document (created)
WHERE deleting = true;

create table delete_record(
       id bigint generated always as identity primary key,
       uuid uuid not null,
       uri text not null,
       type text not null,
       version bigint not null,
       created timestamptz not null,
       creator_uri text not null,
       meta jsonb default null
);

create index delete_record_uuid_idx on delete_record(uuid);

create function delete_document
(
        in uuid uuid,
        in uri text,
        in record_id bigint
)
returns void
language sql
as $$
   delete from document where uuid = delete_document.uuid;

   insert into document(
          uuid, uri, type, created, creator_uri, updated, updater_uri,
          current_version, deleting
   ) values (
     uuid, uri, '', now(), '', now(), '', record_id, true
   );
$$;

-- Should we model document links in the RDBMs? We would gain referential
-- integrity, but do we really need that? I guess that we could go for just
-- indexing the to_document column instead.
create table document_link(
       from_document uuid not null,
       version bigint not null,
       to_document uuid not null,
       rel text,
       type text,
       primary key(from_document, to_document),
       foreign key(from_document) references document(uuid)
               on delete cascade,
       foreign key(to_document) references document(uuid)
               on delete restrict
);

create index document_link_rel_idx on document_link(rel, to_document);

create table document_version(
       uuid uuid not null,
       version bigint not null,
       created timestamptz not null,
       creator_uri text not null,
       meta jsonb default null,
       document_data jsonb,
       archived bool not null default false,
       signature text,
       primary key(uuid, version),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

CREATE INDEX document_version_archived
ON document_version (created)
WHERE archived = false;

create function create_version
(
        in uuid uuid,
        in version bigint,
        in created timestamptz,
        in creator_uri text,
        in meta jsonb,
        in document_data jsonb
)
returns void
language sql
as $$
   insert into document(
               uuid, uri, type,
               created, creator_uri, updated, updater_uri, current_version
          )
          values(
               uuid, document_data->>'uri', document_data->>'type',
               created, creator_uri, created, creator_uri, version
          )
          on conflict (uuid) do update
             set uri = create_version.document_data->>'uri',
                 updated = create_version.created,
                 updater_uri = create_version.creator_uri,
                 current_version = version;

   insert into document_version(
               uuid, version,
               created, creator_uri, meta, document_data, archived
          )
          values(
               uuid, version,
               created, creator_uri, meta, document_data, false
          );
$$;

create table document_status(
       uuid uuid not null,
       name varchar(32) not null,
       id bigint not null,
       version bigint not null,
       created timestamptz not null,
       creator_uri text not null,
       meta jsonb default null,
       archived bool not null default false,
       signature text,
       primary key(uuid, name, id),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

-- TODO: Check if these indexes are effective
CREATE INDEX document_status_archived
ON document_status (created)
WHERE archived = false;

create table status_heads(
       uuid uuid not null,
       name varchar(32) not null,
       current_id bigint not null,
       updated timestamptz not null,
       updater_uri text not null,
       primary key(uuid, name),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

ALTER TABLE status_heads REPLICA IDENTITY FULL;

create function create_status
(
        in uuid uuid,
        in name varchar(32),
        in current_id bigint,
        in version bigint,
        in created timestamptz,
        in creator_uri text,
        in meta jsonb
)
returns void
language sql
as $$
   insert into status_heads(
               uuid, name, current_id, updated, updater_uri
          )
          values(
               uuid, name, current_id, created, creator_uri
          )
          on conflict (uuid, name) do update
             set updated = create_status.created,
                 updater_uri = create_status.creator_uri,
                 current_id = create_status.current_id;

   insert into document_status(
               uuid, name, id, version, created, creator_uri, meta
          )
          values(
               uuid, name, current_id, version, created, creator_uri, meta
          );
$$;

create table acl(
       uuid uuid not null,
       uri text not null,
       permissions text[] not null,
       primary key(uuid, uri),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

create table acl_audit(
       id bigint generated always as identity primary key,
       uuid uuid not null,
       updated timestamptz not null,
       updater_uri text not null,
       state jsonb not null,
       archived bool not null default false,
       foreign key(uuid) references document(uuid)
               on delete cascade
);

create table signing_keys(
       kid text primary key,
       spec jsonb not null
);

create table document_schema(
       name text not null,
       version text not null,
       spec jsonb not null,
       primary key(name, version)
);

create table active_schemas(
       name text primary key,
       version text not null,
       foreign key(name, version) references
               document_schema(name, version)
);

create table status(
       name text primary key,
       disabled bool not null default false
);

create table report(
       name text primary key,
       enabled boolean not null,
       next_execution timestamptz not null,
       spec jsonb not null
);

insert into status(name)
       VALUES ('done'), ('approved'), ('usable');

INSERT INTO document_schema (name, version, spec) VALUES ('tt', 'v1.0.0', '{"name": "TT", "version": 1, "documents": [{"meta": [{"data": {"initials": {"optional": true}, "signature": {"optional": true}}, "name": "TT contact extensions", "match": {"type": {"const": "core/contact-info"}}, "description": "TODO: why the duplicate signature/initials/short desc?"}], "links": [{"name": "Same as TT Author", "declares": {"rel": "same-as", "type": "tt/author"}, "attributes": {"uri": {"glob": ["tt://author/*"]}, "title": {}}, "description": "Marks the author as a special TT author"}], "match": {"type": {"const": "core/author"}}}, {"links": [{"name": "TT Organiser", "declares": {"rel": "organiser", "type": "tt/organiser"}, "attributes": {"uri": {}, "title": {}}, "description": "TODO: is this good data, or just noise?"}, {"name": "TT Participant", "declares": {"rel": "participant", "type": "tt/participant"}, "attributes": {"uri": {}, "title": {}}, "description": "TODO: is this good data, or just noise?"}], "match": {"type": {"const": "core/event"}}}, {"links": [{"data": {"id": {"format": "int"}}, "declares": {"rel": "same-as", "type": "iptc/mediatopic"}, "attributes": {"uri": {"glob": ["iptc://mediatopic/*"]}}}], "match": {"type": {"const": "core/category"}}, "attributes": {"uri": {"glob": ["iptc://mediatopic/*"]}}}, {"meta": [{"declares": {"type": "tt/type"}, "attributes": {"value": {}}}], "match": {"type": {"const": "core/organisation"}}}, {"meta": [{"declares": {"type": "tt/slugline"}, "attributes": {"value": {}}}, {"name": "Sector", "declares": {"type": "tt/sector"}, "attributes": {"value": {}}, "description": "TODO: what is sector?"}], "links": [{"data": {"id": {}}, "name": "Same as TT event", "declares": {"rel": "same-as", "type": "tt/event"}, "attributes": {"uri": {"glob": ["tt://event/*"]}}, "description": "TODO: what is this? Maybe a one-off, was in 69da3ef5-f1b0-5caf-b846-ca5682b9adf9"}, {"name": "Content size", "match": {"type": {"const": "core/content-size"}}, "attributes": {"uri": {"enum": ["core://content-size/article/medium"]}}, "description": "Specifies the content sizes we can use"}, {"name": "Alternate ID", "declares": {"rel": "alternate", "type": "tt/alt-id"}, "attributes": {"uri": {}}, "description": "TODO: is this actually used for live data? See stage/df6ebaba-b3fc-40ff-9ad2-19f953eb0c6a"}], "match": {"type": {"const": "core/article"}}, "content": [{"data": {"text": {"allowEmpty": true}}, "name": "Dateline", "declares": {"type": "tt/dateline"}, "description": "TODO: there MUST be a better name for this!"}, {"data": {"text": {"format": "html", "allowEmpty": true}}, "name": "Question", "declares": {"type": "tt/question"}}, {"data": {"caption": {"allowEmpty": true}}, "name": "TT visual element", "links": [{"data": {"width": {"format": "int"}, "credit": {}, "height": {"format": "int"}, "hiresScale": {"format": "float"}}, "declares": {"rel": "self"}, "attributes": {"uri": {}, "url": {}, "type": {"enum": ["tt/picture", "tt/graphic"]}}}], "declares": {"type": "tt/visual"}, "description": "This can be either a picture or a graphic"}]}]}');

INSERT INTO active_schemas (name, version) VALUES ('tt', 'v1.0.0');

create table status_rule(
       name text primary key,
       description text not null,
       access_rule bool not null,
       applies_to text[] not null,
       for_types text[] not null,
       expression text not null
);

create publication eventlog
for table document, status_heads, delete_record, acl;

---- create above / drop below ----

drop role reporting;
drop table report;
drop publication eventlog;
drop function create_version(
     uuid, bigint, timestamptz, text, jsonb, jsonb);
drop function delete_document(
     uuid, text, bigint);
drop function create_status(
     uuid, varchar(32), bigint, bigint, timestamptz, text, jsonb);
drop table signing_keys;
drop table status;
drop table status_rule;
drop table active_schemas;
drop table document_schema;
drop index document_link_rel_idx;
drop table document_link;
drop index document_version_archived;
drop table document_version;
drop table status_heads;
drop index document_status_archived;
drop table document_status;
drop table delete_record;
drop table acl;
drop table acl_audit;
drop index document_deleting;
drop table document;
