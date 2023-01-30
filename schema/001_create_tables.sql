-- Set up basic tables

create table document(
       uuid uuid primary key,
       uri text unique not null,
       created timestamptz not null,
       creator_uri text not null,
       modified timestamptz not null,
       current_version bigint not null,
       deleted boolean not null default false
);

-- Should we model document links in the RDBMs? We would gain referential
-- integrity, but do we really need that? I guess that we could go for just
-- indexing the to_document column instead.
create table document_link(
       from_document uuid not null,
       version bigint not null,
       to_document uuid not null,
       rel text,
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
       hash bytea not null,
       title text not null,
       type text not null,
       language text not null,
       created timestamptz not null,
       creator_uri text not null,
       meta jsonb default null,
       document_data jsonb,
       archived bool not null,
       primary key(uuid, version),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

create table document_status(
       uuid uuid not null,
       name varchar(32) not null,
       id bigint not null,
       version bigint not null,
       hash bytea not null,
       created timestamptz not null,
       creator_uri text not null,
       meta jsonb default null,
       primary key(uuid, name, id),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

create table status_heads(
       uuid uuid not null,
       name varchar(32) not null,
       id bigint not null,
       primary key(uuid, name),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

create table acl(
       uuid uuid not null,
       uri text not null,
       permissions char(1)[] not null,
       primary key(uuid, uri),
       foreign key(uuid) references document(uuid)
               on delete cascade
);

---- create above / drop below ----

drop index document_link_rel_idx;
drop table document_link;
drop table document_version;
drop table status_heads;
drop table document_status;
drop table acl;
drop table document;
