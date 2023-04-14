-- Write your migrate up statements here

create table metric_kind(
        name text primary key not null
);

create table metric_label(
        name text primary key not null
);

create table metric(
        id bigint generated always as identity primary key,
        document_uuid uuid,
        kind text,
        label text,
        value int,
        created timestamp with time zone,
        foreign key(document_uuid) references document(uuid),
        foreign key(kind) references metric_kind(name),
        foreign key(label) references metric_label(name)
);

---- create above / drop below ----

drop table metric;

drop table metric_label;

drop table metric_kind;

