-- Write your migrate up statements here

create table metric_kind(
        -- id bigint generated always as identity primary key,
        name text primary key not null
);

create table metric_label(
        id bigint generated always as identity primary key,
        name text unique not null
);

create table metric(
        id bigint generated always as identity primary key,
        document_uuid uuid,
        kind text,
        metric_label_id int,
        value int,
        created timestamp with time zone,
        foreign key(document_uuid) references document(uuid),
        foreign key(kind) references metric_kind(name),
        foreign key(metric_label_id) references metric_label(id)
);

---- create above / drop below ----

drop table metric;

drop table metric_label;

drop table metric_kind;

