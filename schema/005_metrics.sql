-- Write your migrate up statements here

create table metric_kind(
        name text primary key not null,
        aggregation smallint not null
);

create table metric_label(
        name text primary key not null
);

create table metric(
        uuid uuid references document(uuid) not null,
        kind text references metric_kind(name) not null,
        label text references metric_label(name),
        value bigint not null,
        primary key(uuid, kind, label)
);

---- create above / drop below ----

drop table metric;

drop table metric_label;

drop table metric_kind;

