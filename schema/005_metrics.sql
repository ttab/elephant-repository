-- Write your migrate up statements here

create table metric_kind(
        name text primary key not null,
        aggregation smallint not null
);

create table metric(
        uuid uuid,
        kind text,
        label text,
        value bigint not null,
        primary key(uuid, kind, label),
        foreign key(uuid) references document(uuid) on delete cascade,
        foreign key(kind) references metric_kind(name) on delete cascade
);

---- create above / drop below ----

drop table metric;

drop table metric_kind;

