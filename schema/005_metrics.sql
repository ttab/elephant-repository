-- Write your migrate up statements here

create table metric_kind(
        name text primary key not null,
        aggregation smallint not null
);

create table metric_label(
        name text,
        kind text,
        primary key(name, kind),
        foreign key(kind) references metric_kind(name) on delete cascade
);

create table metric(
        uuid uuid,
        kind text,
        label text,
        value bigint not null,
        primary key(uuid, kind, label),
        foreign key(uuid) references document(uuid) on delete cascade,
        foreign key(kind) references metric_kind(name),
        constraint metric_label_kind_match foreign key(label, kind) references metric_label(name, kind)
);

---- create above / drop below ----

drop table metric;

drop table metric_label;

drop table metric_kind;

