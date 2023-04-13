-- Write your migrate up statements here

create table metric_kind(
        id int generated always as identity primary key,
        name text not null
);

create table metric_label(
        id int generated always as identity primary key,
        name text not null
);

create table metric(
        id bigint generated always as identity primary key,
        document_uuid uuid,
        metric_kind_id int not null,
        metric_label_id int,
        value int,
        created timestamp with time zone,
        foreign key(document_uuid) references document(uuid),
        foreign key(metric_kind_id) references metric_kind(id),
        foreign key(metric_label_id) references metric_label(id)
);

---- create above / drop below ----

drop table metric;

drop table metric_label;

drop table metric_kind;

