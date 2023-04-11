create table job_lock(
       name text not null primary key,
       holder text not null,
       touched timestamptz not null,
       iteration bigint not null
);

---- create above / drop below ----

drop table if exists job_lock;
