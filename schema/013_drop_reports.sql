drop table report;

---- create above / drop below ----

create table report(
       name text primary key,
       enabled boolean not null,
       next_execution timestamptz not null,
       spec jsonb not null
);
