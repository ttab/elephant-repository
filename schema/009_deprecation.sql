create table deprecation(
       label text not null primary key,
       enforced boolean not null
);

---- create above / drop below ----

drop table if exists deprecation;
