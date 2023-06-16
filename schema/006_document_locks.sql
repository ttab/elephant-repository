-- Write your migrate up statements here

create table document_lock(
  uuid uuid primary key not null,
  token text not null,
  created timestamptz not null,
  expires timestamptz not null,
  uri text,
  app text,
  comment text,
  foreign key(uuid) references document(uuid) on delete cascade
);
---- create above / drop below ----

drop table document_lock;

-- Write your migrate down statements here. If this migration is irreversible
-- Then delete the separator line above.
