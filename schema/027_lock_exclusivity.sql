-- Write your migrate up statements here

alter table document_lock
  add column exclusivity text not null default 'document';

---- create above / drop below ----

alter table document_lock drop column exclusivity;

-- Write your migrate down statements here. If this migration is irreversible
-- Then delete the separator line above.
