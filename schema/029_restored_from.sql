-- The generation a restored document was restored from: the delete record
-- and the nonce of the deleted generation. Both are NULL for a document that
-- wasn't created by a restore.
--
-- Adding nullable columns without a default only changes the catalog, but it
-- still takes an ACCESS EXCLUSIVE lock on document. The lock timeout makes
-- the migration fail fast rather than queue every write behind a long
-- running transaction; run it again if it times out.

SET LOCAL lock_timeout = '5s';

ALTER TABLE document
      ADD COLUMN restored_from_record bigint,
      ADD COLUMN restored_from_nonce uuid;

---- create above / drop below ----

SET LOCAL lock_timeout = '5s';

ALTER TABLE document
      DROP COLUMN restored_from_record,
      DROP COLUMN restored_from_nonce;
