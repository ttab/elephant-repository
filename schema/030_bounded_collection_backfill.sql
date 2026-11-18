-- The bounded_collection column was never written when a type was configured,
-- only the bounded_collection key in the configuration JSON, so every row
-- reads false. Bring the column in line with the configuration it was saved
-- with. document_type holds one row per configured type, so this is cheap.

UPDATE document_type
SET bounded_collection = COALESCE(
    (configuration->>'bounded_collection')::boolean, false);

---- create above / drop below ----

-- Nothing to undo: the backfilled values are the correct ones.
