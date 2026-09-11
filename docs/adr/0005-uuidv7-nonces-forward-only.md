# Document nonces are UUIDv7, and the old v4 nonces are never rewritten

A document's nonce identifies its generation — the run of versions between a
create (or a recreate) and a delete. From v1.9.0 a new nonce is a UUIDv7, so the
generations of a document sort lexically in the order they were created. Every
nonce minted before that release keeps its random UUIDv4 value. There is no
migration and no backfill, and there will not be one.

## Why

Consumers that put the nonce in a sort key — elephant-distribution's push
delivery keys are the case this was built for — need a recreated document's new
key family to sort after the old one. A time-ordered UUID gives that for free
and changes nothing about how the value is read: it is the same opaque UUID in
the same places, `document_nonce` on eventlog items among them.

**Backfilling the old nonces is not available.** Nonces travel on archived
eventlog items and archived document data, all of which is signed, and each
archived object embeds its parent's signature. Rewriting a nonce would break the
signature chain from that point on and make the archive unverifiable — which is
the one property the archive exists to provide (see
[ADR 0001](0001-signed-archive-merkle-chain.md)).

## Consequences

- **Cross-generation ordering is a convenience between two v7 nonces and never a
  contract.** A consumer must keep deciding which generation is live from the
  events and the deletion markers themselves, exactly as before.
- A document with a pre-v1.9.0 generation and a post-v1.9.0 one will sort them
  arbitrarily. This is permanent.
- The nil (all-zeros) nonce that marks a batch import sorts before every
  generation either way.
