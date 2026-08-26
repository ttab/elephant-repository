# Signed archive forms a verifiable merkle chain

Every archived event, document version, and status is written to S3 and signed with an ECDSA P-384 key, and each object embeds the signature of its parent. The archive therefore forms a narrow [merkle tree](https://en.wikipedia.org/wiki/Merkle_tree): a tamper-evident log where any later alteration breaks the chain, and which could back a public transparency log a trusted third party can verify against.

## Why

We need durable proof of what the repository contained at any point in time, not just a backup. A plain copy in S3 would be restorable but not verifiable — nothing would stop silent after-the-fact edits. Chaining signatures makes the whole history self-verifying from the public signing keys (served as JWKS at `GET /signing-keys`; keys rotate every 180 days).

## Consequences

- **Signing happens at archive time, not write time.** Postgres `jsonb` is not guaranteed to be byte-stable, so a signature taken over a database row could not be re-derived and re-verified later. The canonical signed artifact is the marshalled S3 object.
- **Verifying live database contents is therefore indirect**: verify the archive object's signature, then check that the database row is *logically* equivalent to the archived data — not byte-equal.
- Archiving is on the critical path for durability guarantees; it cannot be treated as a best-effort side job.
