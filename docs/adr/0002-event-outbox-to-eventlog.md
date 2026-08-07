# Mutations flow through an event outbox into the eventlog

Document mutations write an entry to an event outbox table in the same database transaction as the change itself. An asynchronous EventlogBuilder consumes the outbox, assigns each event a sequential ID, enriches it, and appends it to the eventlog, which then fans out to SSE, WebSocket, and the optional EventBridge sink.

## Why

Writing the event in the same transaction as the change is the transactional outbox pattern: an event exists if and only if its change committed — no events are lost on success, and none are emitted for rolled-back writes. Doing the enrichment, ordering, and fan-out out of band keeps the write path fast and lets a single builder assign a total order, so every consumer sees one consistent, replayable sequence.

## Consequences

- The eventlog is **eventually consistent** with the document store: there is a small lag between a committed write and the event becoming visible to consumers.
- Delivery is **at-least-once** from a consumer's perspective; consumers resume from their last-seen event ID rather than assuming exactly-once.
- The total ordering and sequential IDs come from the builder, not from wall-clock time — consumers must order by event ID.
