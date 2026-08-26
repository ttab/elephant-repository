# Observability

Every metric the repository exports, and what a *change* in it means. The
definition is in the name; what is written here is which direction is bad, what
is routinely non-zero, and what the number should be read against.

| Document | What it settles |
|---|---|
| [README](../README.md) | Orientation and the working reference: what the repository holds, how to build and run it, and what every configuration flag does. |
| [architecture.md](architecture.md) | How the service is built: the process model, the data flow through every worker, each subsystem, and the API surface. |
| [ops.md](ops.md) | The operator's-eye view: dependencies, deployment shape, bootstrap order, and the failure modes with the signal that shows each one. |
| **observability.md** (this document) | Every metric the service exports and what a change in it means. |

This document does not say what to *do* about a metric that has gone the wrong
way — [ops.md](ops.md) catalogues the failure modes and their actions, and
[its watch list](ops.md#what-to-watch-in-order) ranks these signals. It also
does not describe the `Metrics` Twirp service, which stores per-document
measurements and has nothing to do with Prometheus; that is in
[architecture.md](architecture.md#document-metrics).

Metrics and pprof are served on `--profile-addr` (`:1081` by default) at
`/metrics` and `/debug/pprof/`. Everything is registered against
`prometheus.DefaultRegisterer` from `cmd/repository/main.go`, so a metric only
exists if the subsystem that owns it was started — a disabled archiver exports
no archiver metrics at all, which is different from exporting zeroes.

Two prefixes are in use for historical reasons: newer collectors use
`repository_`, older ones `elephant_`. **Do not rename or relabel any of them
without an explicit decision** — dashboards and alerts outside this repository
depend on the current names.

## Conventions that apply everywhere

* **A position gauge is only meaningful on the instance holding the relevant
  job lock.** The eventlog archiver, batch archiver, docstream position and
  scheduler gauges are set by the worker as it advances, so on an instance that
  does not hold the lock they stay at whatever they were last set to — which,
  for an instance that has never held it, is 0. Aggregate with `max()` across
  instances, never `avg()` or `sum()`.
* **A `status` or `outcome` label of `error` counts an attempt that failed and
  will be retried, not a permanent loss.** Sustained growth is the signal;
  isolated increments during a deploy or a database failover are expected.
* **Counters are cumulative per process.** Every one of them resets when a pod
  restarts, which is itself information — see
  `elephant_eventlog_start_total`.
* Lag is measured by comparing two numbers, not by reading one. Almost every
  interesting question here is "does A still track B", and the pairs to compare
  are named in each section.

## Eventlog builder

The pair to watch is `elephant_eventlog_events_total` against write traffic
(`rpc_requests_total` for `Documents.Update`): **events built must keep pace
with documents written, because everything downstream — SSE, websockets, the
event sink and the entire archive — reads the eventlog and nothing else.**

* `elephant_eventlog_start_total` — starts of the builder in this process.
  Exactly 1 is healthy. More than one means the builder crashed and the job
  lock re-elected it; because the builder is the only writer of eventlog IDs,
  repeated restarts are worth understanding rather than tolerating. Note that a
  process which never won the lock reports 0, so read this only on the leader.
* `elephant_eventlog_events_total{type, doc_type}` — events built from the
  outbox, by event type and document type. A flat line while documents are
  being updated means event delivery has stopped: outbox rows are accumulating
  and no consumer is seeing anything. The `type` breakdown is also the cheapest
  view of what kind of activity the repository is seeing.

## Archiver

The pair to watch is `elephant_archiver_event_archiver_position` against the
highest eventlog ID. **Archiving lag is not just archive lag: deletes block on
their document being fully archived, so a stalled archiver eventually stalls
deletes too.**

* `elephant_archiver_event_archiver_position` — the eventlog ID the archiver
  has written to S3. Must advance whenever events are being built. Compare with
  the eventlog head; the difference is the archive backlog in events.
* `elephant_event_archived_total{event_type, status}` — items archived, by
  event type and `ok`/`error`. `error` increments mean the archiver is failing
  and retrying; after about five minutes of that the process exits. Because
  this worker only runs on the `eventlog-archiver` lock holder, sustained
  errors show up as replicas restarting **one at a time** as leadership moves,
  not as a fleet-wide crash loop — so read this together with
  `pg_job_lock_transitions_total{name="eventlog-archiver"}`.
* `elephant_archiver_batch_1k_position` / `elephant_archiver_batch_10k_position`
  — how far the compaction batchers have got. These trail the event archiver by
  design (up to 999 and 9999 events respectively). A gap much larger than that,
  and growing, means batching has stalled while single-event archiving
  continues. Nothing reads from the batches on the write path, so this is a
  lower-urgency signal than the event archiver's own position.
* `elephant_archiver_batches_created_total{size, status}` — 1k and 10k batches
  created. Its rate is derivable from write volume: one 1k batch per 1000
  events. `error` means a batch failed to build, usually because an event
  object is missing from S3.
* `elephant_archiver_deletes_total{status}` — delete finalisations. Every
  client-visible delete must eventually produce one; until it does the document
  sits as a `deleting` placeholder that cannot be read or written.
* `elephant_archiver_delete_moves_total{status}` — individual S3 object moves
  during delete processing. Expect many per delete. `error` here is what makes
  `elephant_archiver_deletes_total{status="error"}` grow.
* `elephant_archiver_restores_total{status}` — restore operations processed.
  Normally zero; these are operator-initiated.
* `elephant_archiver_purges_total{status}` — purges processed. Normally zero.
* `elephant_archiver_purge_deletes_total{status}` — S3 objects deleted during
  purge. **An `error` here means purged document data may still be sitting in
  the archive**, which is the one archiver error with a compliance dimension
  rather than just a durability one.

Note what is *not* here. **There is no signing-key metric of any kind** — nothing
reports when the current key expires, whether rotation succeeded, or whether the
public keys have been published to the bucket. Rotation is automatic with about
five days of slack, so a rotation that has been failing is invisible until
archiving stops outright; a "seconds until the current key expires" gauge and a
provisioning-failure counter are
[pending work](../README.md#pending-work). There is also no metric for the
delete/restore/purge poll loop's liveness as distinct from its work, so an idle
poll loop and a wedged one look the same.

## Validator and schemas

The pair to watch is `elephant_validator_schema_generation` against the
generation the API reports as active. **They should converge within seconds of an
activation, because the notification that triggers the reload is published in the
activating transaction; a divergence that persists for even a minute is already
a symptom.** The gauge is the only way to see that an activation has actually
reached the instances.

* `elephant_validator_schema_generation` — the generation ID *this instance*
  enforces. Set on every successful schema reload. Because validation is served
  from an in-memory validator while `ListActive` and `GetAllActive` read the
  database, this gauge
  is what distinguishes "activated" from "in effect". Aggregate with `min()`
  across instances to find the laggard, and expect it to catch up in seconds
  rather than minutes — the five-minute reload timer is a fallback for a lost
  notification, so a laggard that takes minutes is telling you notification
  delivery is broken, not that it is working as designed. A value of 0 means no
  generation exists yet — it no longer means "failed to read the generation ID",
  because that case now fails the whole reload instead.
* `elephant_schema_refresh_failures_total` — failed reloads of the active
  schemas. **Every increment leaves the instance enforcing the schemas it last
  loaded successfully, indefinitely.** There is no backoff-to-crash here and
  nothing will retry it into working other than the next notification or the
  five-minute timer, so any sustained growth means at least one instance is
  frozen on stale schemas while reporting healthy.
* `elephant_deprecation_refresh_failures_total` — the same for enforced
  deprecations: the instance keeps the enforcement set it last loaded.
* `elephant_pending_validation_failures_total{doc_type, error}` — writes that
  passed the active schemas but would fail the *pending* generation. This is a
  forecast, not a fault: it is the list of documents that will start being
  rejected if the pending generation is activated as-is. Read it before
  activating, and expect it to be non-zero while a new generation is being
  worked on.
* `elephant_deprecations_total{label}` — uses of a deprecated construct that is
  not yet enforced. Growth means clients are still writing content that will
  break when the deprecation is enforced. Driving a label to zero is the
  precondition for enforcing it.
* `elephant_docs_with_deprecations_total{doc_type}` — the same activity counted
  per document type, which is how you find out *who* to talk to.

Workflow reloads have no failure counter. `Workflows.reloadLoop` logs a failed
refresh with a `LogKeyCountMetric` hint naming
`elephant_workflow_refresh_failure_count`, but no such metric is registered and
nothing converts the log field into one — **a workflow reload that keeps failing
is visible in logs only.** Treat this as a known gap, not as a metric to look
for.

## Scheduled publishing

* `elephant_scheduled_delayed` — documents whose planned publish time has
  passed without them being published, counted over the last 24 hours with a
  one-minute grace threshold. **This is the metric that tells you publishing is
  broken from the newsroom's point of view**, and it is the one number here a
  non-engineer would recognise. It is a gauge set by the scheduler on each
  iteration, so it is only meaningful on the lock holder, and it does not
  distinguish "the scheduler is stuck" from "the scheduler is running and the
  updates are failing". Read it with the counter below.
* `elephant_scheduled_publish_total{outcome}` — publish attempts by
  `success`/`failure`. Failures mean documents that were due did not go out;
  after 30 minutes past the planned time the scheduler stops trying, so a
  failure that persists that long is permanent until someone intervenes.

## Event sink forwarder

Only present when a sink is configured and `--no-eventsink` is not set.

* `elephant_event_forwarder_latency_seconds{name}` — time from event creation to
  sink delivery, exponential buckets from 100 ms across 11 buckets (up to
  ~102 s). Rising quantiles mean the forwarder is falling behind the eventlog.
  This is lag, not loss — the position is persisted, so it catches up.
* `elephant_event_forwarder_restarts_total{name}` — restarts of the forwarder
  loop. Each one is a failure that has been retried after a 10-second wait.
  Steady growth means forwarding is failing repeatedly rather than progressing.
* `elephant_event_forwarder_skipped_total{name, type, reason}` — **events that
  were not delivered and will not be retried. This is loss, and it is the one
  counter in this document where a non-zero value means a downstream consumer
  is permanently missing something.** `reason="deleted"` is the routine case: a
  document deleted before the forwarder reached its event cannot be enriched,
  so the event is dropped. Sustained growth for any other reason deserves
  investigation.

## Document stream (websocket fan-out)

Only present when `--no-websocket` is not set.

* `repository_docstream_position` — the eventlog ID fanned out to websocket
  subscribers. Per-instance, and independent of every other consumer's
  position. A position that stops advancing on one instance while others
  advance means that instance's subscribers have gone quiet — and they will not
  notice, because a stalled stream looks exactly like an idle repository from
  the client side.
* `repository_docstream_emit_total{status}` — emit attempts, labelled `start`
  and then `ok` or `error`. `start` minus `ok` minus `error` is the number of
  emits in flight or lost to a panic. Errors mean subscribers are not receiving
  document updates; the loop continues, so this is dropped delivery for that
  batch rather than a crash.
* `repository_docstream_subscribers` — currently registered stream consumers.
  Per-instance, and it counts subscriptions, not connections: one socket can
  hold several.

## Websocket API

Only present when `--no-websocket` is not set.

* `repository_open_sockets` — currently open websocket connections on this
  instance. Sum across instances for a cluster total. A cliff is a disconnect
  event; a slow climb with no matching drop suggests connections that are never
  being closed.
* `repository_websocket_rate_limited_total{reason}` — connections and
  subscriptions turned away. Despite the name it counts every rejection, not
  just rate limiting; the `reason` label is what matters:
  `no_token`, `invalid_token`, `expired_token` (client-side auth problems, and
  `expired_token` often just means a client that reconnects lazily),
  `rate_limit` (more than one connection per 5 s for a socket token),
  `upgrade_failed` (the HTTP upgrade itself failed), and `eventlog_stream` (a
  subscription's live stream exceeded its token bucket and was stopped).
  **Growth in `eventlog_stream` means clients are being cut off mid-stream and
  are expected to resubscribe** — if it is sustained, either the client is not
  resubscribing or the rate limits are set below what the workload needs.
* `repository_websocket_call_total{method}` — calls received, by socket API
  method. Shows what clients are actually doing and which methods drive load.
* `repository_websocket_response_total{method, status, response}` — responses,
  where `status` carries the error code and **is empty on success**. Any
  non-empty `status` is a client call that failed. `eventlog_resume_oob` there
  means a client asked to resume from a position older than the replay buffer,
  which is a signal to raise `--eventlog-buffer-size` or to accept that clients
  must handle it.

There are no SSE metrics. SSE connections, publishes and replay-buffer misses
are not instrumented at all, so an SSE-only outage is invisible here — check
`rpc_*` for `/sse`'s absence and the logs. Known gap.

## Metrics from libraries, not from this repository

These come from `elephantine`, `pgx` and the Prometheus client library, and are
documented properly in
[elephantine's `docs/metrics.md`](https://github.com/ttab/elephantine). What
matters here is that they exist and what they cover:

| Metric | Covers |
|---|---|
| `rpc_requests_total`, `rpc_responses_total`, `rpc_duration_seconds` | Every Twirp call, by service, method and response code. The first place to look for an API-visible problem. |
| `client_requests_total`, `client_request_duration_seconds`, `client_in_flight_requests` | Outbound HTTP, labelled `s3`. This is the S3 dependency's latency and error rate. |
| `pgxpool_*{pool}` | Connection pool state per pool: `main` and, when a bouncer connection string is configured, `pubsub`. `pgxpool_empty_acquires_total` and `pgxpool_empty_acquire_wait_seconds_total` growing together is pool exhaustion. |
| `pg_job_lock_held`, `pg_job_lock_transitions_total` | Which single-leader jobs this instance holds, and how often leadership moves. Exactly one holder per lock across the cluster is the invariant; frequent transitions mean leases are being lost. |
| `task_restarts_total{task}` | Restarts of retried background tasks, which for this service is the four archiver goroutines. Each tolerates 30 restarts within an hour before the process exits, so this is the early warning for that. Only the `run poll loop` task can climb on a non-leader; the other three park waiting for their job lock and never restart there. |
| `health_check_up{name}` | The readiness checks behind `/health/ready`: `s3` (write, read and delete a probe object in the archive bucket) and `postgres` (read the active schemas). **`postgres` is a hard check; `s3` is optional** — a failing `s3` check sets this gauge to 0 and reports `"ok": false` in the response body, but does not fail readiness or deregister the pod. That makes `health_check_up{name="s3"}` the *only* automatic signal for an archive bucket outage, so it needs an alert rather than a probe. |
| Go runtime and process collectors | Default registry contents: heap, goroutines, GC, file descriptors. |
