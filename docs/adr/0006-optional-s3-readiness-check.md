# The archive bucket is an optional readiness check

The `s3` health check still runs, still reports into
`health_check_up{name="s3"}`, and still appears in the `/health/ready` body as
`"ok": false, "optional": true` when the archive bucket is unreachable — but
since v1.9.0 it no longer fails the readiness probe.

## Why

It used to. An unreachable archive bucket returned 500 from `/health/ready` on
every replica at once, which deregistered the whole fleet from the load balancer
and turned a degraded *background* dependency into a total API outage. Reads and
document writes need Postgres; they do not touch S3. Refusing all traffic
because archiving is behind is strictly worse than serving it.

## Consequences

- **Nothing reacts automatically to an archive bucket outage any more.** If you
  relied on the readiness probe for that, the replacement is an alert on
  `health_check_up{name="s3"}` — there is no other signal.
- The durability guarantee is unchanged and still blunt: the archiver escalates
  a persistent failure into a process exit. Because three of the four archive
  workers park waiting for the `eventlog-archiver` job lock, that kills replicas
  one at a time as leadership moves, rather than all at once. The unlocked
  delete/restore/purge poll loop is the exception — it runs everywhere and so
  fails everywhere at once, when there is work pending.
- Making the probe required again re-creates the fleet-wide outage. If the goal
  is "stop serving when the archive is unavailable", that is a decision about
  the *write* path, not about readiness.
