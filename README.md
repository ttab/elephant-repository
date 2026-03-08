# Elephant repository

![Image](docs/elephant.png?raw=true)

Elephant repository is a [NewsDoc](https://github.com/ttab/newsdoc) document repository with versioning, ACLs for permissions, archiving, validation schemas, workflow statuses, event output, and metrics for observability.

The repository depends on PostgreSQL for data storage and a S3 compatible store for archiving and reports. It can use AWS EventBridge as an event sink, but that is optional and can be disabled with `--no-eventsink`.

All operations against the repository are exposed as a [Twirp RPC API](https://twitchtv.github.io/twirp/docs/intro.html) that you can communicate either using [Protobuf](https://protobuf.dev/) messages or standard JSON. See [Calling the API](#calling-the-api) for more details on communicating with the defined services.

## Versioning

All updates to a document are stored as sequentially numbered versions with information about when it was created, who created it, and optional metadata for the version.

Old versions can always be fetched through the API, and the history can be inspected through `Documents.GetHistory`.

## ACLs for permissions

Documents can be shared with individuals or units (groups of people). By default only the entity that created a document has access to it, and other entities will have to be granted access.

The four ACL permission types are:

* **Read** — read the document
* **Write** — create new versions
* **MetaWrite** — update document metadata without creating a full version
* **SetStatus** — set statuses on the document

In most workflows documents will be shared with a group of people, but this makes it possible to work with private drafts, and share documents with individuals that are untrusted in the sense that they shouldn't have access to all your content.

## Validation schemas

All document types need to be declared before they can be stored in the repository. This serves two purposes, of which the primary is to maintain data quality, the other purpose is to inform automated systems about the shape of your data. This is leveraged by the [elephant-index](https://github.com/ttab/elephant-index) to create correct mappings for OpenSearch/ElasticSearch.

Schema management is handled through the `Schemas` service. For details on how to write specifications, see [revisor "Writing specifications"](https://github.com/ttab/revisor#writing-specifications).

## Workflow statuses

You can define and set statuses for document versions. To publish a version of a document you would typically set the status "usable" for it. Your publishing pipeline would then pick up that status event and act on it. New document versions that are created don't affect the "usable" status you set, to publish a new version you would have to create a new "usable" status update that references that version.

The last status of a given name for a document is referred to as the "head". Just like documents, statuses are versioned and have sequential IDs for a given document and status name.

## Scheduled publishing

The repository includes a scheduler that handles future-dated document publishing. When a document enters the "withheld" workflow state with a planned publish time, the scheduler will automatically publish it at the specified time. The scheduler polls for new scheduled documents every minute and has a 30-minute retry window after the planned publish time.

Scheduled publishing is enabled by default and can be disabled with `--no-scheduler` / `NO_SCHEDULER`.

## Attaching objects (files/assets)

It's possible to attach objects (files/assets) to documents. This can be done using the `documents.CreateUpload` method to get an upload ID and URL. After making a PUT-request to the upload URL with the contents of the object the ID can be used together with a `documents.Update` request that performs a document write to attach the object to the document.

When an object has been attached to a document that information is shown in the event for the update as `attached_objects`, conversely a detach shown as `detached_objects`. Assets are also described in the response to `Documents.GetMeta`.

To download attachments use the `Documents.GetAttachments` with `DownloadLink` set to true, the response will then include a link that the object contents can be downloaded from.

The actual attached objects are currently not being archived. Still an open question whether they should be, if this is used to store images and video it might not be something that we want automatically duplicated. They are, however, copied to the archive bucket if their document is deleted, so a document can be restored together with its attachments. Only the latest version of the currently attached objects are restored, backup of attachments has to be solved outside of the repository.

## Event output

All changes to a document are emitted on the eventlog, accessed through `Documents.Eventlog`. Changes can be:

* a new document version
* a new document status
* updated ACL entries
* a document delete

This eventlog can be used by other applications to act on changes in the repository.

### Server-Sent Events (SSE)

The repository exposes an SSE endpoint at `/sse` for real-time event streaming. It maintains a 200-event replay buffer so that clients that reconnect can catch up on missed events. Clients can filter events by topic using the standard SSE `Last-Event-ID` header for resumption.

SSE can be disabled with `--no-sse` / `NO_SSE`.

### WebSocket

A WebSocket API is available at `/websocket/:token` for per-user rate-limited document streaming. Clients authenticate using JWT socket tokens. The WebSocket API supports subscribing to specific documents and receiving real-time updates.

WebSocket support can be disabled with `--no-websocket` / `NO_WEBSOCKET`.

### Event sink

The repository also has the concept of event sinks where enriched events can be posted to an event sink (right now we only support AWS EventBridge as a sink).

The purpose of the enriched events is to allow the construction of event-based architectures where f.ex. a Lambda function could subscribe to published articles with a specific category. This lets you avoid situations where a lot of systems load unnecessarily just to determine if an event should be handled.

## Document metrics

The repository has a `Metrics` Twirp service for storing and retrieving custom document-level metrics. These are distinct from the operational Prometheus metrics (see [Observability](#observability)). Document metrics support different aggregation modes (replace, increment) and can be used for tracking things like character counts. A built-in character counter is enabled by default (disable with `--no-charcounter` / `NO_CHARCOUNTER`).

## Partial document fetching

The `Documents.Get` and `Documents.GetMeta` API calls support a `Subset` field for extracting specific parts of a document using subset expressions. This allows clients to request only the data they need rather than fetching entire documents.

## Observability

Prometheus metrics and PPROF debugging endpoints are exposed on port 1081 at "/metrics" and "/debug/pprof/".

The metrics cover API usage and most internal operations in the repository as well as Go runtime metrics.

The PPROF debugging endpoints allow for CPU and memory profiling to get to the bottom of performance issues and concurrency bugs.

## Calling the API

The API is defined in [service.proto](https://github.com/ttab/elephant-api/blob/main/repository/service.proto).

Authentication is OIDC-based, configured via the `--oidc-config`, `--jwt-audience`, `--jwt-scope-prefix`, `--client-id`, and `--client-secret` flags (or their corresponding environment variables).

### Fetching a document

``` shell
curl --request POST \
  --url http://localhost:1080/twirp/elephant.repository.Documents/Get \
  --header "Authorization: Bearer $TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{
	"uuid": "8090ff79-030e-419b-952e-12917cfdaaac"
}'
```

Here you can specify `version` to fetch a specific version, or `status` to fetch the version that last got f.ex. the "usable" status.

### Fetching document metadata

``` shell
curl --request POST \
  --url http://localhost:1080/twirp/elephant.repository.Documents/GetMeta \
  --header "Authorization: Bearer $TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{
	"uuid": "8090ff79-030e-419b-952e-12917cfdaaac"
}'
```

## Running locally

### Preparing the environment

Follow [the instructions](#the-database) to get the database up and running.

Then create a ".env" file containing the following values:

```
S3_ENDPOINT=http://localhost:9000/
S3_ACCESS_KEY_ID=minioadmin
S3_ACCESS_KEY_SECRET=minioadmin
```

The S3 variables are only needed for local development with MinIO. In production, standard AWS credential resolution is used.

###  Running the repository server

The repository server runs the API, archiver, and eventlog builder. If your environment has been set up correctly (env vars, postgres, and minio) you should be able to run it like this:

``` shell
go run ./cmd/repository run --no-eventsink
```

### Configuration reference

The server is configured via CLI flags and/or environment variables. Key options:

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--addr` | `ADDR` | `:1080` | API listen address |
| `--tls-addr` | `TLS_ADDR` | `:1443` | TLS listen address |
| `--cert-file` | `TLS_CERT_PATH` | | TLS certificate path |
| `--key-file` | `TLS_KEY_PATH` | | TLS key path |
| `--profile-addr` | `PROFILE_ADDR` | `:1081` | Metrics/pprof listen address |
| `--log-level` | `LOG_LEVEL` | `error` | Log level |
| `--default-language` | `DEFAULT_LANGUAGE` | `sv-se` | Default document language |
| `--default-timezone` | `DEFAULT_TIMEZONE` | `Europe/Stockholm` | Default timezone |
| `--db` | `CONN_STRING` | `postgres://elephant-repository:pass@localhost/elephant-repository` | PostgreSQL connection string |
| `--db-bouncer` | `BOUNCER_CONN_STRING` | | PgBouncer connection string (used for all DB ops except pubsub) |
| `--db-parameter` | `CONN_STRING_PARAMETER` | | Additional connection string parameter |
| `--s3-endpoint` | `S3_ENDPOINT` | | S3 endpoint override (for MinIO) |
| `--s3-key-id` | `S3_ACCESS_KEY_ID` | | S3 access key ID (for MinIO) |
| `--s3-key-secret` | `S3_ACCESS_KEY_SECRET` | | S3 access key secret (for MinIO) |
| `--archive-bucket` | `ARCHIVE_BUCKET` | `elephant-archive` | S3 bucket for archives |
| `--asset-bucket` | `ASSET_BUCKET` | `elephant-assets` | S3 bucket for assets |
| `--eventsink` | `EVENTSINK` | `aws-eventbridge` | Event sink type |
| `--cors-host` | `CORS_HOSTS` | | CORS hosts to allow (supports wildcards) |
| `--ensure-schema` | `ENSURE_SCHEMA` | | Schema specifications to ensure on startup |
| `--migrate-db` | `MIGRATE_DB` | `false` | Run database migrations on startup |
| `--no-eventsink` | `NO_EVENTSINK` | `false` | Disable event sink |
| `--no-archiver` | `NO_ARCHIVER` | `false` | Disable archiver |
| `--no-eventlog-builder` | `NO_EVENTLOG_BUILDER` | `false` | Disable eventlog builder |
| `--no-scheduler` | `NO_SCHEDULER` | `false` | Disable scheduled publishing |
| `--no-charcounter` | `NO_CHARCOUNTER` | `false` | Disable built-in character counter |
| `--no-websocket` | `NO_WEBSOCKET` | `false` | Disable WebSocket API |
| `--no-sse` | `NO_SSE` | `false` | Disable SSE API |
| `--no-core-schema` | `NO_CORE_SCHEMA` | `false` | Don't register built-in core schema |
| `--tolerate-eventlog-gaps` | `TOLERATE_EVENTLOG_GAPS` | `false` | Tolerate eventlog gaps when archiving |
| `--oidc-config` | `OIDC_CONFIG` | | OIDC configuration URL |
| `--jwt-audience` | `JWT_AUDIENCE` | | Expected JWT audience |
| `--jwt-scope-prefix` | `JWT_SCOPE_PREFIX` | | Prefix for JWT scopes |
| `--client-id` | `CLIENT_ID` | | OAuth client ID |
| `--client-secret` | `CLIENT_SECRET` | | OAuth client secret |

## The database

### Running and DB schema ops

The repository uses [mage](https://magefile.org/) as a task runner. Start a local postgres instance using the `mage sql:postgres pg16`. Create a database using `mage sql:db`.

The database schema is defined using numbered [tern](https://github.com/jackc/tern) migrations in "./schema/". Initialise the schema by running `mage sql:migrate`. Set the `CONN_STRING` environment variable to run the `mage sql:*` operations against a remote database.

Start a local minio instance and the necessary buckets using `mage s3:minio s3:bucket elephant-archive s3:bucket elephant-assets`.

Queries are defined in "./postgres/query.sql" and are compiled using [sqlc](https://sqlc.dev/) to a `Queries` struct in "./postgres/query.sql.go". Run `mage sql:generate` to compile queries.

Use `mage sql:rollback 0` to undo all migrations, to migrate to a specific version, f.ex. 7, use `mage sql:rollback 7`.

Connect to the local database using `psql $(mage sql:connString)` or `psql postgres://elephant-repository:pass@localhost/elephant-repository`.

### Introduction to the schema

Each document has a single row in the `document` table. New versions of the document get added to the `document_version` table, and `document(updated, updater_uri, current_version)` is updated at the same time. The same logic applies to `document_status` and `status_heads(id, updated, updater_uri)`. This relationship between the tables is formalised in the stored procedures `create_version` and `create_status`.

An update to a document always starts with getting a row lock on the `document(uuid)` table for the transaction. This gives us serialisation guarantees for writes to a single document, and lets us use straight-forward numbering for document versions and status updates.

### Data mining examples

#### Published article cause

`¤` is `NULL`, in other words it's the initial publication of an article.

``` sql
SELECT date(s.created), s.meta->>'cause' AS cause, COUNT(*) AS num
FROM document_status AS s
WHERE s.name='usable'
GROUP BY date(s.created), cause
ORDER BY date(s.created), cause NULLS FIRST;
```

```
    date    │    cause    │ num
════════════╪═════════════╪═════
 2023-02-07 │ ¤           │ 620
 2023-02-07 │ correction  │   4
 2023-02-07 │ development │  64
 2023-02-07 │ fix         │  10
 2023-02-08 │ ¤           │ 734
 2023-02-08 │ correction  │   3
 2023-02-08 │ development │  97
 2023-02-08 │ fix         │  14
 2023-02-09 │ ¤           │ 613
 2023-02-09 │ correction  │   5
 2023-02-09 │ development │  89
 2023-02-09 │ fix         │   8
 2023-02-10 │ ¤           │ 428
 2023-02-10 │ correction  │   2
 2023-02-10 │ development │  52
 2023-02-10 │ fix         │  12
(16 rows)
```

#### Time to correction after first publish

``` sql
SELECT s.uuid, i.created AS initially_published, s.created-i.created AS time_to_correction
FROM document_status AS s
     INNER JOIN document_status AS i
           ON i.uuid = s.uuid AND i.name = s.name AND i.id = 1
WHERE s.name='usable' AND s.meta->>'cause' = 'correction'
ORDER BY s.created;
```

```
                 uuid                 │  initially_published   │    time_to_correction
══════════════════════════════════════╪════════════════════════╪═══════════════════════════
 54123854-9303-4cc6-b98d-afa9b2656602 │ 2023-02-07 09:19:50+00 │ @ 11 mins 55 secs
 eedf4fe2-5b3a-4fa4-a2c8-cf2029ca268b │ 2023-02-07 09:20:58+00 │ @ 1 hour 59 mins 30 secs
 03d47f19-a4b5-4de5-b6e2-664d759683ec │ 2023-02-07 12:58:07+00 │ @ 4 mins 34 secs
 37041f9b-386b-47f5-a974-f054bb628292 │ 2023-02-07 13:10:55+00 │ @ 17 mins 5 secs
 f550fbce-6c8c-43cc-a31d-0cbdb464a681 │ 2023-02-08 05:15:02+00 │ @ 1 hour 13 mins 13 secs
 f550fbce-6c8c-43cc-a31d-0cbdb464a681 │ 2023-02-08 05:15:02+00 │ @ 3 hours 15 mins 2 secs
 6ee43615-2cb8-441a-9c0f-fb68a675e1f2 │ 2023-02-08 08:30:02+00 │ @ 3 mins 56 secs
 5d75600e-4d26-488e-bcd2-1c27bd05794f │ 2023-02-09 01:30:02+00 │ @ 1 hour 2 mins 31 secs
 629ddc10-47e0-46ae-b47d-6c9fbb3ad7e0 │ 2023-02-09 08:24:37+00 │ @ 1 hour 27 mins 13 secs
 44e6653b-8be7-4175-8e4c-0c24c132e774 │ 2023-02-09 10:36:31+00 │ @ 5 hours 9 mins 25 secs
 71b61828-510d-4a6b-a8fa-574101eb54f5 │ 2023-02-09 08:30:26+00 │ @ 9 hours 54 mins 52 secs
 be6c03f8-81d1-40dd-bbe1-9b0c727b39a8 │ 2023-02-09 09:54:13+00 │ @ 8 hours 40 mins 27 secs
 d6413696-d189-4ad0-9454-8f0681a3f541 │ 2023-02-10 05:00:02+00 │ @ 1 hour 32 mins 2 secs
(13 rows)
```

#### High newsvalue articles per section

``` sql
SELECT vs.section, vs.newsvalue, COUNT(*)
FROM (
     SELECT d.uuid, s.created,
            (jsonb_path_query_first(
                v.document_data,
                '$.meta[*] ? (@.type == "core/newsvalue").data'
            )->>'score')::int AS newsvalue,
            jsonb_path_query_first(
                v.document_data,
                '$.links[*] ? (@.rel == "subject" && @.type == "core/section")'
            )->>'title' AS section
     FROM document_status AS s
          INNER JOIN document AS d ON d.uuid = s.uuid
          INNER JOIN document_version AS v
                ON v.uuid = d.uuid
                   AND v.version = d.current_version
                   AND v.type = 'core/article'
     WHERE
        s.name='usable'
        AND s.id = 1
        AND date(s.created) = '2023-02-08'
) AS vs
WHERE vs.newsvalue <= 2 AND newsvalue > 0
GROUP BY vs.section, vs.newsvalue
ORDER BY vs.section, vs.newsvalue;
```

```
 section │ newsvalue │ count
═════════╪═══════════╪═══════
 Ekonomi │         1 │     2
 Ekonomi │         2 │     5
 Inrikes │         1 │     2
 Inrikes │         2 │    12
 Kultur  │         2 │     2
 Nöje    │         2 │     5
 Sport   │         1 │     4
 Sport   │         2 │     7
 Utrikes │         1 │     2
 Utrikes │         2 │     7
(10 rows)
```

### Archiving data

The repository has an archiving subsystem that records all eventlog events, and their associated document or status data, to a S3 compatible store.

All archived objects (events, statuses, and documents) contain the signature of their parent, so the entire archive forms a narrow [merkle tree](https://en.wikipedia.org/wiki/Merkle_tree). This would for example allow for the publication of a transparency log, with which a trusted third party could verify what was in the repository at any given time.

#### Signing

The repository maintains a set of ECDSA P-384 signing keys that are used to sign archived objects. The signature is an ASN1 signature of the sha256 hash of the marshalled data of the archive object. The format of a signature string looks like this:

```
v1.[key ID].[sha256 hash as raw URL base64].[signature as raw URL base64]
```

The signature is set as the metadata header "X-Amz-Meta-Elephant-Signature" on the S3 object itself. After the object has been archived the corresponding database row is updated with the signature of the archived object.

Event, status and document version archive objects contain the signature of their parents to create a signature chain that can be verified. That means that everything that gets written can be verified against the log.

The reason that signing has to be done during archiving is that the jsonb data type isn't guaranteed to be byte stable. Verifying signatures on the archive objects is straightforward, verifying signatures for the database would have to be done by verifying the signature for the archive signature, and then verifying that the data in the database is "logically" equivalent to the archive data.

Signing keys are used for 180 days. A new signing key will be created 7 days before the current key expires, with a 2-day heads-up period before it is taken into use.

#### Fetching signing keys

The `GET /signing-keys` endpoint returns the archive signing public keys as a JWKS document. This is a public, unauthenticated endpoint. The response contains ECDSA P-384 public keys that can be used to verify archive signatures. Each key includes `iat` (issued at), `nbf` (not before), and `exp` (not after) as unix timestamps.

```
curl http://localhost:1080/signing-keys
```

Example response:
```json
{
  "keys": [
    {
      "kid": "1",
      "kty": "EC",
      "crv": "P-384",
      "x": "...",
      "y": "...",
      "iat": 1700000000,
      "nbf": 1700000000,
      "exp": 1715552000
    }
  ]
}
```

Signing keys are also written to the S3 archive bucket under `signing-keys/{kid}.json` as individual JWK objects (the same format as a single entry in the endpoint response above). The archiver writes each key when it's created and catches up on any unarchived keys on startup.

Note that anyone who wants to independently validate the archive should store the signing keys in a location they control. The archived keys are provided as a convenience, but relying solely on keys stored alongside the data they sign doesn't provide independent verification — an attacker who can modify the archive could also modify the keys.

#### Deletes

Archiving is used to support the delete functionality. A delete request will acquire a row lock for the document, and then wait for its versions and statuses to be fully archived. It then creates a delete_record with information about the delete, and deletes the document row to replace it with a system_state `deleting` placeholder. From the clients' standpoint the delete is now finished. But no reads of, or updates to the document are allowed until the delete has been finalised by an archiver. The reason that the archiver is responsible for finalising the delete is that we then can ensure that the database and S3 archive are consistent. Otherwise we would be forced to manage error handling and consistency across a db transaction and the object store.

The archiver looks for documents with pending deletes and then moves the objects from the "documents/[uuid]" prefix to a "deleted/[uuid]/[delete record id]" prefix in the bucket. Once the move is complete the document row is deleted, and the only thing that remains is the delete_record and the archived objects.

#### Restoring documents

When a restore is initiated a system locked document row is created in documents (system_state == "restoring"). This is not reflected in the eventlog, but all the restored document versions and status updates will be, and when the restore is finished a "restore_finished" will be emitted. All event log events that result from a restore will have "system_state" set to "restoring" so that they can be ignored by event processors.

#### Purging documents

When a document has been deleted the archived information associated with it can be purged. This will remove all objects in S3 and clear information about status heads, ACLs, and the document version from the delete record. The information that will remain is:

* UUID and URI of the document
* Type of the document
* The time the document was deleted and who deleted it
* The time the document was purged

So what remains is the bare-bones information that something existed, and an audit trail related to its removal.
