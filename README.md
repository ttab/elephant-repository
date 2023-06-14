# Elephant repository

![Image](docs/elephant.png?raw=true)

Elephant repository is a [NewsDoc](https://github.com/ttab/newsdoc) document repository with versioning, ACLs for permissions, archiving, validation schemas, workflow statuses, reporting support, event output, and metrics for observability.

The repository depends on PostgreSQL for data storage and a S3 compatible store for archiving and reports. It can use AWS EventBridge as an event sink, but that is optional and can be disabled with `--no-eventsink`.

All operations against the repository is exposed as a [Twirp RPC API](https://twitchtv.github.io/twirp/docs/intro.html) that you can communicate either using [Protobuf](https://protobuf.dev/) messages or standard JSON. See [Calling the API](#calling-the-api) for more details on communicating with the defined services.

N.B. Until we reach v1.0.0 this should be seen as a tech preview of work in progress without any stability guarantees, and it is not in ANY way production ready.

## Versioning

All updates to a document are stored as sequentially numbered versions with information about when it was created, who created it, and optional metadata for the version.

Old versions can always be fetched through the API, and the history can be inspected through `Documents.GetHistory`.

## ACLs for permissions

Documents can be shared with individuals or units (groups of people). By default only the entity that created a document has access to it, and other entities will have to be granted read and/or write access.

In most workflows documents will be shared with a group of people, but this makes it possible to work with private drafts, and share documents with individuals that are untrusted in the sense that they shouldn't have access to all your content.

## Archiving

All document statuses and versions are archived by a background process in the repository. Archiving is tightly integrated with the document lifecycle and a document cannot be deleted until it has been fully archived.

As part of the archiving process the archive objects are signed with an archiving key, and as the signature of the previous version is included in the object we create a tamper-proof chain of updates. Statuses also include the signature of the document version they refer to.

The archive status and signature are fed back into the database after the object has been successfully archived.

See [Archiving data](#archiving-data) for further details.

## Validation schemas

All document types need to be declared before they can be stored in the repository. This serves two purposes, of which the primary is to maintain data quality, the other purpose is to inform automated systems about the shape of your data. This is leveraged by the [elephant-index](https://github.com/ttab/elephant-index) to create correct mappings for OpenSearch/ElasticSearch.

Schema management is handled through the `Schemas` service. For details on how to write specifications, see [revisor "Writing specifications"](https://github.com/ttab/revisor#writing-specifications).

## Workflow statuses

You can define and set statuses for document versions. To publish a version of a document you would typically set the status "usable" for it. Your publishing pipeline would then pick up that status event and act on it. New document versions that are created don't affect the "usable" status you set, to publish a new version you would have to create a new "usable" status update that references that version.

The last status of a given name for a document is referred to as the "head". Just like documents, statuses are versioned and have sequential IDs for a given document and status name.

TODO: write and link to workflow rule documentation, see presentation.slide

## Event output

All changes to a document are emitted on the eventlog, accessed through `Documents.Eventlog`. Changes can be:

* a new document version
* anew document status
* updated ACL entries
* a document delete

This eventlog can be used by other applications to act on changes in the repository.

### Event sink

The repository also has the concept of event sinks where enriched events can be posted to an event sink (right now only we only support AWS EventBridge as a sink).

The purpose of the enriched events is to allow the constructions of event-based architectures where f.ex. a Lambda function could subscribe to published articles with a specific category. This let's you avoid situations where a lot of systems load unnecessary just to determine if an event should be handled.

TODO: link to more documentation of enriched format.

## Reporting support

The repository allows for scheduling of automatic reports. Reports are defined using SQL and are run with decreased privileges that has read-only access to select tables in the internal database.

The results of the report run are written to a reporting S3 bucket where other systems can pick them up and post to f.ex. Slack.

## Metrics

Prometheus metrics and PPROF debugging endpoints are exposed on port 1081 at "/metrics" and "/debug/pprof/".

The metrics cover API usage and most internal operations in the repository as well as Go runtime metrics.

The PPROF debugging endpoints allow for CPU and memory profiling to get to the bottom of performance issues and concurrency bugs.

## Calling the API

The API is defined in [service.proto](https://github.com/ttab/elephant-api/blob/main/repository/service.proto).

### Retrieving a token

The API service has an endpoint for fetching dummy tokens for use with the API.

``` shell
curl http://localhost:1080/token \
    -d grant_type=password \
    -d 'username=Hugo Wetterberg <user://tt/hugo, unit://tt/unit/a, unit://tt/unit/b>' \
    -d 'scope=doc_read doc_write doc_delete'
```

This will yeild a JWT with the following claims:

``` json
{
  "iss": "test",
  "sub": "user://tt/hugo",
  "exp": 1675894185,
  "sub_name": "Hugo Wetterberg",
  "scope": "doc_read doc_write doc_delete",
  "units": [
    "unit://tt/unit/a",
    "unit://tt/unit/b"
  ]
}
```

It's essentially a password-less password grant where you can specify your own permissions and identity.

Example scripting usage:

``` shell
TOKEN=$(curl -s http://localhost:1080/token \
    -d grant_type=password \
    -d 'username=Hugo Wetterberg <user://tt/hugo, unit://tt/unit/a, unit://tt/unit/b>' \
    -d 'scope=doc_read doc_write doc_delete' | jq -r .access_token)

curl --request POST \
  --url http://localhost:1080/twirp/elephant.repository.Documents/Get \
  --header "Authorization: Bearer $TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{
        "uuid": "23ba8778-36c2-417b-abc7-323db47a7472"
}'
```

### Fetching a document

``` shell
curl --request POST \
  --url http://localhost:1080/twirp/elephant.repository.Documents/Get \
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
JWT_SIGNING_KEY='MIGkAgEBBDAgdjcifmVXiJoQh7IbTnsCS81CxYHQ1r6ftXE6ykJDz1SoQJEB6LppaCLpNBJhGNugBwYFK4EEACKhZANiAAS4LqvuFUwFXUNpCPTtgeMy61hE-Pdm57OVzTaVKUz7GzzPKNoGbcTllPGDg7nzXIga9ObRNs8ytSLQMOWIO8xJW35Xko4kwPR_CVsTS5oMaoYnBCOZYEO2NXND7gU7GoM'
```

I load this environment file using `export $(cat .env | xargs)`.

The server will generate and a JWT signing key (and log a warning) if it's missing from the environment.

###  Running the repository server

The repository server runs the API, archiver, and replicator. If your environment has been set up correctly (env vars, postgres, and minio) you should be able to run it like this:

``` shell
go run ./cmd/repository run
```

## The database

### Running and DB schema ops

Start a local database using the `./run-postgres.sh` script. It will create and/or start a container and launch a psql console. If the container is running it will just launch psql. Exiting psql will not stop the container.

The database schema is defined using numbered [tern](https://github.com/jackc/tern) migrations in "./schema/". The database can either be initialised by running `make db-migrate` or loading "./postgres/schema.sql" and "./postgres/schema_version.sql".

Queries are defined in "./postgres/query.sql" and are compiled using [sqlc](https://sqlc.dev/) to a `Queries` struct in "./postgres/query.go". Run `make generate-sql` to compile queries.

Use `make db-rollback` to undo all migrations, set `rollback_to` to migrate to a specific version `rollback_to=1 make db-rollback`.

When you run `make db-migrate` tern will run migrations, and then `./dump-postgres-schema.sh` is run to write the final schema to "./postgres/schema.sql" and the current version of the schema to "./postgres/schema_version.sql". "schema.sql" is used by `sqlc` for schema and query compilation and type checking.

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

### Change data capture

As part of the schema the `eventlog` [publication](https://www.postgresql.org/docs/current/sql-createpublication.html) is created, and it captures changes for the tables `document`, `status_heads`, `delete_record` and `acl`. See `PGReplication` in "./eventlog.go" for the beginnings of an implementation.

As we only want one process to consume the replication updates the CDC process starts with a request to acquire an [advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS) for the transaction using [pg_advisory_xact_lock](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS) which means that it will block until the lock is acquired, or the request fails.

A [logical](https://www.postgresql.org/docs/15/logicaldecoding.html) [replication slot](https://www.postgresql.org/docs/15/protocol-replication.html) will be created if it doesn't already exist, using [pglogrepl](https://github.com/jackc/pglogrepl).

TODO: Currently the implementation just logs the events, but the plan is for it to create an event payload, store it in an eventlog table, and potentially send a [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) thin event that tells any waiting subsystems that there is a new event to consume.

### Archiving data

The repository has an archiving subsystem that records all document changes (versions and statuses) to a S3 compatible store. TODO: We will use this fact to be able to purge document data from old versions in the database.

#### Signing

The repository maintains a set of ECDSA P-384 signing keys that are used to sign
archived objects. The signature is an ASN1 signature of the sha256 hash of the
marshalled data of the archive object. The format of a signature string looks
like this:

```
v1.[key ID].[sha256 hash as raw URL base64].[signature as raw URL base64]
```

The signature is set as the metadata header "X-Amz-Meta-Elephant-Signature" on
the S3 object itself. After the object has been archived the database row is
updated with `archived=true` and the `signature`.

Status and version archive objects contain the signature of their parents to
create a signature chain that can be verified.

The reason that signing has to be done during archiving is that the jsonb data type isn't guaranteed to be byte stable. Verifying signatures on the archive objects is straightforward, verifying signatures for the database would have to be done by verifying the signature for the archive signature, and then verifying that the data in the database is "logically" equivalent to the archive data.

Signing keys are used for 180 days, a new signing key will be created and published 30 days before it's taken into use.

#### Fetching signing keys

TODO: Not implemented yet, the idea is to borrow heavily from JWKS and to that end the internal `SigningKey` data struct is based on a JWK key spec.

#### Deletes

Archiving is used to support the delete functionality. A delete request will acquire a row lock for the document, and then wait for its versions and statuses to be fully archived. It then creates a delete_record with information about the delete, and deletes the document row to replace it with a `deleting` placeholder. From the clients' standpoint the delete is now finished. But no reads of, or updates to the document are allowed until the delete has been finalised by an archiver. The reason that the archiver is responsible for finalising the delete is that we then can ensure that the database and S3 archive are consistent. Otherwise we would be forced to manage error handling and consistency across a db transaction and the object store.

The archiver looks for documents with pending deletes and then moves the objects from the "documents/[uuid]" prefix to a "deleted/[uuid]/[delete record id]" prefix in the bucket. Once the move is complete the document row is deleted, and the only thing that remains is the delete_record and the archived objects.

