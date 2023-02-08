# Docformat

Local application meant for exploring DAWROC data format conversion.

Executable entrypoints in cmd/docformat and cmd/repository.

TODO: UI is in a broken state at the moment, prioritised getting Postgres store right.

## Preparing the environment

Follow [the instructions](#the-database) to get the database up and running. Then start minio as well, and log into http://localhost:9001/ using minioadmin/minioadmin. Go to "Access Keys" and create an access key for use in the ".env" file.

Create a ".env" file containing the following values:

```
S3_ENDPOINT=http://localhost:9000/
S3_ACCESS_KEY_ID=[your access key]
S3_ACCESS_KEY_SECRET=[your access key secret]
JWT_SIGNING_KEY='MIGkAgEBBDAgdjcifmVXiJoQh7IbTnsCS81CxYHQ1r6ftXE6ykJDz1SoQJEB6LppaCLpNBJhGNugBwYFK4EEACKhZANiAAS4LqvuFUwFXUNpCPTtgeMy61hE-Pdm57OVzTaVKUz7GzzPKNoGbcTllPGDg7nzXIga9ObRNs8ytSLQMOWIO8xJW35Xko4kwPR_CVsTS5oMaoYnBCOZYEO2NXND7gU7GoM'
```

I load this environment file using `export $(cat .env | xargs)`.

The server will generate and log a JWT signing key if it's missing from the environment.

## Running ingester

Set `NAVIGA_BEARER_TOKEN` to the value of the "dev-imidToken" (your session cookie when logged in to [Dashboard](https://tt.stage.dashboard.infomaker.io/)) cookie and start ingest like so:

``` shell
export NAVIGA_BEARER_TOKEN=[your cookie value]
go run ./cmd/docformat ingest --state-dir ../localstate/docformat.data
```

Pass in `--start-pos=-5000` to start from the last 5000 events, or an exact event number to start after.

Pass in `--ui` to start a web UI on ":1080", pass in f.ex. `--addr 1026.30.32:8080` if you want to start the listener on a different port or interface.

## Running the repository server

The repository server runs the API, archiver, and replicator. If your environment has been set up correctly (env vars, postgres, and minio) you should be able to run it like this:

``` shell
go run ./cmd/repository run
```

### Running ingester against production

Use the imidToken cookie instead, and set

```
--cca-url="https://cca-eu-west-1.saas-prod.infomaker.io"
--oc-url="https://xlibris.editorial.prod.oc.tt.infomaker.io:7777"
```

Make sure to use a different state dir for the environment.

## Calling the API

The API is defined in [service.proto](rpc/repository/service.proto).

Run `make proto` to re-generate code based on the protobuf declaration. This will run in a local docker image (to avoid a dep on local protoc), so it'll take some time to build the first time, but should be quick after that.

### Fetching a document

``` shell
curl --request POST \
  --url http://localhost:1337/twirp/elephant.repository.Documents/Get \
  --header 'Content-Type: application/json' \
  --data '{
	"uuid": "8090ff79-030e-419b-952e-12917cfdaaac"
}'
```

Here you can specify `version` to fetch a specific version, or `status` to fetch the version that last got f.ex. the "usable" status.

### Fetching document metadata

``` shell
curl --request POST \
  --url http://localhost:1337/twirp/elephant.repository.Documents/GetMeta \
  --header 'Content-Type: application/json' \
  --data '{
	"uuid": "8090ff79-030e-419b-952e-12917cfdaaac"
}'
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

### Change data capture

As part of the schema the `eventlog` [publication](https://www.postgresql.org/docs/current/sql-createpublication.html) is created, and it captures changes for the tables `document`, `status_heads`, `delete_record` and `acl`. See `PGReplication` in "./eventlog.go" for the beginnings of an implementation.

As we only want one process to consume the replication updates the CDC process starts with a request to acquire an [advisory lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS) for the transaction using [pg_advisory_xact_lock](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS) which means that it will block until the lock is acquired, or the request fails.

A [logical](https://www.postgresql.org/docs/15/logicaldecoding.html) [replication slot](https://www.postgresql.org/docs/15/protocol-replication.html) will be created if it doesn't already exist, using [pglogrepl](https://github.com/jackc/pglogrepl).

TODO: Currently the implementation just logs the events, but the plan is for it to create an event payload, store it in an eventlog table, and potentially send a [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) thin event that tells any waiting subsystems that there is a new event to consume.

### Archiving data

The repository has an archiving subsystem that records all document changes (versions and statuses) to a S3 compatible store. TODO: We will use this fact to be able to purge document data from old versions in the database.

TODO: The archiver doesn't use any form of concurrency at the moment, so expect it to start lagging behind if the system is put on a write load above 1 op/s.

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

#### TODO: Restoring from archive

## Some starting points

* `constraints/*.json`: format constraint specifications
* `convert.go`: navigadoc to document conversion
* `ingest.go`: ingestion implementation
* `planning.go`: parsing of NewsML planning items
* `concepts.go`: concept post-processing to get rid of properties.
* `fsdocstore.go`: implementation of the on-disk document store.
* `pgdocstore.go`: implementation of the PostgreSQL document store.

## Common errors

If a block/property et.c. isn't handled by the ingester you will be faced with an error like this:

```
failed to handle event 14429185: failed to ingest "4e2d5ff8-c65b-4293-a9a7-59141aa1ee86": failed to ingest version 1: failed to convert source doc: failed to convert document: failed to convert link blocks: no processor for block 5: unknown block type "x-im/channel,rel=channel,role="
```

Check the source document in cache or CCA/OC and update the relevant code in "convert.go", in this case it was this that was needed:

``` diff
@@ -128,6 +128,7 @@ func LinkBlockProcessors() map[string]BlockProcessor {
 		"x-im/event":         BlockProcessorFunc(convertIMBlockToCore),
 		"x-im/organisation":  BlockProcessorFunc(convertIMBlockToCore),
 		"x-im/article":       BlockProcessorFunc(convertIMBlockToCore),
+		"x-im/channel":       BlockProcessorFunc(convertIMBlockToCore),
 		"x-im/assignment":    BlockProcessorFunc(fixAssignmentLink),
 		"x-im/group":         BlockProcessorFunc(convertIMBlockToCore),
 		"x-im/articlesource": BlockProcessorFunc(convertArticleSource),
```

If the document fails validation you will get an error like this:

```
- link 3 channel(core/channel): undeclared block type or rel
- attribute "uuid" of link 3 channel(core/channel): undeclared block attribute
- attribute "type" of link 3 channel(core/channel): undeclared block attribute
- attribute "title" of link 3 channel(core/channel): undeclared block attribute
- attribute "rel" of link 3 channel(core/channel): undeclared block attribute
failed to handle event 14429185: failed to ingest "4e2d5ff8-c65b-4293-a9a7-59141aa1ee86": failed to ingest version 1: document has 5 validation errors : link 3 channel(core/channel): undeclared block type or rel
```

The document data is dumped to "invalid_doc.json" in the state dir for easy inspection.

In this case a channel link would have to be declared for articles in "constraints/core.json":

``` diff
@@ -403,6 +403,13 @@
             "title": {}
           }
         },
+        {
+          "declares": {"rel":"channel", "type": "core/channel"},
+          "attributes": {
+            "uuid": {},
+            "title": {}
+          }
+        },
         {
           "name": "Premium",
           "declares": {"type": "core/premium", "rel":"premium"},
```

## Storage

All state that resulted from ingest is stored in the "data" folder. If you want to restart the ingest process from scratch that's what whould be deleted.

Cached data is stored in "cache", and should be preserved if you restart the process, all cached data is keyed on uuid, version and any variables in the request, and is assumed to be immutable. 

Automatically detected **bad** documents are added to "blocklist.txt" in the format "[uuid] [Error description]\n". If a document blocks ingestion you can add it manually and re-start ingestion (without a `--start-pos` flag).

If a document fails validation it will be dumped to "invalid_doc.json".

### data/index.bleve

Local search index

### data/state.db

Local key-value store database that tracks things like the log position, replaces-relationships, and last known OC versions of documents. 

### cache/documents

Cached documents from CCA

### cache/properties

Cached OC property lookups organised by:

```
- cache/properties
  - [uuid]
    - [version]-[prop name hash].json
```

## Notes

There is an actual TT author concept, though its metadata is a bit borked: 0463ee71-572f-5185-bedc-62306d7c7ca8

There's data in the NewML document that doesn't make it into NavigaDoc:

``` xml
<!-- From article -->
<link rel="texttype" title="Till red" type="x-tt/texttype" uri="tt://texttype/message"/>
<!-- ... -->
<itemMetaExtProperty literal="Artikel" type="ttext:typ"/>
<itemMetaExtProperty literal="INFO" type="profil"/>
```

...might have to fall back to full NewsML parsing in the end, but CCA was a convenient shortcut.

## License

Uses code and definitions from [NavigaDoc](https://github.com/navigacontentlab/navigadoc), as reflected in the LICENSE file.
