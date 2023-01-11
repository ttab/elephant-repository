# Docformat

Local application meant for exploring DAWROC data format conversion.

Executable entrypoint in cmd/docformat.

## Running

Set `NAVIGA_BEARER_TOKEN` to the value of the "dev-imidToken" (your session cookie when logged in to [Dashboard](https://tt.stage.dashboard.infomaker.io/)) cookie and start ingest using:

``` shell
go run ./cmd/docformat ingest --state-dir ../docformat.data
```

Pass in `--start-pos=-5000` to start from the last 5000 events, or an exact event number to start after.

Pass in `--ui` to start a web UI on ":1080", pass in f.ex. `--addr 1026.30.32:8080` if you want to start the listener on a different port or interface.

### Running against production

Use the imidToken cookie instead, and set

```
--cca-url="https://cca-eu-west-1.saas-prod.infomaker.io"
--oc-url="https://xlibris.editorial.prod.oc.tt.infomaker.io:7777"
```

Make sure to use a different state dir for the environment.

## Some starting points

* `constraints/*.json`: format constraint specifications
* `convert.go`: navigadoc to document conversion
* `ingest.go`: ingestion implementation
* `planning.go`: parsing of NewsML planning items
* `concepts.go`: concept post-processing to get rid of properties.
* `fsdocstore.go`: implementation of the on-disk document store.

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

### data/documents

Document data organised by:

```
- data/documents
  - [uuid]
    - meta.json # Document metadata (versions, statuses, et.c.)
    - N.json    # the document data for a specific versions
```

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
