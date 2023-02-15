# Revisor

Revisor allows you to define specifications for Elephant document contents as a
series of declarations and pattern matching extensions to existing declarations.

*TODO: NB. the README is not up to date with the changes for Elephant*

## Local testing

For running the actual tests and benchmarks, see the section on [Testing](#markdown-header-testing).

The easiest way to test specifications against documents is by running the "revisor" command like so:

``` bash
$ revisor document ./testdata/article-borked.json
```

That will validate the document using only the specifications in "./constraints/core.json".

Try running the same validation against a document with customer-specific content:

``` bash
$ revisor document ./testdata/example-article.json 
affd4b2e-9709-45a7-a668-3abea1e1eac6 x-im/article ==>
link 2 resourceType(x-im/resourceType): undeclared block type or rel
link 3 template(x-im/template): undeclared block type or rel
link 4 commentingVisible(x-im/commentingVisible): undeclared block type or rel
link 5 commentingMode(x-im/commentingMode): undeclared block type or rel
meta block 1 (x-smf-sndp/tag): undeclared block type or rel
data attribute "selectedTimestamp" of meta block 1 (x-smf-sndp/tag): unknown attribute
data attribute "title" of meta block 1 (x-smf-sndp/tag): unknown attribute
data attribute "contentRepoId" of meta block 1 (x-smf-sndp/tag): unknown attribute
data attribute "resourceSubtype" of meta block 1 (x-smf-sndp/tag): unknown attribute
property "x-smf/newsSource": undeclared property
property "x-smf/originalPublishingChannel": undeclared property
property "x-smf/originalPublishingChannelName": undeclared property
documents had validation errors
```

Use the flag `-spec ./constraints/example.json` to load the specifications from "./constraints/example.json" as well.

### Running a revisor server

It's also possible to run revisor as a service with the `serve` command, it takes the same `--spec`/`--naviga-spec` as the `document` command, and adds `--addr` to control the address to listen to.

Start the server in one shell:

``` bash
$ revisor serve
```

...and post the example article to it in another using `curl`:

``` bash
$ curl --data @testdata/example-article.json localhost:8000
```

You should get the same validation errors as in the previous example, but in JSON format. An empty array is returned for valid documents.

## Writing specifications

The main entities points in a specification are documents, blocks and properties. Documents are declared by type, blocks by type and/or rel, and properties by name. An entity is not valid if we don't have a matching declaration for it, regardless of whether somebody has pattern-matched against it.

Both pattern matching and a lot of the validation that's performed is done though key value pairs of a name and a string constraint. Say that we want to match all links that have a rel of "subject", "channel", or "section" and add the ability to have "broader" links added to them, the specification would then look like this:

``` json
{
  "name": "Associated with and broader links",
  "description": "Extends subject, channel, and section links with broader links",
  "match": {"rel": {
    "enum": ["subject", "channel", "section"]
  }},
  "links": [
    {
      "declares": {"rel":"broader"},
      "attributes": {
        "type": {},
        "title": {}
      }
    }
  ]
}
```

Here we declare that links with `rel` "broader" are valid for all blocks that matches our expression, see "Block attributes" for a list of attributes that can be used in pattern matching. We also define that the attributes `type` and `title` must be present. The `{"enum":...}` object and the empty objects (`{}`) for `type` and `title` are all examples of string constraints.

### String constraints

| Name       | Use                                                                                     |
|:-----------|:----------------------------------------------------------------------------------------|
| optional   | Set to `true` if the value doesn't have to be present                                   |
| allowEmpty | Set to `true` if an empty value is ok                                                   |
| const      | A specific `"value"` that must match                                                    |
| enum       | A list `["of", "values"]` where one must match                                          |
| pattern    | A regular expression that the value must match                                          |
| glob       | A list of glob patterns `["http://**", "https://**"]` where one must match              |
| template   | A Go template expression that the value must match, not available for pattern matching. |
| format     | A named format that the value must follow                                               |
| time       | A time format specification                                                             |

#### Formats

The following formats are available:

* `RFC3339`: an RFC3339 timestamp ("2022-05-11T14:10:32Z")
* `int`: an integer ("1234")
* `float`: a floating point number ("12.34")
* `bool`: a boolean ("true" or "false")
* `html`: validate the contents as HTML

When using the format "html" it's also possible to use `htmlPolicy` to use a specific HTML policy. See the section on [HTML policies](#markdown-header-html-policies).

#### Time formats

A Go time parsing layout (see the [time package](https://pkg.go.dev/time#pkg-constants) for documentation) that should be used to validate the timestamp.

#### Globs

Glob matching uses [https://github.com/gobwas/glob](https://github.com/gobwas/glob) for matching, and the glob patterns are compiled with "/" and "+" as separators.

#### Templates

The template expressions have the following values available:

* **Blocks**: the current block as `this`, and if the block has a parent block that is available as `parent`.

* **Documents**: the current document as `this`.

* **Properties**: the current property as `this`, and the document as `parent`.

Templating cannot be used for pattern matching, just for validation.

### Writing a document specification

A specification for a document contains:

* documentation attributes `name` and `description`
* a declaration (`declares`) or pattern matching rule (`match`) 
* attribute constraints (`attributes`)
* `meta`, `links`, and `content` block specifications
* `properties` specifications

``` json
{
  "name": "Planning item",
  "description": "Planned news coverage",
  "declares": "x-im/newscoverage",
  "meta": [
    {
      "name": "Main metadata block",
      "declares": {"type":"x-im/newscoverage"},
      "count": 1,
      "data": {
        "dateGranularity": {"enum":["date", "datetime"]},
        "description": {"allowEmpty":true},
        "start": {"format":"RFC3339"},
        "end": {"format":"RFC3339"},
        "priority": {},
        "publicDescription":{"allowEmpty":true},
        "slug": {"allowEmpty":true}
      }
    }
  ],
  "links": [
    {
      "declares": {"type": "x-im/assignment"},
      "links": [
        {
          "declares": {
            "rel":"assignment", "type": "x-im/assignment"
          },
          "attributes": {
            "uuid": {}
          }
        }
      ]
    }
  ],
  "properties": {
    "nrp:sector": {
      "value": {},
      "maxCount": 1,
      "parameters": {
        "literal": {}
      }
    }
  }
}
```

### Writing a block specification

A block specification can contain:

* documentation attributes `name` and `description`
* a declaration (`declares`) or pattern matching rule (`match`) 
* attribute constraints (`attributes`)
* `data` constraints
* `meta`, `links`, and `content` block specifications
* `properties` specifications
* `count`, `minCount` and `maxCount` to control how many times a block can occur in the list of blocks it's in
* `blocksFrom` directives that borrows the allowed blocks from a declared document type.

``` json
{
  "declares": {"type": "x-im/socialembed"},
  "links": [
    {
      "declares": {"rel":"self", "type":"x-im/tweet"},
      "maxCount": 1,
      "attributes": {
        "uri": {"glob":["im://tweet/*"]},
        "url": {"glob":["https://twitter.com/narendramodi/status/*"]}
      }
    },
    {
      "declares": {"rel":"alternate", "type":"text/html"},
      "maxCount": 1,
      "attributes": {
        "url": {"glob":["https://**"]},
        "title": {}
      },
      "data": {
        "context": {},
        "provider": {}
      }
    }
  ]
}
```

### Writing a property specification

* a documentation attribute `description`
* a value string constraint
* `data` string constraints
* parameter string constraints (`parameters`)
* `count`, `minCount` and `maxCount` to control how many times a block can occur in the list of blocks it's in

``` json
        "imext:haspublishedversion": {
          "description": "Is set to true if the document has been published previously",
          "value": {"format":"bool"},
          "count": 1
        },
        "infoSource": {
          "parameters": {
            "literal": {}
          },
          "maxCount": 1
        }
```

### HTML policies

HTML policies are used to restrict what elements and attributes can be used in strings with the format "html". Attributes are defined as string constraints on elements. The default policy could look like this:

``` json
  "htmlPolicies": [
    {
      "name": "default",
      "elements": {
        "strong": {
          "attributes": {
            "id": {"optional":true}
          }
        },
        "a": {
          "attributes": {
            "id": {"optional":true},
            "href": {}
          }
        }
      }
    },
    {
      "name": "table",
      "uses": "default",
      "elements": {
        "tr": {
          "attributes": {
            "id": {"optional":true}
          }
        },
        "td": {
          "attributes": {
            "id": {"optional":true}
          }
        },
        "th": {
          "attributes": {
            "id": {"optional":true}
          }
        }
      }
    }
  ]
```

All "html" strings that use the default policy would then be able to use `<strong>` and `<a>`, and the "href" attribute would be requred for `<a>`. A "html" string that uses the "table" policy would be able to use everything from the default policy *and* `<tr>`, `<td>`, and `<th>`.

A customer can extend HTML policies using the "extend" attribute:

``` json
  "htmlPolicies": [
    {
      "extends": "default",
      "elements": {
        "personTag": {
          "attributes": {
            "id": {}
          }
        }
      }
    }
  ]
```

This would add support for "<persontag>/<personTag>" (HTML is case insensitive) to the default policy, and any policies that use it. Only one level of "extends" and "uses" is allowed, further chaining policies will result in an error.

### Attribute reference

#### Document attributes

A list of available document attributes, and whether they can be used in pattern matching.

| Name     | Description                                    | Match |
|:---------|------------------------------------------------|:------|
| uuid     | The document uuid                              | No    |
| uri      | The URI that identifies the document           | No    |
| url      | A web-browsable location for the document      | No    |
| type     | The type of the document                       | Yes   |
| subtype  | The document sub-type                          | Yes   |
| language | The document language                          | No    |
| status   | Publication status                             | No    |
| title    | The document title                             | No    |
| provider | The system or entity that provided the content | Yes   |

#### Block attributes

A list of available block attributes, and whether they can be used in pattern matching.

| Name        | Description                                               | Match |
|:------------|:----------------------------------------------------------|:------|
| uuid        | The UUID of the document the block represents             | No    |
| type        | The type of the block                                     | Yes   |
| uri         | Identifies a resource in in URI form                      | Yes   |
| url         | A web-browsable location for the block                    | Yes   |
| title       | Human readable title of the block                         | No    |
| rel         | The relationship the block describes                      | Yes   |
| name        | A name that identifies the block                          | Yes   |
| value       | A generic value for the block                             | Yes   |
| contenttype | The content type of the resource that the block describes | Yes   |
| role        | The role that the block or resource has                   | Yes   |

## Testing

Revisor implements a file-driven test in `TestValidateDocument` that checks so that all the "testdata/results/*.json" files match the validation results for the corresponding document under "testdata/". Result files with the prefix "base-" will be validated against "constraints/naviga.json", for result files with the prefix "example-" the "constraints/example.json" constraints will be used as well.

If the constraints have been updated, or new example documents have been added, the result files can be regenerated using `./update-test-results.sh`.

### Benchmarks

The benchmark `BenchmarkValidateDocument` tests the performance of validating "testdata/example-article.json" against the naviga and example organisation contsraint sets.

To run the benchmark execute:

``` bash
$ go test -bench . -benchmem -cpu 1
```

Add the flags `-memprofile memprofile.out -cpuprofile profile.out` to collect CPU and memory profiles. Run `go tool pprof -web profile.out` for the respective profile files to open a profile graph in your web browser.

#### Comparing benchmarks

Install `benchstat`: `go install golang.org/x/perf/cmd/benchstat@latest`.

Run the benchmark on the unchanged code (stash your changes or check out main):

``` bash
$ go test -bench . -benchmem -count 5 -cpu 1 | tee old.txt
```

Then run the benchmarks on the new code:

``` bash
$ go test -bench . -benchmem -count 5 -cpu 1 | tee old.txt
```

Finally, run benchstat to get a summary of the change:

``` bash
$ benchstat old.txt new.txt
name              old time/op    new time/op    delta
ValidateDocument     203µs ± 7%      99µs ± 3%  -51.03%  (p=0.008 n=5+5)

name              old alloc/op   new alloc/op   delta
ValidateDocument     134kB ± 0%      35kB ± 0%  -73.74%  (p=0.008 n=5+5)

name              old allocs/op  new allocs/op  delta
ValidateDocument     1.05k ± 0%     0.59k ± 0%  -43.48%  (p=0.008 n=5+5)
```

### Fuzz tests

There are two fuzz targets in the project: `FuzzValidationWide` that allows fuzzing of the document and two constraint sets. It will load the naviga constraints, the example organisation constraints, and all documents in "./testdata/" and add them as fuzzing seeds. `FuzzValidationConstraints` adds all constraint sets from the "./constraints/" and adds them as fuzzing seeds. The fuzzing operation is then done against all documents in "./testdata/".
