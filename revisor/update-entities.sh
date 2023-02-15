#!/usr/bin/env bash

echo "package revisor

//nolint:misspell
var namedEntities = [][]byte{" > html_entities_data.go

curl https://html.spec.whatwg.org/entities.json \
    | jq 'keys' | jq -r '.[]' | grep ';' \
    | awk -F'&' '{print "[]byte(\"" $2 "\"),"}' >> html_entities_data.go

echo "}" >> html_entities_data.go

gofmt -w html_entities_data.go
