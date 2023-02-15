#!/usr/bin/env bash

for file in testdata/*.json
do
    name=$(basename $file)
    go run ./cmd/revisor document \
       -json ./testdata/$name \
       > testdata/results/base-$name
    go run ./cmd/revisor document -spec constraints/example.json \
       -json ./testdata/$name \
       > testdata/results/example-$name
done
