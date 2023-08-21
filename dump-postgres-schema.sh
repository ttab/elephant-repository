#!/usr/bin/env zsh

ip=$(docker inspect repository-postgres | \
         jq -r '.[0].NetworkSettings.IPAddress')
url="postgres://repository:pass@${ip}/repository"

docker run --rm -it postgres:15.3 pg_dump ${url} --schema-only \
       > postgres/schema.sql

docker run --rm -it postgres:15.3 pg_dump ${url} --data-only \
       --column-inserts --table=schema_version \
       > postgres/schema_version.sql
