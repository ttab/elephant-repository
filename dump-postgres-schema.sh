#!/usr/bin/env zsh

ip=$(docker inspect repository-postgres | \
         jq -r '.[0].NetworkSettings.IPAddress')
url="postgres://repository:pass@${ip}/repository"

docker run --rm -it postgres pg_dump ${url} --schema-only \
       > postgres/schema.sql

docker run --rm -it postgres pg_dump ${url} --data-only \
       --column-inserts --table=schema_version \
       > postgres/schema_version.sql

docker run --rm -it postgres pg_dump ${url} --data-only \
       --column-inserts --table=document_schema --table=active_schemas \
       > postgres/document_schema.sql
