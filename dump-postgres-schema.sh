#!/usr/bin/env zsh

ip=$(docker inspect repository-postgres | \
         jq -r '.[0].NetworkSettings.IPAddress')
url="postgres://repository:pass@${ip}/repository"

docker run --rm -it postgres pg_dump ${url} --schema-only \
       > postgres/schema.sql
