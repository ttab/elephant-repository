#!/usr/bin/env zsh

set -o pipefail

data_dir="${STATE_DIR:-$HOME/localstate}"
pgdata="${data_dir}/repo-pgdata"

containerStatus=$(docker inspect repository-postgres | jq -r '.[0].State.Status')

if [[ $status -ne 0 ]]; then
    # Start postgres with wal_level=logical
    docker run -d --rm \
       --name repository-postgres \
       -e POSTGRES_DB=repository \
       -e POSTGRES_USER=repository \
       -e POSTGRES_PASSWORD=pass \
       -e PGDATA=/var/lib/postgresql/data/pgdata \
       -v "${pgdata}":/var/lib/postgresql/data \
       -p 5432:5432 \
       postgres \
       -c wal_level=logical \
       -c log_lock_waits=on

    sleep 5
elif [[ $containerStatus == "exited" ]]; then
    docker start repository-postgres
    sleep 3
fi

ip=$(docker inspect repository-postgres | jq -r '.[0].NetworkSettings.IPAddress')
url="postgres://repository:pass@${ip}/repository"

echo ${url}
cat <<EOF

If this is a fresh install, create a reporting user:

CREATE ROLE reporting;

GRANT SELECT
ON TABLE
   document, delete_record, document_version, document_status,
   status_heads, status, status_rule, acl, acl_audit
TO reporting;

CREATE ROLE reportuser WITH LOGIN PASSWORD 'reportuser' IN ROLE reporting;

EOF

psql ${url}
