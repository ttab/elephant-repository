#!/usr/bin/env zsh

set -euo pipefail
mode=${1:-""}

data_dir="${STATE_DIR:-$HOME/localstate}"
pgdata="${data_dir}/repo-pgdata"

mkdir -p $pgdata

if containerStatus=$(docker inspect repository-postgres | jq -r '.[0].State.Status'); then
    if [[ $containerStatus == "exited" ]]; then
        echo "Restarting postgresql..."
        docker start repository-postgres
        sleep 3
    else
        echo "Postgresql has status: ${containerStatus}"
    fi
else
    echo "Starting postgresql..."
    docker run -d --rm \
       --name repository-postgres \
       --user $UID:$GID \
       -e POSTGRES_DB=repository \
       -e POSTGRES_USER=repository \
       -e POSTGRES_PASSWORD=pass \
       -e PGDATA=/var/lib/postgresql/data/pgdata \
       -v "${pgdata}":/var/lib/postgresql/data \
       -p 5432:5432 \
       postgres:15.3 \
       -c wal_level=logical \
       -c log_lock_waits=on

    sleep 5
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
   status_heads, status, status_rule, acl, acl_audit, metric
TO reporting;

CREATE ROLE reportuser WITH LOGIN PASSWORD 'reportuser' IN ROLE reporting;

EOF

[ "$mode"  = 'psql' ] && psql ${url}
