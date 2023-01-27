#!/usr/bin/env zsh

set -o pipefail

pgdata="$(dirname $PWD)/localstate/repo-pgdata"

containerStatus=$(docker inspect repository-postgres | jq -r '.[0].State.Status')

if [[ $status -ne 0 ]]; then
    # Start postgres with wal_level=logical
    docker run -d \
       --name repository-postgres \
       -e POSTGRES_DB=repository \
       -e POSTGRES_USER=repository \
       -e POSTGRES_PASSWORD=pass \
       -e PGDATA=/var/lib/postgresql/data/pgdata \
       -v "${pgdata}":/var/lib/postgresql/data \
       postgres \
       -c wal_level=logical
elif [[ $containerStatus == "exited" ]]; then
     docker start repository-postgres
fi

ip=$(docker inspect repository-postgres | jq -r '.[0].NetworkSettings.IPAddress')
url="postgres://repository:pass@${ip}/repository"

echo ${url}

psql ${url}
