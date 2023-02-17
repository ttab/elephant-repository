#!/usr/bin/env zsh

set -o pipefail

data_dir="${STATE_DIR:-$HOME/localstate}"
data="${data_dir}/minio"

containerStatus=$(docker inspect minio-server | jq -r '.[0].State.Status')

if [[ $status -ne 0 ]]; then
    # Start a local minio instance
    docker run \
           --name minio-server \
           -d --rm -p 9000:9000 -p 9001:9001 \
           -v ${data}:/data \
           minio/minio server /data --console-address ":9001"
elif [[ $containerStatus == "exited" ]]; then
     docker start minio-server
fi
