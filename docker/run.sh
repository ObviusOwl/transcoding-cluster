#!/bin/bash
export LANG="C.UTF-8"
export LANGUAGE="C.UTF-8"
export LC_ALL="C.UTF-8"

if [ "$MODE" == "server" ]; then
    uwsgi --plugin python3,http --enable-threads \
        --http 0.0.0.0:5000 \
        --uid 1000 --gid 1000 --die-on-term \
        --chdir /opt/tc --venv /opt/tc-venv \
        --module "transcoding_cluster_server:create_app()" 
elif [ "$MODE" == "worker" ]; then
    /opt/tc/bin/tcc work
else
    /opt/tc/bin/tcc "$@"
fi