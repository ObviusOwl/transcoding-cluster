#!/bin/bash
export LANG="C.UTF-8"
export LANGUAGE="C.UTF-8"
export LC_ALL="C.UTF-8"

if [ "$MODE" == "server" ]; then
    cd /opt/tc
    . ../tc-venv/bin/activate
    export FLASK_APP=transcoding_cluster_server
    flask run -h 0.0.0.0
elif [ "$MODE" == "worker" ]; then
    /opt/tc/bin/tcc work
else
    /opt/tc/bin/tcc "$@"
fi