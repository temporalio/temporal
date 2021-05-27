#!/bin/bash

set -eu -o pipefail

export BIND_ON_IP="${BIND_ON_IP:-$(hostname -i)}"

if [[ "${BIND_ON_IP}" =~ ":" ]]; then
    # ipv6
    export TEMPORAL_CLI_ADDRESS="[${BIND_ON_IP}]:7233"
else
    # ipv4
    export TEMPORAL_CLI_ADDRESS="${BIND_ON_IP}:7233"
fi

dockerize -template ./config/config_template.yaml:./config/docker.yaml

# Automatically setup Temporal Server (databases, Elasticsearch, default namespace) if "autosetup" is passed as an argument.
for arg in "$@" ; do [[ ${arg} == "autosetup" ]] && ./auto-setup.sh && break ; done

# Setup Temporal Server in development mode if "develop" is passed as an argument.
for arg in "$@" ; do [[ ${arg} == "develop" ]] && ./setup-develop.sh && break ; done

# Run bash instead of Temporal Server if "bash" is passed as an argument (convenient to debug docker image).
for arg in "$@" ; do [[ ${arg} == "bash" ]] && bash && exit 0 ; done

./start-temporal.sh
