#!/bin/bash

set -e

export BIND_ON_IP="${BIND_ON_IP:-$(hostname -i)}"

# tctl
if [[ "BIND_ON_IP" =~ ":" ]]
then  # ipv6
    export TEMPORAL_CLI_ADDRESS="[${BIND_ON_IP}]:7233"
else  # ipv4
    export TEMPORAL_CLI_ADDRESS="${BIND_ON_IP}:7233"
fi

dockerize -template ./config/config_template.yaml:./config/docker.yaml

# autosetup
for arg in "$@" ; do [[ $arg == "autosetup" ]] && ./auto-setup.sh && break ; done

# debug
for arg in "$@" ; do [[ $arg == "debug" ]] && ./debug.sh && break ; done

# bash
for arg in "$@" ; do [[ $arg == "bash" ]] && bash && exit 0 ; done

./start-temporal.sh
