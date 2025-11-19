#!/bin/sh

set -eu

: "${BIND_ON_IP:=$(getent hosts "$(hostname)" | awk '{print $1;}')}"
export BIND_ON_IP

if [ "${BIND_ON_IP}" = "0.0.0.0" ] || [ "${BIND_ON_IP}" = "::0" ]; then
    : "${TEMPORAL_BROADCAST_ADDRESS:=$(getent hosts "$(hostname)" | awk '{print $1;}')}"
    export TEMPORAL_BROADCAST_ADDRESS
fi

# check TEMPORAL_ADDRESS is not empty
if [ -z "${TEMPORAL_ADDRESS:-}" ]; then
    echo "TEMPORAL_ADDRESS is not set, setting it to ${BIND_ON_IP}:7233"

    case "${BIND_ON_IP}" in
        *:*)
            # ipv6
            export TEMPORAL_ADDRESS="[${BIND_ON_IP}]:7233"
            ;;
        *)
            # ipv4
            export TEMPORAL_ADDRESS="${BIND_ON_IP}:7233"
            ;;
    esac
fi

# Support TEMPORAL_CLI_ADDRESS for backwards compatibility.
# TEMPORAL_CLI_ADDRESS is deprecated and support for it will be removed in the future release.
if [ -z "${TEMPORAL_CLI_ADDRESS:-}" ]; then
    export TEMPORAL_CLI_ADDRESS="${TEMPORAL_ADDRESS}"
fi

# Automatically setup Temporal Server (databases, Elasticsearch, default namespace) if "autosetup" is passed as an argument.
for arg in "$@"; do
    if [ "${arg}" = "autosetup" ]; then
        /etc/temporal/auto-setup.sh
        break
    fi
done

# Setup Temporal Server in development mode if "develop" is passed as an argument.
for arg in "$@"; do
    if [ "${arg}" = "develop" ]; then
        /etc/temporal/setup-develop.sh
        break
    fi
done

# Run sh instead of Temporal Server if "bash" is passed as an argument (convenient to debug docker image).
for arg in "$@"; do
    if [ "${arg}" = "bash" ]; then
        sh
        exit 0
    fi
done

exec /etc/temporal/start-server.sh
