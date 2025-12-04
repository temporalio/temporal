#!/bin/sh
# pipefail is included in alpine
# shellcheck disable=SC3040
set -eu -o pipefail

# Resolve hostname to IP address for binding if not already set (supports both IPv4 and IPv6)
: "${BIND_ON_IP:=$(getent hosts "$(hostname)" | awk '{print $1;}')}"
export BIND_ON_IP

# If binding to wildcard address (0.0.0.0 or ::0), set broadcast address if not already set
if [ "${BIND_ON_IP}" = "0.0.0.0" ] || [ "${BIND_ON_IP}" = "::0" ]; then
    : "${TEMPORAL_BROADCAST_ADDRESS:=$(getent hosts "$(hostname)" | awk '{print $1;}')}"
    export TEMPORAL_BROADCAST_ADDRESS
fi

exec temporal-server start 
