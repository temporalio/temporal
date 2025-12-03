#!/bin/sh

set -eu

# Resolve hostname to IP address for binding if not already set (supports both IPv4 and IPv6)
: "${BIND_ON_IP:=$(getent hosts "$(hostname)" | awk '{print $1;}')}"
export BIND_ON_IP

# If binding to wildcard address (0.0.0.0 or ::0), set broadcast address if not already set
if [ "${BIND_ON_IP}" = "0.0.0.0" ] || [ "${BIND_ON_IP}" = "::0" ]; then
    : "${TEMPORAL_BROADCAST_ADDRESS:=$(getent hosts "$(hostname)" | awk '{print $1;}')}"
    export TEMPORAL_BROADCAST_ADDRESS
fi

# Set default Temporal server address if not already configured
if [ -z "${TEMPORAL_ADDRESS:-}" ]; then
    echo "TEMPORAL_ADDRESS is not set, setting it to ${BIND_ON_IP}:7233"

    # IPv6 addresses contain colons and must be wrapped in brackets to avoid ambiguity with port separator
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

exec temporal-server start 
