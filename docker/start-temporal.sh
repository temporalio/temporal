#!/bin/bash

set -eu -o pipefail

SERVICES="${SERVICES:-}"
SERVICE_ARGS="${SERVICE_ARGS:-}"

if [ -n "${SERVICES}" ]; then
    # Convert semicolon (or comma for backward compatibility) separated string (i.e. "history:matching")
    # to valid flags list (i.e. "--service=history --service=matching").
    IFS=':,' read -ra SERVICE_LIST <<< "${SERVICES}"
    SERVICE_ARGS=$(printf -- "--service=%s " "${SERVICE_LIST[@]}")
fi

exec temporal-server --env docker start "${SERVICE_ARGS[@]}"
