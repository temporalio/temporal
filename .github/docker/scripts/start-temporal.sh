#!/bin/bash

set -eu -o pipefail

: "${SERVICES:=}"

flags=()
if [[ -n ${SERVICES} ]]; then
    # Convert colon (or comma, for backward compatibility) separated string (i.e. "history:matching")
    # to valid flag list (i.e. "--service=history --service=matching").
    SERVICES="${SERVICES//:/,}"
    SERVICES="${SERVICES//,/ }"
    for i in $SERVICES; do flags+=("--service=$i"); done
fi

exec temporal-server --env docker start "${flags[@]}"
