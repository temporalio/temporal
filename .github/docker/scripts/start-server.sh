#!/bin/sh

set -eu

: "${SERVICES:=}"

flags=""
if [ -n "${SERVICES}" ]; then
    # Convert colon (or comma, for backward compatibility) separated string (i.e. "history:matching")
    # to valid flag list (i.e. "--service=history --service=matching").
    SERVICES=$(echo "${SERVICES}" | tr ':,' ' ')
    for service in ${SERVICES}; do
        flags="${flags} --service=${service}"
    done
fi

exec temporal-server start ${flags}
