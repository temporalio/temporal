#!/usr/bin/env sh
set -euo pipefail
PROTO_OUT=api
PROTO_ROOT=${PROTO_ROOT:-proto}
PROTO_OUT=api
PROTO_OPTS="paths=source_relative:${PROTO_OUT}"

case $(uname -s) in
    Darwin*)
        SED="sed -i ''"
        ;;
    *)
        SED="sed -i"
        ;;
esac

# Compile protos
find "${PROTO_ROOT}/internal" -name "*.proto" | sort | xargs -I{} dirname {} | uniq | while read -r dir; do
    protoc --fatal_warnings \
        -I="${PROTO_ROOT}/internal" \
        -I="${PROTO_ROOT}/api" \
        -I="${PROTO_ROOT}/dependencies" \
        --go_out="${PROTO_OPTS}" \
        --go-grpc_out="${PROTO_OPTS}" \
        --go-helpers_out="${PROTO_OPTS}" \
        "${dir}"/*
done
grep -ERl "^//\s+(- )?protoc[ -]" ${PROTO_OUT} | xargs "${SED[@]}" -E -e '\@//[[:space:]]*(- )?protoc@d'
mv -f "${PROTO_OUT}/temporal/server/api/"* "${PROTO_OUT}" && rm -rf "${PROTO_OUT}"/temporal

# The generated enums in go are just plain terrible, so we fix them
# by removing the typename prefixes. We already made good choices with our enum
# names, so this shouldn't be an issue
# Note that we have to special case one of them because we didn't make a good choice of name for it
# We turn off -e because grep will fail if it doesn't find anything, which is fine. We don't care.
set +e
grep -R '^enum ' "${PROTO_ROOT}" | cut -d ' ' -f2 | while read -r enum; do
    grep -Rl "${enum}" "${PROTO_OUT}" \
        | grep -E "\.go" \
        | xargs "${SED[@]}" -e "s/${enum}_\(.*\) ${enum}/\1 ${enum}/g" \
                            -e "s/\.${enum}_\(.*\)/.\1/g"
done
"${SED[@]}" -e "s/BuildId_\(.*\) BuildId_State/\1 BuildId_State/g;s/return BuildId_\(.*\)/return \1/g" "${PROTO_OUT}/persistence/v1/task_queues.pb.go"
set -e
# We rely on the old temporal CamelCase JSON enums for presentation, so we rewrite the String method
# on all generated enums
find . -name "*.pb.go" | xargs enumrewriter
