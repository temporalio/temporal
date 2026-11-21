#!/usr/bin/env bash
set -euo pipefail
cd ../../..
api_binpb=${API_BINPB:-proto/api.binpb}
protoc_gen_go=${PROTOC_GEN_GO:-protoc-gen-go}
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
go build -o "$tmpdir/protoc-gen-go-helpers" go.temporal.io/api/cmd/protoc-gen-go-helpers
protoc -I=. --descriptor_set_in="$api_binpb" \
  --plugin="protoc-gen-go=$protoc_gen_go" \
  --plugin="protoc-gen-go-helpers=$tmpdir/protoc-gen-go-helpers" \
  --go_out=paths=source_relative:"$tmpdir" \
  --go-helpers_out=paths=source_relative,draft_omission=true:"$tmpdir" \
  common/draft/testfixture/filter.proto
generated="$tmpdir/common/draft/testfixture"
for companion in common/draft/testfixture/*.draft.pb.go; do
  if [[ -f "$companion" && ! -f "$generated/${companion##*/}" ]]; then
    rm "$companion"
  fi
done
# Strip the protoc and plugin versions from the header, as protogen's
# VersionRemover does for the server's other generated protos, so the output
# does not depend on the local protoc version.
for file in "$generated"/*.pb.go; do
  sed -E \
    -e '1,/^package /{' \
    -e 's|^// versions:$|// plugins:|' \
    -e 's|^(//[[:space:]]+(- )?protoc[-_[:alnum:]]*)[[:space:]]+[_.[:alnum:]]+.*$|\1|' \
    -e '}' \
    "$file" >"common/draft/testfixture/${file##*/}"
done
