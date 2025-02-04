#!/usr/bin/env bash

set -eu -o pipefail

color() { printf "\e[1;36m%s\e[0m\n" "$*"; }

api=$PROTO_OUT
new=$api.new

color "Generating protos..."

rm -rf "$new" && mkdir -p "$new"

$PROTOGEN \
  --descriptor_set_in="$API_BINPB" \
  --root="$PROTO_ROOT"/internal \
  --rewrite-enum=BuildId_State:BuildId \
  --output="$new" \
  -p go-grpc_out=paths=source_relative:"$new" \
  -p go-helpers_out=paths=source_relative:"$new"

color "Run goimports for proto files..."
$GOIMPORTS -w "$new"

color "Generate proto mocks..."
find "$new" -name service.pb.go -o -name service_grpc.pb.go | while read -r src; do
  dst=$(echo "$src" | sed -e 's,service/,servicemock/,' -e 's,[.]go$,.mock.go,')
  pkg=$(basename "$(dirname "$(dirname "$dst")")")
  $MOCKGEN -copyright_file LICENSE -package "$pkg" -source "$src" -destination "$dst"
  # Since we're generating mocks from files in incorrect locations, mockgen
  # writes incorrect imports. Fix them manually. The awkward sed/rm invocation
  # is to work on linux and macos.
  sed -i.replaced "s,$new/temporal/server/,," "$dst"
  rm -f "$dst.replaced"
done

color "Update license headers for proto files..."
go run ./cmd/tools/copyright/licensegen.go --scanDir "$new"

color "Modify history service server interface..."
sed -i.bak -e \
    's/GetWorkflowExecutionHistory(context\.Context, \*GetWorkflowExecutionHistoryRequest) (\*GetWorkflowExecutionHistoryResponse, error)/GetWorkflowExecutionHistory(context.Context, *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponseWithRaw, error)/g' \
    "$new"/temporal/server/api/historyservice/v1/service_grpc.pb.go && rm "$new"/temporal/server/api/historyservice/v1/service_grpc.pb.go.bak

color "Moving proto files into place..."
old=$api.old
[[ -d "$api" ]] && mv -f "$api" "$old"
mkdir -p "$api"
mv -f "$new"/temporal/server/api/* "$api"/
rm -rf "$new" "$old"
