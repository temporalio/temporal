# Using draft APIs

Draft annotations drive stable proto projection, explicit usage acknowledgments,
and server-side RPC gating and payload filtering. SDKs consume the stable API.
The server uses `github.com/temporalio/api-go/api_next` at commit `4bd28ec6`
as its replacement for `go.temporal.io/api`.

## Annotating draft declarations

Edit sources under `api/temporal/api_next`, importing
`temporal/api_next/protometa/v1/draft.proto`. A field can name one feature:

```proto
string debug_text = 42 [(temporal.api.protometa.v1.draft) = "feature-a"];
```

A whole message can also name a feature:

```proto
message DraftDetails {
  option (temporal.api.protometa.v1.draft_message) = "feature-a";
  string text = 1;
}
```

A field in a stable message that references `DraftDetails` must explicitly
carry `draft` with feature `feature-a`. The same rule applies to repeated
elements, map values and the members of stable oneofs. A member of a draft
container inherits that container's feature and may only reference draft types
with the same feature. This also applies to types nested inside a draft message.
Keep a shared type stable and annotate its reference fields when only some
uses should be draft.

Use `draft_enum` for a whole enum, `draft_enum_value` for a new value in a stable
enum, and `draft_oneof` for a whole oneof. A draft value in a stable enum must
have a unique, nonzero number. A nonempty enum, oneof or service whose members
are all draft must carry the corresponding container annotation. Stable messages
can retain their identity when all their fields are draft.

Use `draft_service` for a whole service and `draft_method` for an RPC in a stable
service. Both endpoints of a draft RPC must explicitly carry `draft_message`
with the RPC's feature. A draft endpoint type can be used in only one RPC
endpoint position; stable RPCs may share endpoint types. The stable proto
projection removes draft declarations and retains their stable containers.

Each declaration has one feature. Feature names are case-sensitive, nonempty
whitespace-free tokens. Containment is the sole source of implicit draft status.
Proto files and extension declarations have no draft annotation. Extension
references to draft types are rejected.

## Acknowledging references in Go

The `draftapi` analyzer reports named references to draft declarations,
including fields, getters, messages, enums and enum values, oneof wrappers,
and generated service and RPC symbols. It reads the descriptors in the API module
selected by `go.mod`; no separate API inventory needs updating. Each invalid API
annotation, descriptor or generated-symbol mapping error is reported once at the
API package that produced it. This fails `go vet` even when the API module is
only a transitive dependency of the packages being vetted.

Use an exact feature acknowledgment:

```go
value := req.GetDraftField() // temporal:allow-draft-api feature-a
```

A directive immediately before a statement or declaration covers that node and
its body. A trailing directive covers its line. A file-leading directive applies
to the file. A directive can acknowledge several features when the covered code
references several draft declarations:

```go
// temporal:allow-draft-api feature-a feature-b // Both behaviors are gated here.
func applyDraftFeatures(req *Request) {
    use(req.GetFirstDraftField(), req.GetSecondDraftField())
}
```

Names match exactly; there is no wildcard. An explanation may follow after a
separate `//` token; a feature name may itself contain `//`. A missing, misspelled or wrong-case
feature still produces a diagnostic. `//nolint:draftapi`, `//nolint:all` and
blanket `//nolint` do not acknowledge draft usage. The analyzer and its
acknowledgments aid review. The runtime filter enforces draft API exposure in
the [gRPC interceptor](../../common/rpc/interceptor/draft_fields.go) and the
[Nexus operation](../../service/frontend/nexus_handler.go) and
[completion](../../service/frontend/nexus_completion_http_handler.go) handlers.

The analyzer checks named references to draft symbols, including promoted
fields. It misses draft data accessed through `protoreflect`, dynamic field
names, `protojson` or other serialization, `proto.Merge` or `proto.Clone`,
interface methods such as `GetUser`, ad-hoc type assertions, generic constraints,
or an acknowledged wrapper. Unkeyed composite literals hide the names of the
fields they initialize. Naming a draft type through an alias declared in
another server package also bypasses the check. Implementing a server method
does not reference the generated RPC declaration.

The analyzer intentionally skips files with a standard `Code generated` header,
`_test.go` files, and packages under `tests/` and `common/testing/`.

Run the same analyzer used by normal lint/CI:

```sh
make lint-draft-api
make lint-draft-api LINT_CODE_TARGETS=./service/frontend
```

The analyzer fixtures use real generated output. Regenerate after editing their
schemas from `tools/draftapi/testdata/api/` with protoc 35.1,
`protoc-gen-go` v1.36.12, `protoc-gen-go-grpc` v1.6.2, and
`protoc-gen-grpc-gateway` v2.30.0:

```sh
protoc -I proto --go_out=. --go_opt=module=go.temporal.io/api \
  --go-grpc_out=. --go-grpc_opt=module=go.temporal.io/api \
  draft.proto message.proto references.proto
protoc -I proto --grpc-gateway_out=. \
  --grpc-gateway_opt=module=go.temporal.io/api,generate_unbound_methods=true message.proto
```

## Exposure and runtime boundaries

Enable exposure through existing namespace dynamic-config constraints:

```yaml
frontend.enabledDraftFeatureAPIs:
  - value: [feature-a, feature-b]
    constraints:
      namespace: early-access-ns
  - value: [feature-a]
```

The default empty list disables all draft features. Unknown names have no effect;
there is no caller opt-in. Bare strings, null, non-list values and lists containing
null or nonstrings select the empty default with a throttled warning. A malformed
namespace entry does not fall back to an enabling unconstrained entry. Separate
behavior flags control feature execution and creation of draft values.

WorkflowService and OperatorService unary calls on frontend and internal-frontend
gate draft RPCs and filter requests immediately after existing namespace
normalization, before authorization, routing, ordinary validation and handlers.
A disabled draft RPC returns `codes.Unimplemented`. The outermost response
wrapper filters responses and decoded gRPC status details, including early
returns. Unrecognized status detail envelopes pass through.
Generated HTTP operations use that same chain. Unary authorizers therefore see
omitted requests; direct Nexus authorization sees original input before conversion.

One lazy selection reads configuration at most once per invocation and shares it
between the RPC gate, request and response. Stable RPCs whose payloads are
unaffected never read the list. Existing task-token
normalization supplies namespaces, including query tokens; omission parses no
tokens. Remaining namespace IDs resolve through the registry; an unresolved
supplied ID selects an empty list. Namespace-less requests use the unconstrained
value. Namespace-less exposure therefore requires that unconstrained setting.

Direct Nexus start/cancel operations and completion callbacks filter materialized
public proto roots at conversion boundaries before matching/history submission
or SDK conversion. Completion fallback shares the selection. Transparent Nexus
forwarding relays bodies/tokens/result metadata; the locally handling cluster's
list governs it. Unary redirection filters every decoded hop. Keep exposure lists
aligned across clusters serving a namespace.

Generated `OmitDraftFields(enabled func(string) bool) (removedEnumValue bool)`
methods mutate affected graphs in place. Disabled optional fields and oneof
members lose presence, including present zero values. Disabled draft enum
values in retained fields are cleared from singular fields, dropped from lists,
or removed with their map entries. Stable siblings, unrecognized enum numbers
and opaque bytes remain. A request that loses a draft enum value from a retained
field returns `codes.InvalidArgument` or a Nexus bad-request error with a generic
message. Removing a whole disabled field or oneof skips its contents. Responses
and error details are filtered regardless of whether they contained disabled
enum values.
A directly supplied draft root cannot nil its caller's pointer;
when its type gate is disabled, its method resets the entire message, including
unknown fields. Recursive schemas are supported; cyclic runtime object graphs
are not. Repeated omission is idempotent.

Handlers returning borrowed registry/cached/stored data must copy affected
subgraphs first, including shared nested pointers, maps and slices. A new root
alone does not establish ownership. The gRPC filter copies affected responses
and status details before omission and passes through unaffected errors and
non-protobuf response values. Omission/config panics yield a generic Internal
error without payload details; existing handler panic recovery remains unchanged.

- A frontend registration test requires each registered gRPC service to have
  both draft RPC gating and payload filtering or an explicit exclusion.
  The exclusions cover AdminService, internal service APIs such as HistoryService
  and MatchingService, gRPC health, and reflection.
- A separate frontend registration test fails if any covered service gains a
  streaming RPC. Streaming RPCs require a stream interceptor.
- Non-Nexus custom HTTP routes are outside the filter.
- `google.protobuf.Any` fields within messages are opaque. For example, the
  update protocol packs `update.Request` into `protocol.v1.Message.body`. The
  filter leaves that embedded request, application payload bytes and raw history
  opaque. Annotating the containing field removes it whole. Raw history may
  retain previously stored draft values after exposure is disabled. The filter
  traverses structured fields reachable from `HistoryEvent`.
- In messages the filter retains, protobuf forward compatibility preserves
  fields the server does not define. The `api_next` rule requiring deleted draft
  fields to reserve both their number and name prevents reuse of those field
  numbers.

## Regeneration, retirement and promotion

Run commands in dependency order from the indicated repositories:

```sh
# api/
make stable-api-test stable-api
# api-go/: use matching local schemas, without updating remote submodule refs.
go install ./cmd/protoc-gen-go-helpers
make helper-test-protos
make api-next PROTO_ROOT=../api
make check test
go -C api_next build -tags protolegacy ./...
```

Once the regenerated `api_next` commit is available in `temporalio/api-go`,
set `api_go_commit` to its hash and run these commands from the server repo.
`go mod tidy` records the canonical pseudo-version and checksums in
`go.mod` and `go.sum`.

For CI, the resolved stable API version and the draft replacement version must
match. The initial draft pin is accepted with stable API `v1.64.3` while paired
release tags are being introduced.

```sh
api_go_commit=4bd28ec6
go mod edit -replace="go.temporal.io/api=github.com/temporalio/api-go/api_next@${api_go_commit}"
go mod tidy
make proto
make lint-draft-api
```

Install generator prerequisites with `make grpc-install` in API-Go if needed.
Refresh the local helper when switching sources/tool locations; an old installed
helper may otherwise be selected through PATH despite an existing install stamp.
The `draft_omission=true` helper parameter is enabled only for `api_next` and
compiled fixtures. Stable/internal generated types receive no omission methods.
The draft module also regenerates proxy visitors, services and request headers
against its descriptors and Go types; copying stable proxy output misses draft
payload-bearing subtrees.
CI regeneration detects stale fixtures/companions; independent registry coverage
requires methods on affected linked types, including enum-only messages and
error details, and detects stale methods.

Before annotating a feature, record behavior flags and affected transports,
including encoded exclusions; confirm omission preserves safe request meaning;
audit borrowed response subgraphs; document preserve/delete semantics for update
or replace RPCs (no generic merge); deploy annotation-aware frontends before
creating values; review transitive generated roots and benchmark large collections.
Add focused feature tests, aiming at useful coverage rather than artificial cases.

To promote, remove the annotation and regenerate; retain the declaration/field
in `api_next`. To abandon a field, remove it and reserve **both its number and
name** in the source schema. The next-to-next Buf check enforces this even for
fields never present in stable protos. Normal breaking checks still protect the
stable API. Never overwrite independently authored expected fixture trees with
actual generator output.

## Omission benchmark

`go test -tags test_dep ./common/draft -run '^$' -bench BenchmarkOmit -benchmem`
measures interface dispatch and omission, not end-to-end frontend latency.
The earlier implementation baseline on Apple M5 Pro measured 1.821 ns/op with
zero allocations for an unaffected message, 84.47 ns/op and one 16-byte allocation
for a small affected graph, and 31.86 ns/op and one 16-byte allocation for a root
with opaque 2 MiB bytes. Those bytes are neither decoded nor copied. These are
historical baseline measurements, not a new whole-message benchmark claim.
