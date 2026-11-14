# Using draft APIs

Draft annotations drive stable proto projection, explicit usage acknowledgments,
and generated request/response omission. SDKs consume the stable API; the server
builds against `../api-go/api_next` through its module replacement.

## Annotating fields and messages

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

Every field referencing `DraftDetails` automatically requires `feature-a`.
Projection removes the message and those references. Runtime omission removes
an entire referencing field, repeated/map container, or active oneof branch when
its type's gate is disabled. Stable containing messages and siblings remain,
even if a containing message becomes empty. Map value types determine the gate.

A field tagged B and typed as a message tagged A requires **both A and B**.
Declarations nested inside a draft message inherit its gate, including nested
enums referenced elsewhere. A nested message tagged B inside an outer message
tagged A requires both gates. Keep a shared type stable and annotate its reference
fields when only some uses should be draft. An annotation on a type cannot be
overridden at a reference.

Only field and message annotations are supported. Standalone enum annotations,
enum values, whole oneofs, services, RPCs, files and extensions are unsupported.
A stable RPC cannot directly use a draft message or nested type as its request
or response: projection reports that dangling signature. Extension references
to draft types are rejected too; extensions are not a supported omission path.
Feature names are case-sensitive, nonempty whitespace-free tokens.

## Acknowledging references in Go

The `draftapi` analyzer reports direct references to draft fields, getters, types,
contained declarations and oneof branches, including otherwise unannotated
fields/getters whose type is draft. It reads the descriptors in the API module
selected by `go.mod`; no separate API inventory needs updating. Invalid API
annotation, descriptor and generated-symbol mapping errors propagate through
package facts, including intervening imports, so dependency-mode `go vet` fails
closed instead of silently losing usage diagnostics.

Use an exact feature acknowledgment:

```go
value := req.GetDraftField() // temporal:allow-draft-api feature-a
```

A directive immediately before a statement or declaration covers that node and
its body. A trailing directive covers its line. A file-leading directive applies
to the file. Multiple required gates must all be acknowledged, either together
or through applicable directives:

```go
// temporal:allow-draft-api feature-a feature-b // Both behaviors are gated here.
func applyDraftFeature(req *Request) {
    use(req.GetDraftField())
}
```

Names match exactly; there is no wildcard. A missing, misspelled or wrong-case
feature still produces a diagnostic. `//nolint:draftapi`, `//nolint:all` and
blanket `//nolint` do not acknowledge draft usage. Acknowledgment is a review
signal, not static proof of a dynamic behavior guard.

Generated files, `_test.go` files, and packages under `tests/` and
`common/testing/` are exempt. Go type information covers aliases and promoted
fields. Reflection, dynamic field names, serialization and references hidden
behind an acknowledged wrapper remain outside the check. Indirect access through
interfaces, ad-hoc type assertions or generic constraints, and naming a draft
type through an alias declared in another package, are not checked, because the
analyzer reports direct references only. Implementing a server method is not itself a reference to a
generated RPC declaration.

Run the same analyzer used by normal lint/CI:

```sh
make lint-draft-api
make lint-draft-api LINT_CODE_TARGETS=./service/frontend
```

The analyzer fixtures use real generated output. Regenerate after editing their
schemas, from `tools/draftapi/testdata/api/`:

```sh
protoc -I proto --go_out=. --go_opt=module=go.temporal.io/api \
  --go-grpc_out=. --go-grpc_opt=module=go.temporal.io/api \
  draft.proto message.proto references.proto
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

The default empty list hides every draft field. Unknown names have no effect;
there is no caller opt-in. Bare strings, null, non-list values and lists containing
null or nonstrings select the empty default with a throttled warning. A malformed
namespace entry does not fall back to an enabling unconstrained entry. Separate
behavior flags control feature execution and creation of draft values.

WorkflowService and OperatorService unary calls on frontend/internal-frontend
omit disabled request fields immediately after existing namespace normalization,
before authorization, routing, ordinary validation and handlers. The outermost
response wrapper filters successful responses, including inner early returns.
Generated HTTP operations use that same chain. Unary authorizers therefore see
omitted requests; direct Nexus authorization sees original input before conversion.

One lazy selection reads configuration at most once per invocation and shares it
between input/output. Unaffected roots never read the list. Existing task-token
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

Generated `OmitDraftFields(enabled func(string) bool)` methods mutate affected
graphs in place. Disabled optional/oneof fields lose presence, including present
zero values. Stable siblings, unaffected containers, unknown fields and opaque
bytes remain. A directly supplied draft root cannot nil its caller's pointer;
when its type gate is disabled, its method resets the entire message, including
unknown fields. Recursive schemas are supported; cyclic runtime object graphs
are not. Repeated omission is idempotent.

Handlers returning borrowed registry/cached/stored data must copy affected
subgraphs first, including shared nested pointers, maps and slices. A new root
alone does not establish ownership. Handler errors and non-protobuf values pass
through. Omission/config panics yield a generic Internal error without payload
details; existing handler panic recovery remains unchanged.

Admin/internode services, streaming, non-Nexus custom HTTP routes and gRPC error
details are excluded. Encoded Any contents, application payload bytes and raw
history remain opaque. Annotating their containing field removes it whole; no
encoded contents are decoded. Raw history may retain previously stored draft
values after exposure is disabled. Structured HistoryEvent-reachable fields are
supported. Older frontends cannot classify unknown fields from newer binaries.

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
# temporal/
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
requires methods on affected linked types, rejects stale methods, and rejects
affected error-detail messages on excluded channels.

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
