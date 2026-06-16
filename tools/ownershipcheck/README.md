# ownershipcheck

A static analyzer that flags a **borrowed** (possibly shared) reference value — a
map, slice, or other kind selected by `-value-kinds` — being embedded by reference
into a **sink type that escapes the function**: a value later serialized or read
outside the lock that protected it. By default the sink is a **protobuf message**
marshaled by gRPC, so a retained alias can be mutated during the marshal, producing
`concurrent map iteration and map write` crashes. See temporalio/temporal#10706 /
#10707.

The **core is domain-agnostic and has no defaults** — it knows nothing about proto.
In taint-analysis terms the config is the classic triad: *sources* (structural —
the shared receiver), *sinks*, and *sanitizers*, supplied via flags:

| flag | meaning |
|---|---|
| `-sink` (**required**) | comma-separated sinks; a value of such a type is the hazard target. Each entry is either a **bare marker-method name** (e.g. `ProtoReflect` — robust for a unique generated marker) or a **qualified interface** (e.g. `encoding/json.Marshaler` — precise, matched by `types.Implements`, for ambiguous method names) |
| `-value-kinds` | comma-separated reference kinds tracked as the embedded hazard: `map`, `slice`, `pointer`, `interface`, `chan`, `func`. The proto profile uses `map,slice` (collections iterated during marshal). Adding `pointer` also flags borrowed sub-message pointers — broader coverage but noisier (see below) |
| `-sanitizers` | comma-separated `pkg.Func` calls that yield an **owned** value (clone/copy helpers) |
| `-escape-funcs` | comma-separated **opaque callees** that retain/marshal an argument where inference can't see (a gRPC client send, a cache/queue), as `pkgpath.Func#argN` or `pkgpath.Type.Method#argN` (arg index defaults to 0; `-1` = receiver). Passing a borrowed value to the listed parameter is flagged at the call site |

With no `-sink` set there are no sinks and nothing is reported. The protobuf profile
lives in one place — the `lint-ownership` Makefile target — which CI invokes:

```
make lint-ownership
# = go run ./cmd/tools/ownershipcheck \
#     -sink=ProtoReflect \
#     -sanitizers=maps.Clone,slices.Clone,proto.Clone,proto.CloneOf,common.CloneProto \
#     -value-kinds=map,slice ./...
```

Diagnostics name the concrete sink type (e.g. *embedded into ScheduleListInfo field
FutureActionTimes*) — no noun to configure.

## What it reports

Inside an exported function, embedding a map/slice into a `proto.Message` literal
when that value is **borrowed** — i.e. read out of the shared, leased component
(the method receiver), directly or through an accessor/getter:

```go
// flagged:
return &schedulepb.DescribeScheduleResponse{
    Memo: &commonpb.Memo{Fields: visibility.CustomMemo(ctx)},
}
// fixed:
return &schedulepb.DescribeScheduleResponse{
    Memo: &commonpb.Memo{Fields: maps.Clone(visibility.CustomMemo(ctx))},
}
```

Both embed forms are covered: a composite-literal field (`&Memo{Fields: x}`) and a
field assignment (`resp.Memo.Fields = x`). Each finding carries the flow path from
the borrowed source (e.g. *borrowed from the receiver → returned by GetCallback →
returned by GetLinks*) as related diagnostic information.

It only flags a proto that **escapes** the function — returned, or assigned into a
returned proto. A proto that is deep-cloned in place (`return CloneProto(tmp)`),
marshaled synchronously (`return p.Marshal()`), or read for a local computation
never reaches an out-of-lock marshal and is not flagged. It is also quiet on the
common-and-safe case: a value produced locally (`make`/literal/clone) with no
retained alias is the sole owner. A value rooted at a **parameter** or a **package
global** is treated as owned. The hazard is specifically shared *instance* state.

## Running it

```
make lint-ownership                          # protobuf profile (the configured target)
go run ./cmd/tools/ownershipcheck -sink=… -sanitizers=… ./...   # ad-hoc, flags required
```

It uses `go/analysis` Facts, so cross-package inference is build-cache friendly
and runs package-at-a-time.

## Directives

- `//ownership:result owned` / `//ownership:result borrowed` — on a function
  declaration, override inference for that function's result. `owned` suppresses
  (e.g. an accessor that returns an immutable snapshot, or clones internally in a
  way inference can't see). `borrowed` forces a borrowed result (e.g. the real
  source is reached through an interface). The override is exported as a Fact, so
  it applies cross-package.

- `//ownership:ignore <reason>` — on the embed line (or the line directly above),
  suppress a single finding. A **reason is required**, so suppressions are
  reviewable rather than silent.

  ```go
  Fields: c.memo, //ownership:ignore immutable after init, never mutated
  ```

- `//ownership:param <name> escapes` — in a function's doc comment, declare that
  the named parameter's data is retained or marshaled by the callee (e.g. an RPC
  sender that inference can't see through). Passing a borrowed value to it is then
  flagged at the call site. (Named, not positional/inline — Go doesn't reliably
  attach comments to individual parameters.)

  ```go
  //ownership:param req escapes
  func (c *client) Send(req *pb.Request) error
  ```

## Rollout

It runs as a standalone vet-style tool — the `ownership-check` job in
`.github/workflows/run-tests.yml` invokes `make lint-ownership` (which holds the
sink/sanitizer config).

1. **Warn-only** first: the CI job uses `continue-on-error`, so findings show in the
   log without blocking. Triage the initial hit list —
   annotate the legitimate ones (`//ownership:ignore <reason>`),
   fix the real ones.
2. **Gate** once the backlog is handled: drop `continue-on-error` and add
   `ownership-check` to the `test-status` job's `needs`.

(There is no golangci-lint integration; the standalone tool and its
`//ownership:ignore` hatch cover suppression directly.)

## Tracked value kinds

The proto profile tracks `map,slice` — collections that are *iterated and mutated in
place* during marshal (`append`/`delete`/key-assign), the loud `concurrent map
iteration and map write` case. `-value-kinds` can add `pointer` (and `interface`,
`chan`, `func`) to also flag borrowed sub-message pointers. Three precision rules
keep that from being noisy:

- **Reaches-a-leaf** — a borrowed pointer is flagged only if its pointee
  *transitively reaches* a map or non-byte slice through its **exported** (marshaled)
  fields. A pointer to a scalar-only message (`timestamppb`, `durationpb`, a
  type/name config) can never produce the crash, so it is silent.
- **No `[]byte`** — a byte slice is a blob/token, not a mutable collection, so it is
  not a hazard leaf.
- **Input sets** — a helper that returns one of several inputs (e.g. `min(a, b)`)
  resolves to *owned* when its actual arguments are owned, instead of collapsing to
  borrowed and reporting a false positive.

A repo-wide run shows the effect: `map,slice` reports 3 findings (all real); adding
`pointer` reports **10** (the same 3 plus 7 borrowed sub-message pointers), down from
23 before these rules. The 7 are genuine same-class bugs `map,slice` *misses*
because the mutable map lives one level down inside the sub-message — e.g. matching's
live `UserData`/`EphemeralData`, an activity's borrowed `Header`/`Input` (which hold
`Payloads`/maps). With the noise removed, `pointer` is a reasonable audit pass; the
CI profile still stays `map,slice` pending triage of those 7.

## Scope (what it does NOT do)

- No lock/region or concurrency analysis; "borrowed" is approximated structurally.
- Sinks are **assignments** (composite literals + field assignments). Setters
  (`resp.SetX(v)`) are not detected — that needs cross-call param→field modeling.
- Escape through a callee is detected when the callee's leak is inferable (same
  build) or declared via `-escape-funcs` / `//ownership:param escapes`; an opaque
  retaining callee that is *not* listed is still a blind spot.
- Spatial axis only (value crosses a package boundary). The temporal axis
  (same-package goroutine/closure capture) is out of scope.
- Not sound: accepts some false positives (annotate them) and some false negatives
  (e.g. a mutable *package-global* map, or borrowing reached only through
  reflection/`any`). Targets this specific aliasing class, not all aliasing bugs.

See `../../../plan.md` for the full design and milestone history.
