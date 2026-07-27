# configurator (vendored)

Vendored copy of https://github.com/davidporter-id-au/configurator at commit `4c50e59`.

## Why vendored rather than a `go.mod` dependency

The upstream `go.mod` declares the module path
`github.com/davidporter-id-au/constraints-config`, which does not match the repository URL
(`github.com/davidporter-id-au/configurator`), so `go get` cannot resolve it, and there are
no tags. Rather than wait on that, the source lives here.

Import paths have been rewritten from `github.com/davidporter-id-au/constraints-config/...`
to `go.temporal.io/server/common/dynamicconfig/configurator/...`.

**This copy is the source of truth.** Upstream was an experiment; changes are made here
directly rather than being sent upstream and re-vendored. Local changes so far:

- `types.Lookup`, a one-method interface that `Expression.Matches` and `Eval` take in place
  of a concrete `Constraints` map. It lets a caller present several layers of constraints —
  process-ambient ones and per-request ones — as a single view without copying them into a
  new map on every evaluation. `Constraints` still satisfies it, so passing a plain map works.
- `Configurator.ReferencedKeys(key)`, returning the constraint keys an entry's expressions
  test. Used to tell at load time whether an entry can be resolved once up front, and to
  reject expressions that reference an undeclared key.
- **Values are opaque.** `Config[V]` carries already-decoded values of the caller's choosing
  instead of `json.RawMessage`, and the library never inspects, decodes or converts one — it
  parses the match expressions and hands back whichever `V` won. Decoding here would mean
  imposing a type system on the caller, and Temporal's notion of a setting's "type" is an
  arbitrary conversion function, not a fixed set. `JSONConfig[T]` keeps the old
  decode-from-JSON behaviour available for callers that want it.
- The `go:generate pigeon` directive is omitted from `internal/library/parse.go`, so
  `go generate ./...` does not fail for anyone without pigeon installed. `expr.pigeon` is not
  vendored; the grammar has not needed to change.

## What it does

Evaluates a default-plus-ordered-overrides configuration against an open
`map[string]any` of runtime constraints, using a small boolean DSL:

```yaml
some.setting:
  defaultValue: 4
  overrides:
    - matchString: '"env" = "staging" and ("zone" = "us-west-1" or "zone" = "us-west-2")'
      matchResult: 16
```

Supported operators: `=`, `!=`, `>`, `<`, `and`, `or`, and parenthesised nesting.
Values may be quoted strings, integers, or floats. A constraint key that is absent from the
supplied map never matches.

## Known issues

- `internal/library/impl.go` `Load` writes to a bare `map` while `Eval` reads it, so
  registering a *new* key concurrently with reads is a data race. Updating an *existing*
  key is safe (`atomic.Pointer`). `../configurator_client.go` therefore builds a fresh
  `Configurator` per reload and swaps it behind a single `atomic.Pointer`, and never calls
  `Load` on a live instance. Worth fixing here rather than working around.
- The DSL has no `>=`, `<=`, `in`, `not`, regex, or percentage-rollout operators.
- There is no semver comparison: `"v" < "1.28.0"` compares lexicographically, so `1.9.0`
  reads as greater. Callers pass numeric components instead (`sdkMajor`, `sdkMinor`).
- A quoted number never equals a numeric constraint — `"deployRing" = "2"` does not match
  the integer 2, because a string literal compares against `Num`, which is 0. Write numbers
  bare.
- There is no change-notification mechanism; watching is supplied by the caller.

`internal/library` is unexported by Go's `internal` rule, so it is reachable only from
within this directory. Use the `configurator` package facade.
