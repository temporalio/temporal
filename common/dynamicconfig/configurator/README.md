# configurator (vendored)

Vendored copy of https://github.com/davidporter-id-au/configurator at commit `4c50e59`.

## Why vendored rather than a `go.mod` dependency

The upstream `go.mod` declares the module path
`github.com/davidporter-id-au/constraints-config`, which does not match the repository URL
(`github.com/davidporter-id-au/configurator`). `go get` therefore cannot resolve it. Until
upstream renames the module and tags a release, the source is vendored here.

Import paths have been rewritten from `github.com/davidporter-id-au/constraints-config/...`
to `go.temporal.io/server/common/dynamicconfig/configurator/...`. No other changes have been
made to the upstream source.

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

## Known upstream issues (worked around by the adapter, not patched here)

- `internal/library/impl.go` `LoadKey` writes to a bare `map` while `Eval` reads it, so
  registering a *new* key concurrently with reads is a data race. Updating an *existing*
  key is safe (`atomic.Pointer`). `../configurator_evaluator.go` therefore builds a fresh
  `Configurator` per reload and swaps it behind a single `atomic.Pointer`, and never calls
  `LoadKey` on a live instance.
- The DSL has no `>=`, `<=`, `in`, `not`, regex, or percentage-rollout operators.
- There is no change-notification mechanism; watching is supplied by the caller.

`internal/library` is unexported by Go's `internal` rule, so it is reachable only from
within this directory. Use the `configurator` package facade.
