# failpoint

Package `failpoint` provides typed, zero-cost-when-off fault injection points for Go code under
test. Call sites look up a key in a `Source` registry; without a registry they cost a nil check,
and with an empty registry they cost one atomic load and zero allocations.

This package is the core substrate for both seeded random fault injection (BUGGIFY, added in the
next PRs) and targeted deterministic tests.

## Declare a key

```go
var ChargeCard = failpoint.NewKey[error]("billing.chargeCard")
```

The type parameter is the type of the value that the failpoint returns, so call sites and tests
agree on it at compile time. Each name can be declared once; `NewKey` panics on a duplicate.
`ChargeCard.ForScope("tenant-1")` scopes a key, for example to one tenant; `Execute` falls back to
the unscoped key when no scoped failpoint is registered.

## Execute it

```go
func (s *Service) Charge(ctx context.Context) error {
	// s.failpoints implements failpoint.Source: Failpoints() returns the registry, or nil.
	if err := failpoint.CheckError(s.failpoints, ChargeCard); err != nil {
		return err
	}
	// ...
}
```

`CheckError` returns the injected error for an error failpoint. Use `Execute` for other value
types, and `ExecuteSwitch` for branch-style call sites that only need to know whether to take an
alternate path:

```go
if failpoint.ExecuteSwitch(s.failpoints, SkipCache) {
	return s.loadUncached(ctx)
}
```

## Inject it in a test

```go
// A *Registry implements failpoint.Source.
registry := failpoint.NewRegistry()
svc := NewService(registry)

fp := failpoint.NewFailpoint[error](ChargeCard)
fp.Return(errors.New("card declined"))
registry.Set(fp)

require.EqualError(t, svc.Charge(ctx), "card declined")
```

Use `fp.On()` for switch-style failpoints, `fp.Disable()` to turn a failpoint off, and
`registry.Delete(key)` to detach it from the registry.

## Fallback

A registry can have a `Fallback` that decides for executions of keys with nothing registered, for
the key or its unscoped key. `registry.SetFallbackIfAbsent(f)` installs it and
`registry.SetFallbackActive(f, true)` turns it on. `Execute` and `CheckError` then consult
`f.Error(key)`. A registered failpoint always takes precedence, even after `Disable`. While
nothing is registered and no fallback is active, `registry.MayFire()` is false and executions cost
one atomic load.
