# failpoint

Package `failpoint` provides typed, zero-cost-when-off fault injection points for Go code under
test. Call sites look up a key in a `*Registry`; without a registry they cost a nil check,
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

Code that builds names at run time, such as one key per request method, reserves a prefix with
`ReserveKeyPrefix[T]` and builds keys with the returned `KeyPrefix`.

## Execute it

```go
func (s *Service) Charge(ctx context.Context) error {
	// s.failpoints is a *failpoint.Registry, or nil when failpoints are disabled.
	if err := failpoint.CheckError(s.failpoints, ChargeCard); err != nil {
		return err
	}
	// ...
}
```

`CheckError` returns the injected error for an error failpoint. Use `Execute` for other value
types, `ExecuteInOrder` to evaluate ordered keys (and scopes in precedence order) from a single
registry snapshot and pass an argument to `CallWithArg` callbacks, and `ExecuteSwitch` for
branch-style call sites that only need to know whether to take an alternate path:

```go
if failpoint.ExecuteSwitch(s.failpoints, SkipCache) {
	return s.loadUncached(ctx)
}
```

## Inject it in a test

```go
registry := failpoint.NewRegistry()
svc := NewService(registry)

fp := failpoint.NewFailpoint[error](ChargeCard)
fp.Return(errors.New("card declined"), failpoint.Trigger{MaxFires: 1})
registry.Set(fp)

require.EqualError(t, svc.Charge(ctx), "card declined")
```

Each action setter (`Return`, `Panic`, `Call`, `Block`, `Pause`, `On`) accepts an optional
`Trigger` (`SkipFirst`, `MaxFires`) to limit which executions fire. Without one, the failpoint
fires on every execution. `fp.SetTrigger(t)` replaces the trigger without changing the action (for
`Block` it also releases held executions and rearms), and `fp.Stats()` / `fp.TotalFires()` inspect
execution and fire counts.

Use `fp.On()` for switch-style failpoints, `fp.Block()` + `fp.Wait(ctx)` to synchronize on an
execution reaching a point, `fp.Pause(d)` to delay executions, `fp.Disable()` to turn a failpoint
off and release held executions, `registry.Swap(fp)` when replacing an entry that may hold blocked
executions, and `registry.Delete(key)` to detach it from the registry.

## Default policy (BUGGIFY)

`Registry.SetPolicy` installs a `Policy` that decides `CheckError` and `ExecuteSwitch` call sites
when no explicit failpoint is registered for a key. Explicitly registered failpoints always take
precedence over the policy, even when disabled. Subpackage [`buggify`](./buggify) implements a
seeded probabilistic policy on top of this interface.
