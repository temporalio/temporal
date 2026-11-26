# failpoint

Package `failpoint` provides typed, zero-cost-when-off failpoints for Go code under test. A
failpoint lets a test force an alternate code path at a named point. Call sites look up a key in a
`*Registry`; without a registry they cost a nil check, and with an empty registry they cost one
atomic load and zero allocations.

## Declare a key

```go
var SkipCache = failpoint.NewKey[struct{}]("billing.skipCache")
```

The type parameter types the key, so call sites and tests agree on it at compile time. Each name
can be declared once; `NewKey` panics on a duplicate. `SkipCache.ForScope("tenant-1")` scopes a
key, for example to one tenant; `ExecuteSwitch` falls back to the unscoped key when no scoped
failpoint is registered.

## Execute it

Branch-style call sites use `ExecuteSwitch` to find out whether to take an alternate path:

```go
func (s *Service) Load(ctx context.Context) error {
	// s.failpoints is a *failpoint.Registry, or nil when failpoints are disabled.
	if failpoint.ExecuteSwitch(s.failpoints, SkipCache) {
		return s.loadUncached(ctx)
	}
	// ...
}
```

## Inject it in a test

```go
registry := failpoint.NewRegistry()
svc := NewService(registry)

fp := failpoint.New[struct{}](SkipCache)
fp.On()
registry.Set(fp)

// svc.Load now takes the uncached path.
```

Use `fp.Disable()` to turn a failpoint off, and `registry.Delete(key)` to detach it from the
registry.
