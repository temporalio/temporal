# failpoint

Package `failpoint` injects faults and timing control at named points in code under test. A
failpoint can return a value, panic, pause, block until released, or call a callback.

Failpoints only do anything when the code under test enables them. `Execute` and `Checkpoint`
look the failpoint up in the registry of a `Source`; without a registry they are no-ops.
`ExecuteCtx` looks it up in the registry attached to the context, and also fires failpoints that a
test injected directly into the context with `InjectReturn` and the other `Inject*` helpers; with
neither, it is a no-op.

The consumer enables failpoints by providing a registry, for example only in test builds, and
leaves it out everywhere else.

## Declare a key

```go
var ChargeCard = failpoint.NewKey("billing.chargeCard")
```

Each name can be declared once; `NewKey` panics on a duplicate. `ChargeCard.ForScope("tenant-1")`
scopes a key, for example to one tenant; `Execute` falls back to the unscoped key.

## Execute it

```go
func (s *Service) Charge(ctx context.Context) error {
	// s.hooks implements failpoint.Source: Failpoints() returns the registry, or nil.
	if err := failpoint.Checkpoint(s.hooks, ChargeCard); err != nil {
		return err
	}
	// ...
}
```

`Checkpoint` returns the error of an error failpoint. Use `Execute[T]` for other return types, and
`ExecuteCtx[T]` to look the failpoint up in a context (see `InjectRegistry`). `ExecuteWith[T]` passes
an argument, such as the request, to a callback configured with `fp.CallWith(func(arg any) T, ...)`.
`ExecuteWithFired[T]` also reports whether the failpoint fired with any action, such as a pause.

## Inject it in a test

```go
registry := failpoint.NewRegistry()
svc := NewService(hooksWith(registry))

fp := failpoint.NewFailpoint[error](ChargeCard)
fp.Return(errors.New("card declined"), failpoint.Once)
registry.Set(ChargeCard, fp)

require.EqualError(t, svc.Charge(ctx), "card declined")
```

For a failpoint in a single context, without a registry:

```go
ctx, _ := failpoint.InjectReturn(ctx, ChargeCard, errors.New("card declined"), failpoint.Once)
err, _ := failpoint.ExecuteCtx[error](ctx, ChargeCard) // card declined
```

## Switch-style failpoints

A call site that only needs to know whether to take a fault path checks `Active`:

```go
if failpoint.Active(s.hooks, SkipCache) {
	return s.loadUncached(ctx)
}
```

`fp.On(failpoint.Always)` makes the failpoint active. `fp.SetEnabled(false)` suspends any failpoint
without losing its configuration, trigger or counts; it releases held blocks.

## Declarative rules

A `RuleSet` keeps the failpoints of a registry in line with a list of `Rule`s, for example ones
loaded from a configuration file:

```go
set := failpoint.NewRuleSet(registry, "billing rules")
defer set.Close()
err := set.Apply([]failpoint.Rule{
	{Key: "billing.chargeCard", Scope: "tenant-1", Action: failpoint.Return, Error: "Unavailable"},
	{Key: "billing.refund", Action: failpoint.Call, Behavior: "slowRefund"},
	{Key: "billing.skipCache", Action: failpoint.On, Trigger: failpoint.Trigger{Probability: 0.1}},
})
```

Rules name errors and behaviors registered with `RegisterError` and `RegisterBehavior`. `Apply`
creates, reconfigures and removes the failpoints that the set created; an unchanged rule keeps its
failpoint's state. It returns an error and changes nothing if a rule is invalid or names the key of
a failpoint that other code registered. The set's failpoints report the set's source as their
`Owner`, and the `failpointgrpc` service refuses to change them.

To configure failpoints in another process, serve the registry over gRPC with
[`failpointgrpc`](failpointgrpc).
