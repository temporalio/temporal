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
`ExecuteCtx[T]` to look the failpoint up in a context (see `InjectRegistry`).

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

To configure failpoints in another process, serve the registry over gRPC with
[`failpointgrpc`](failpointgrpc).
