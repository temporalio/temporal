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

## BUGGIFY

`registry.SetBuggify(cfg)` enables FoundationDB-style BUGGIFY: with `cfg.Seed`, each key name that
has nothing registered is activated for the whole run with probability `Activation`, in every
scope, and each execution of an activated key fires with probability `Fire`. Both are pure
functions of the seed, the name and, for firing, the scope and the number of the execution in that
scope, so a seed reproduces the activated keys without a shared random stream, and one scope's
executions don't change which executions fire in another. `Prefixes` override the probabilities per name prefix (longest wins) and name a catalog
error, which `Execute`/`Checkpoint` of an `error` return when the key fires. `Include`/`Exclude`
select names with `*` patterns.

BUGGIFY is meant for whole-system runs, such as a test cluster or a deterministic simulation,
enabled by the consumer at process start, not for tests that assert exact behavior.
`failpoint.BuggifyConfigFromEnv(os.LookupEnv)` reads `FAILPOINT_BUGGIFY_SEED` (a number, `random`,
or `gomad` to derive it from `GOMADSEED`) and the optional `FAILPOINT_BUGGIFY_ACTIVATION` and
`FAILPOINT_BUGGIFY_FIRE`; consumers apply their own prefixes and patterns on top and log the seed.

```go
registry.SetBuggify(failpoint.BuggifyConfig{
	Seed: seed, Activation: 0.25, Fire: 0.25,
	Prefixes: []failpoint.BuggifyPrefix{{Prefix: "rpc:", Activation: 0.1, Fire: 0.01, Error: "Unavailable"}},
})

if failpoint.Buggify(s.hooks, SmallBatch) { // or BuggifyWithProb(src, key, p)
	batchSize = 1 // Legal but unlikely: exercise the multi-batch path.
}
```

A registered failpoint always takes precedence over BUGGIFY for its key, even while disabled;
`Buggify` then reports whether it fired with a value, as `Active` does. Call sites that execute
several keys for one operation use `ExecuteRegistered` for each key and `BuggifyError` last.
`registry.SuppressBuggify()` turns BUGGIFY off for tests that assert exact behavior. Subscribers see
a `BuggifyActivationEvent` per name and a `BuggifyFireEvent` per execution that fired;
`registry.BuggifyStats()` and the `ListFailpoints` RPC report each name's activation, hits and
fires (activated names first, within a size limit), so a run can prove that it reached the keys it
claims to stress. While
nothing is registered and BUGGIFY is off, `registry.Armed()` is false and all of these cost one
atomic load.

To configure failpoints in another process, serve the registry over gRPC with
[`failpointgrpc`](failpointgrpc).
