# buggify

Package `buggify` implements FoundationDB-style BUGGIFY on top of a failpoint registry. With a seed,
it activates a random subset of failpoint keys for the whole run, and makes a random subset of their
executions fire: an activated key returns an error at `failpoint.Checkpoint`, or takes an unusual but
legal branch at `buggify.Fires`.

BUGGIFY is meant for whole-system runs, such as a test cluster or a deterministic simulation, enabled
at process start. It is not meant for tests that assert exact behavior.

## Enable it

Install BUGGIFY as the registry's fallback and give it a seed:

```go
b := buggify.For(registry) // Installs BUGGIFY, disabled.
err := b.Set(buggify.Config{
	Seed:       seed,
	Activation: 0.25, // A key is activated for the run with this probability.
	Fire:       0.25, // An execution of an activated key fires with this probability.
})
```

To configure it from the environment:

```go
cfg, err := buggify.ConfigFromEnv(os.LookupEnv)
```

| Variable | Value |
|---|---|
| `FAILPOINT_BUGGIFY_SEED` | A number; `random` to pick one; `gomad` to derive it from `GOMADSEED`. Unset disables BUGGIFY. |
| `FAILPOINT_BUGGIFY_ACTIVATION` | Optional activation probability, default 0.25. |
| `FAILPOINT_BUGGIFY_FIRE` | Optional fire probability, default 0.25. |

Apply your own `Prefixes`, `Include` and `Exclude` on top of the result, and log the seed.

## Add a call site

Use `Fires` for a branch that is legal but unlikely, such as a smaller batch or an extra retry:

```go
var SmallBatch = failpoint.NewKey("billing.smallBatch")

if buggify.Fires(s.hooks, SmallBatch) { // Or FiresWithProb(s.hooks, SmallBatch, 0.01).
	batchSize = 1 // Exercise the multi-batch path.
}
```

Never use it for a state that cannot happen in reality.

## Inject errors at existing failpoints

Name an error from the catalog (see `failpoint.RegisterError`) to make an activated key fail
`failpoint.Checkpoint` and the other `Execute` functions of an `error`. Without an error, keys only
fire at `Fires` call sites.

```go
b.Set(buggify.Config{
	Seed: seed, Activation: 0.25, Fire: 0.25,
	Prefixes: []buggify.Prefix{
		{Prefix: "rpc:", Activation: 0.1, Fire: 0.01, Error: "Unavailable"},
	},
	Exclude: []string{"rpc:/admin.*"},
})
```

The longest prefix of the key name sets the probabilities and the error for a key. `Include` and `Exclude`
select key names with `*` patterns; an empty `Include` selects every name, and `Exclude` wins.

## Reproduce a run

Every decision is a pure function of the seed, the key name and, for firing, the scope and the
number of the execution in that scope. So:

- The same seed activates the same keys.
- One scope's traffic doesn't change which executions fire in another.
- Which executions fire also depends on the order of executions within a scope, which is only
  reproducible under a deterministic scheduler.

## Force or suppress it in a test

A failpoint registered for a key always takes precedence over BUGGIFY, even while disabled:

```go
fp := failpoint.NewFailpoint[error](SmallBatch)
fp.On(failpoint.Always) // buggify.Fires(..., SmallBatch) now always returns true.
registry.Set(SmallBatch, fp)
```

`b.Suppress()` turns BUGGIFY off until its returned `restore` is called.

## Observe a run

- Subscribers of the registry receive a `failpoint.FallbackActivationEvent` per key name and a
  `failpoint.FallbackFireEvent` per execution that fired.
- `b.FallbackStats()` and the `ListFailpoints` RPC of [`failpointgrpc`](../failpointgrpc) report each
  key's activation, hits and fires, so a run can show it reached the keys it claims to stress.
