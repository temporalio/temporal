# buggify

Package `buggify` implements seeded randomized fault activation (BUGGIFY) as a `failpoint.Policy` on
a failpoint registry. With a seed, it activates a random subset of failpoint keys for the whole run,
and makes a random subset of their executions fire: an activated key returns an error at
`failpoint.CheckError`, or takes an alternate branch at `failpoint.ExecuteSwitch`.

BUGGIFY is meant for whole-system runs, such as a test cluster or a deterministic simulation, enabled
at process start. It is not meant for tests that assert exact behavior.

## Enable it

Create a `Buggify` bound to a registry and give it a seed and an allowlist of keys:

```go
b := buggify.New(registry) // Starts disabled.
err := b.Set(buggify.Config{
	Seed: seed, // Zero disables BUGGIFY; a seed needs Keys.
	Keys: []buggify.Key{
		{Match: "billing.*", ActivationProbability: 0.25, FireProbability: 0.25},
	},
})
```

BUGGIFY applies only to the keys that `Keys` lists. `Match` is a key name in which `*` matches any text
anywhere in the name; there is no other syntax. The first entry that matches a name sets its
probabilities (a key is activated for the run with `ActivationProbability`, and an execution of an
activated key fires with `FireProbability`) and its error. A name that matches no entry is never
buggified, and there is no default list or probability. `Set` rejects a seed without keys, an empty or
duplicate `Match`, a probability outside [0, 1] and an error that is not registered.

The package reads no environment variable and no file: build the `Config` from your process's own
configuration, and log the seed.

## Add a call site

Call sites only use `failpoint`—they never import `buggify`. Use `failpoint.ExecuteSwitch` for a
branch that is legal but unlikely, such as a smaller batch or an extra retry:

```go
var SmallBatch = failpoint.NewKey[bool]("billing.smallBatch")

if failpoint.ExecuteSwitch(s.hooks, SmallBatch) {
	batchSize = 1 // Exercise the multi-batch path.
}
```

Never use it for a state that cannot happen in reality.

A key can have a scope, such as a namespace, so a test can target one instance with a failpoint.
BUGGIFY's decisions are numbered and drawn per scope, so a scope that changes between runs, such as
the ID of a namespace that the test recreates, changes which executions fire.
`failpoint.ExecuteSwitchWithPolicyScope(src, key, scope)` gives the policy a restart-stable scope
(such as the name of the namespace), while failpoints registered for `key` still match by `key.Scope()`.

## Inject errors at existing failpoints

Name an error from the catalog (see `failpoint.RegisterError`) to make an activated key fail
`failpoint.CheckError` and the other `Execute` functions of an `error`. Without an error, keys only
fire at switch call sites.

```go
b.Set(buggify.Config{
	Seed: seed,
	Keys: []buggify.Key{
		// A narrow entry comes before the broad one that would match it: the admin API never fails.
		{Match: "rpc:/admin.*", ActivationProbability: 0},
		{Match: "rpc:*", ActivationProbability: 0.1, FireProbability: 0.01, Error: "Unavailable"},
	},
})
```

Order the entries from narrow to broad: the first match wins, so an entry after a broad one that
matches the same names never applies.

## Reproduce a run

Every decision is a pure function of the seed, the key name and, for firing, the scope and the
number of the execution in that scope. So:

- The same seed activates the same keys.
- One scope's traffic doesn't change which executions fire in another, for up to 65536
  `(key name, scope)` pairs, counted across all activated key names of a run. A run with more
  pairs reports an `EventScopeLimit`; the pairs beyond the bound share one counter per key name,
  and the `ExecutionNumber` of their `EventFire` events is the number from that shared counter.
- The scope is part of every fire decision, so a replay needs the same scope identities: a scope
  that a run names differently fires differently with the same seed. The activated keys don't
  depend on the scope. A namespace that is created again gets a new ID, so call sites scope
  the policy by the namespace name (see `failpoint.ExecuteSwitchWithPolicyScope`), which stays the same.
- Which executions fire also depends on the order of executions within a scope, which is only
  reproducible under a deterministic scheduler.

## Force or suppress it in a test

A failpoint registered for a key always takes precedence over BUGGIFY, even one without an action:

```go
fp := failpoint.NewFailpoint(SmallBatch)
fp.On() // failpoint.ExecuteSwitch(..., SmallBatch) now always returns true.
registry.Set(fp)
```

## Observe a run

`b.OnDecision(fn)` sets an observer that receives an `EventActivation` per key name and an
`EventFire` per execution that fired, so a run can show which keys it reached.
Events arrive in the order that BUGGIFY decided them, one at a time, and the activation of a key
precedes its fires. The observer can execute keys, including the key of its event: the events of
those executions are delivered after it returns, and an execution doesn't wait for an observer that
another goroutine runs. So an event can be delivered on another goroutine than the execution that
it is about, and after that execution returned.
