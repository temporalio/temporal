# Umpire sparse regressions

A sparse regression describes the semantic moments that make a behavior interesting. The compiler
synthesizes routine setup, intermediate actions, data flow, waits, and cleanup from the canonical
Umpire protocol.

```go
plan := regress.AllPaths(
	nexus.Complete("op", nexus.Succeeded),
	nexus.RespondStart("op", nexus.Async),
	nexus.State("op", nexus.Completed),
	nexus.LateStartResponseAccepted("op"),
)
```

This plan preserves “completion arrives before the async start response.” It does not prescribe
namespace creation, endpoint setup, polling, concrete IDs, or the actions needed to create the
operation. Those mechanics may evolve without changing the regression's intent.

## Authoring model

Plans contain typed instructions from domain packages under `tests/umpire2/regress`:

| Instruction | Meaning |
| --- | --- |
| Outcome | A model predicate that must be observed, such as `nexus.State` |
| Action | A semantic action that must occur, such as `nexus.Complete` |
| Relation | A typed relationship that must hold |
| Binding | A named value projected from observed model state |
| Policy | A scoped environmental behavior, such as failing the next RPC |
| Requirement | A capability the execution profile must provide |

An outcome is both a planning goal and a runtime milestone. Do not add a test-local assertion when
the expected behavior can be represented as a registered predicate or global property.

The main constructors are:

```go
regress.OnePath(instructions...)
regress.AllPaths(instructions...)
```

`OnePath` selects the canonical satisfying semantic path. `AllPaths` enumerates every satisfying
path under the compiled domain and explicit limits. Use `AllPaths` only when alternate semantic
routes are part of the regression; it is not a request for unbounded exploration.

## Symbols and values

String arguments such as `"op"`, `"caller"`, and `"handler"` are typed symbols, not concrete
Temporal IDs. First use establishes the symbol's type; later uses must agree. Different names are
distinct unless a declared relation connects them.

```go
plan := regress.OnePath(
	nexus.Start("operation", nexus.HandlerWorkflow("handler")),
	regress.Bind("run", workflow.RunID("handler")),
)
```

The runtime may ground a symbol from an action result, an observed fact, or a typed relation. This
is how plans refer to server-minted IDs and successor executions without predicting them.

Typed literals remain distinct from symbols. For example, `nexus.StartToClose(2*time.Second)` is a
duration value in the action schema, not an entity name.

## Ordering and policies

Top-level list order orders the listed key frames. The compiler may synthesize any registered
behavior between them that preserves the requested order.

Use composition only when the ordering is semantically relevant:

```go
regress.AnyOrder(a, b)              // a and b are mutually unordered
regress.During(policy, body...)     // policy is active for the synthesized body
regress.Step("a", instruction)      // label an occurrence
regress.Before("a", "b")           // add a non-local ordering edge
regress.Repeat(2, instructions...)  // request a finite repetition
```

For example, cancellation retry is expressed as a scoped fault rather than an imperative hook:

```go
plan := regress.OnePath(
	nexus.State("op", nexus.Started),
	regress.During(
		nexus.FailNext(rpc.CancelNexusOperation),
		nexus.CancelWithRetry("op"),
	),
	nexus.CancelRequestFailed("op"),
	nexus.State("op", nexus.Canceled),
)
```

Policies are registered capabilities with bounded lifetimes. Arbitrary callbacks and inline RPCs
do not belong in a sparse plan.

## Capabilities and compilation

A `Profile` names the intended environment and lists the capabilities it grants:

```go
profile := regress.Profile{
	Name:         "local",
	Capabilities: []string{capability.Faults.Name},
}

domain, err := protocol.DefaultRegressionDomain()
require.NoError(t, err)

suite, err := regress.Compile(plan, domain, profile)
require.NoError(t, err)
```

`Compile` is pure; it does not create a cluster or issue traffic. It:

1. normalizes instructions into a typed partial-order graph;
2. validates symbol use, policy scopes, and requirements;
3. regresses outcomes through registered action capabilities;
4. synthesizes resources, identities, and data flow;
5. selects or enumerates satisfying paths; and
6. validates the completed suite before execution.

`Profile.Limits.MaxPaths` is an explicit guard. Reaching it is an error rather than silent
truncation.

Compilation errors identify the source instruction and a stable category, including type
conflicts, contradictory ordering, unreachable outcomes, missing realizers or resources,
unavailable capabilities, ambiguous grounding, and unbounded enumeration. Fix the plan when its
intent is wrong; extend the canonical domain when the behavior is valid but absent from the model.

## Execution

The live harness consumes only a validated completed suite:

```go
harness := action.NewRegressionHarness(newEnvironment, artifactSink)

ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
defer cancel()
require.NoError(t, regress.Run(ctx, suite, harness))
```

Each completed path receives an isolated environment. The executor creates resources in dependency
order, installs reactive actions and policies, fires proactive actions, grounds symbols, observes
milestones, reconciles effects, checks safety, drains to quiescence, resolves final progress
obligations, and cleans up in reverse order.

`RunWithOptions` may execute paths concurrently when `MaxParallel` is positive. Concurrency changes
resource use, not suite semantics; every path remains isolated.

The working end-to-end harness setup is in
[`tests/umpire_regress_test.go`](./tests/umpire_regress_test.go). Prefer copying its environment
construction rather than inventing another cluster integration.

## Artifacts and replay

The sparse source is stable intent. It is recompiled against the current protocol on normal test
runs. A completed artifact records the model version, profile, completed paths, resource choices,
action order, grounded bindings, observations, and verdicts for diagnosis and same-version replay.

A random seed is insufficient as a durable regression because generator changes and uncontrolled
server scheduling can change its meaning. Preserve the sparse plan as the test and the completed
artifact as evidence of a particular execution.

## Choosing the right test shape

Use a sparse regression for lifecycle behavior, cross-entity relationships, retries, timeouts,
cancellation, and races whose semantics exist in the protocol.

Keep a specialized test when the contract is an exact protobuf or error detail, authorization,
performance, schema compatibility, metrics, or a low-level synchronization schedule. Do not widen
the protocol merely to hide implementation-specific assertions.

## Verification

Fast checks for authoring and compiler changes are:

```sh
go test -tags test_dep ./common/testing/umpire/regress/...
go test -tags test_dep ./tests/umpire2/regress/... ./tests/umpire2/protocol/...
go test -tags test_dep ./tests -run '^TestSparseRegression' -count=1
```

The first two commands are pure and should be run before the cluster-backed functional prefix.
