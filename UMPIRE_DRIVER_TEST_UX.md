# Umpire — Driver: how a developer writes a test

The developer-facing guide to the Driver (Umpire's active half). For the broader architecture read
[`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md); for the model these tests plan over read
[`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md) and [`SAAMODEL.md`](./SAAMODEL.md).

## The one idea: describe states, not steps

The Monitor already lets you *judge* by declaring invariants over entity **states**. The Driver is
the mirror image: you *drive* by declaring the **states you want reached**, and it computes how to
get there. You never hand-write the sequence of RPCs — you name a target and it plans a route
through the model graph (the same `umpire.Lifecycle` graph, via `Reachable`/`Cells`).

```
Monitor: observed state ── judged by ──▶ invariants   (declare what must hold)
Driver:  target state   ── planned by ──▶ routes       (declare what to reach)
```

This is why a test is "mostly" deterministic: the **target states are guaranteed**, but because
you described a state and not a path, the model may admit several routes to it. Which route(s) to
take is an explicit knob (below), not an accident.

Two kinds of test fall out: **fixed** (known regressions — reach these exact states) and
**exploratory** (constrained — roam this problem space).

## Addressing states: one catalog, fully-qualified targets

You don't fetch a model by hand. `driver.DefaultModels()` is the catalog of every entity model,
and you name a target **fully-qualified by entity** — `("WorkflowUpdate", "completed")`. Both the
catalog and the Monitor's fact-routing derive from one declaration, `model.DefaultEntities()`, so
the drive side and the judge side can never disagree about which entities exist ("one model, two
consumers" — `UMPIRE_DRIVER.md`); adding an entity is a one-line change in one place. This matters
because state names are shared across entities: both `Workflow` and `WorkflowUpdate` have a
`completed` state, so `"completed"` alone is ambiguous. Qualifying by entity resolves it, validates
the state against that model, and is the addressing scheme cross-entity targets will extend.

```go
models := driver.DefaultModels()

wf, _  := models.PlanTo("Workflow", "completed", driver.Shortest, driver.Constraints{})
// wf.Routes  == [[start complete]]
upd, _ := models.PlanTo("WorkflowUpdate", "completed", driver.Shortest, driver.Constraints{})
// upd.Routes == [[admit complete]]
```

(The lower-level `driver.PlanTo(lc, …)` that takes a raw `*umpire.Lifecycle` still exists — the
catalog is a thin, generic-free façade over it — but tests should use the catalog.)

## Plan before run (the core property)

Every Driver run produces an inspectable **`Plan`** before it touches the server. A `Plan` is a
concrete set of routes (event sequences) over the model, and it is validated at plan time:

- if the target isn't a state of the model, or is unreachable under your constraints, planning
  **fails fast** — before a single RPC;
- the routes provably stay inside your constraints, because the planner builds them only from
  edges the constraints allow (constraints are enforced *by construction*, not checked after).

That makes a plan reviewable, diff-able, and replayable. (The randomization caveat is at the end:
fixed-target plans are fully pre-checkable; exploration plans enforce constraints by construction
and record their realized route for replay.)

## Kind 1 — Fixed regression tests

Name the target state; the Driver finds the route. This is a *known regression*: the developer is
assured that state gets exercised, or the test fails at plan time.

```go
func TestUpdateReachesCompleted(t *testing.T) {
	models := driver.DefaultModels()

	plan, err := models.PlanTo("WorkflowUpdate", "completed", driver.Shortest, driver.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"admit", "complete"}}, plan.Routes)

	require.NoError(t, plan.Run(ctx, act)) // the Monitor judges the resulting traffic
}
```

### Route mode — one route, all routes, or a random one

When the model admits several routes to the target, the mode decides which are driven:

| Mode | Behaviour | Use it for |
|---|---|---|
| `driver.Shortest` (default) | one canonical shortest route; deterministic | fast, reproducible regressions |
| `driver.AllRoutes` | every simple route to the target | route-dependent bugs ("does it matter *how* we got accepted?") |
| `driver.Random` + `WithSeed(n)` | one route chosen by the seed; reproducible | cheap variation without exhaustive cost |

```go
// Exercise every distinct way an update can reach "completed".
plan, err := models.PlanTo("WorkflowUpdate", "completed", driver.AllRoutes, driver.Constraints{})
require.NoError(t, err)
require.ElementsMatch(t, [][]string{
	{"admit", "complete"},           // accept + complete in one workflow task
	{"admit", "accept", "complete"}, // separate accept, then complete
}, plan.Routes)
```

`AllRoutes` is how a "mostly deterministic" test becomes *fully* deterministic: it pins down every
route, not just the target.

## Kind 2 — Constrained exploration

Instead of a target, give **constraints** that carve a sub-graph, and let the Driver roam it.
Exploration is reproducible (seeded) and cannot leave the sub-graph.

```go
func TestExploreUpdateHappyPath(t *testing.T) {
	models := driver.DefaultModels()

	plan, err := models.Explore("WorkflowUpdate", driver.Constraints{
		DenyEvents: []string{"reject", "abort"}, // stay on the success paths
		MaxDepth:   5,
	}, driver.WithSeed(1))
	require.NoError(t, err)

	require.NoError(t, plan.Run(ctx, act)) // the Monitor judges what the walk produces
}
```

`Constraints` are the shared vocabulary for both kinds — they also shape Kind-1 routes:

```go
type Constraints struct {
	AllowEvents []string // if set, only these events may be used
	DenyEvents  []string // these events may never be used
	DenyStates  []string // routes may never enter these states
	MaxDepth    int      // cap route / walk length (0 = graph-bounded)
}
```

## The Actuator — turning a plan into real traffic

A `Plan`'s routes are *abstract model events* (`"admit"`, `"accept"`, …). An `Actuator` realizes
each one as real traffic against the SUT; it is the single seam between the pure planner and a live
server.

```go
type Actuator interface {
	Do(ctx context.Context, event string) error
}
```

A real Temporal actuator is constructed for one target entity and maps events onto RPCs / worker
polls / fault injection:

```go
// Illustrative — the concrete actuator is the next thing to build (see status).
type updateActuator struct {
	client         workflowservice.WorkflowServiceClient
	wfID, updateID string
}

func (a *updateActuator) Do(ctx context.Context, event string) error {
	switch event {
	case "admit":    return a.startAndSendUpdate(ctx)
	case "accept":   return a.pollAndAccept(ctx)
	case "complete": return a.pollAndComplete(ctx)
	// ...
	}
	return fmt.Errorf("updateActuator: unhandled event %q", event)
}
```

The full loop a developer writes:

```go
models := driver.DefaultModels()
plan, err := models.PlanTo("WorkflowUpdate", "completed", driver.Shortest, driver.Constraints{})
require.NoError(t, err)               // (1) plan + validate, no traffic yet
require.NoError(t, plan.Run(ctx, act)) // (2) drive it
env.Monitor().CheckNamespace(ctx, ns)  // (3) the Monitor judges — no hand-written assertions
```

## Other kinds worth considering

Ranked by value, these reuse the same plan-over-the-model core:

1. **Replay & minimize** — the bridge from exploration to regression. When a Kind-2 walk trips a
   violation, freeze its realized route into a Kind-1 fixed `Plan` (and delta-debug it to the
   minimal route). Exploration *finds*; replay *locks it in*. This is the most important loop to
   support and mostly falls out of `Plan` being serializable.
2. **Negative / reachability assertions** — assert a state is *unreachable* under constraints.
   This needs **no execution at all**: it is just planning and expecting failure, i.e. using the
   planner as a small model checker.
   ```go
   _, err := models.PlanTo("WorkflowUpdate", "accepted", driver.Shortest,
       driver.Constraints{DenyEvents: []string{"accept"}})
   require.Error(t, err) // "accepted" is unreachable without the accept event
   ```
3. **Coverage-goal tests** — "exercise *every* cell/edge in sub-graph Z at least once." The
   `Cells()` denominator from `SAAMODEL.md` #2 becomes the goal; the Driver plans a covering set
   of routes. Directly attacks dead-rule detection.
4. **Fault overlays** — take any fixed or exploratory plan and inject faults (drop/delay/error,
   early timer) at chosen edges: "same route to completed, but crash between accept and complete."
   A cross-product of route × fault schedule, riding the dormant `FaultInjector` hook.
5. **Differential / parity plans** — drive the *same* plan through two actuators (two surfaces or
   two builds) and assert equivalence, as SAA does for SAA↔WFA.
6. **Soak / fuzz** — a long, loosely-constrained `Explore` walk to shake out rare interleavings;
   seed-logged so any failure replays as a fixed regression (kind 1).

## The randomization caveat (answering "can plans always be checked?")

There is a spectrum:

- **Fixed-target plans** (`Shortest`/`AllRoutes`) are concrete routes — fully pre-checkable: we
  verify they reach the target and use only allowed edges before running.
- **`Random` and `Explore`** cannot enumerate the future, so constraints are enforced *as the walk
  is built* (the planner only ever steps along allowed edges) rather than checked afterward, and
  the **realized route is recorded** so it is both post-hoc checkable and replayable as a fixed
  plan. So even the random modes give a plan you can trust and reproduce — just enforced by
  construction rather than by enumeration.

## Status

- **Built and tested today** (`tests/umpire/driver`, no server needed): the `DefaultModels()`
  catalog (fully-qualified `(entity, state)` targets) and the planner it fronts —
  `PlanTo` (Shortest/AllRoutes/Random), `Explore`, `Constraints` (enforced by construction),
  fail-fast reachability, and `Plan.Run` over an `Actuator`. The examples above are taken from the
  package's passing unit tests.
- **The next seam**: a concrete Temporal `Actuator` (events → RPCs / worker polls), wired to the
  same client and namespace the Monitor scopes, so `Run` drives a real cluster and the Monitor judges.
- **The hard next step**: **cross-entity targets** ("an update `accepted` while its workflow is
  `completed`") — the interesting bugs. Planning over the *product* of entity graphs needs the
  cross-entity coupling that today's per-entity models don't carry; it ties into the "enrich the
  event alphabet / fold correlations into a model" work in `UMPIRE_PLAN.md`. Single-entity targets
  work now; the multi-entity planner is future.
