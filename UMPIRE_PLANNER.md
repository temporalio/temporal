# Umpire — Planner (the brains): spec & how a developer writes a test

> **Status: component reference; implemented for per-entity route planning.** The pure planner,
> action-plan integration, and sparse regression compiler are built. Cross-entity authoring is
> available in sparse regressions, and the protocol now has a typed runtime relation model;
> relation-aware general planning remains planned.

The Planner is Umpire's active **brains**. Where the **Driver**
([`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md)) is the mechanics that turn abstract events into real
traffic, the Planner decides *what* to reach and *how*: given a target state or a set of
constraints, it plans routes over the Monitor's model. Coverage-guided fuzzing is the Planner's
most advanced mode, not a separate subsystem.

For the whole-system pitch read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for the model these plans
run over read [`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md) and [`UMPIRE_PRIOR_ART.md` (SAA)](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-the-saa-behavioral-model).

## The one idea: describe states, not steps

The Monitor lets you *judge* by declaring invariants over entity **states**. The Planner is the
mirror image: you *drive* by declaring the **states you want reached**, and the Planner computes
how to get there. You never hand-write the sequence of RPCs — you name a target and it plans a
route through the model graph (the same `umpire.Lifecycle` graph, via `Reachable`/`Cells`).

```
Monitor:  observed state ── judged by ──▶ invariants   (declare what must hold)
Planner:  target state  ── planned by ──▶ routes       (declare what to reach)
Driver:   route event   ── realized as ──▶ real traffic (see UMPIRE_DRIVER.md)
```

A test is "mostly" deterministic: the **target states are guaranteed**, but because you
described a state and not a path, the model may admit several routes to it. Which route(s) to
take is an explicit knob (below), not an accident.

Two kinds of test fall out: **fixed** (known regressions — reach these exact states) and
**exploratory** (constrained — roam this problem space).

## Addressing states: one catalog, fully-qualified targets

You don't fetch a model by hand. `planner.DefaultModels()` is the catalog of every entity model,
and you name a target **fully-qualified by entity** — `("WorkflowUpdate", "completed")`. Both the
catalog and the Monitor's fact-routing derive from one declaration, `model.DefaultEntities()`, so
the drive side and the judge side can never disagree about which entities exist ("one model, two
consumers"); adding an entity is a one-line change in one place. This matters because state names
are shared across entities: both `Workflow` and `WorkflowUpdate` have a `completed` state, so
`"completed"` alone is ambiguous. Qualifying by entity resolves it, validates the state against
that model, and is the addressing scheme cross-entity targets will extend.

```go
models := planner.DefaultModels()

wf, _  := models.PlanTo("Workflow", "completed", planner.Shortest, planner.Constraints{})
// wf.Routes  == [[start complete]]
upd, _ := models.PlanTo("WorkflowUpdate", "completed", planner.Shortest, planner.Constraints{})
// upd.Routes == [[admit complete]]
```

(The lower-level `planner.PlanTo(lc, …)` that takes a raw `*umpire.Lifecycle` still exists — the
catalog is a thin, generic-free façade over it — but tests should use the catalog.)

## Plan before run (the core property)

Every run produces an inspectable **`Plan`** before it touches the server. A `Plan` is a
concrete set of routes (event sequences) over the model, validated at plan time:

- if the target isn't a state of the model, or is unreachable under your constraints, planning
  **fails fast** — before a single RPC;
- the routes provably stay inside your constraints, because the Planner builds them only from
  edges the constraints allow (constraints are enforced *by construction*, not checked after).

That makes a plan reviewable, diff-able, and replayable. (The randomization caveat is at the end:
fixed-target plans are fully pre-checkable; exploration plans enforce constraints by construction
and record their realized route for replay.)

## Kind 1 — Fixed regression tests

Name the target state; the Planner finds the route. This is a *known regression*: the developer
is assured that state gets exercised, or the test fails at plan time.

```go
func TestUpdateReachesCompleted(t *testing.T) {
	models := planner.DefaultModels()

	plan, err := models.PlanTo("WorkflowUpdate", "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"admit", "complete"}}, plan.Routes)

	require.NoError(t, plan.Run(ctx, driver)) // Monitor judges the resulting traffic
}
```

### Route mode — one route, all routes, or a random one

When the model admits several routes to the target, the mode decides which are driven:

| Mode | Behaviour | Use it for |
|---|---|---|
| `planner.Shortest` (default) | one canonical shortest route; deterministic | fast, reproducible regressions |
| `planner.AllRoutes` | every simple route to the target | route-dependent bugs ("does it matter *how* we got accepted?") |
| `planner.Random` + `WithSeed(n)` | one route chosen by the seed; reproducible | cheap variation without exhaustive cost |

```go
// Exercise every distinct way an update can reach "completed".
plan, err := models.PlanTo("WorkflowUpdate", "completed", planner.AllRoutes, planner.Constraints{})
require.NoError(t, err)
require.ElementsMatch(t, [][]string{
	{"admit", "complete"},           // accept + complete in one workflow task
	{"admit", "accept", "complete"}, // separate accept, then complete
}, plan.Routes)
```

`AllRoutes` is how a "mostly deterministic" test becomes *fully* deterministic: it pins down every
route, not just the target.

## Kind 2 — Constrained exploration

Instead of a target, give **constraints** that carve a sub-graph, and let the Planner roam it.
Exploration is reproducible (seeded) and cannot leave the sub-graph.

```go
func TestExploreUpdateHappyPath(t *testing.T) {
	models := planner.DefaultModels()

	plan, err := models.Explore("WorkflowUpdate", planner.Constraints{
		DenyEvents: []string{"reject", "abort"}, // stay on the success paths
		MaxDepth:   5,
	}, planner.WithSeed(1))
	require.NoError(t, err)

	require.NoError(t, plan.Run(ctx, driver)) // Monitor judges what the walk produces
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

## Handing off to the Driver

A `Plan`'s routes are *abstract model events* (`"admit"`, `"accept"`, …). `plan.Run(ctx, driver)`
walks the route and asks the Driver to realize each event as real traffic. The Driver is the
single seam between the pure Planner and a live server — its contract is one method,
`Do(ctx, event)`, detailed in [`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md). The full loop a developer
writes:

```go
models := planner.DefaultModels()
plan, err := models.PlanTo("WorkflowUpdate", "completed", planner.Shortest, planner.Constraints{})
require.NoError(t, err)              // (1) plan + validate, no traffic yet
require.NoError(t, plan.Run(ctx, d)) // (2) Driver drives it
env.Umpire().CheckNamespace(ctx, ns) // (3) Monitor judges — no hand-written assertions
```

## Other kinds worth considering

Ranked by value, these reuse the same plan-over-the-model core:

1. **Replay & minimize** — the bridge from exploration to regression. When a Kind-2 walk trips a
   violation, freeze its realized route into a Kind-1 fixed `Plan` (and delta-debug it to the
   minimal route). Exploration *finds*; replay *locks it in*. The most important loop to
   support, and it mostly falls out of `Plan` being serializable.
2. **Negative / reachability assertions** — assert a state is *unreachable* under constraints.
   This needs **no execution at all**: it is just planning and expecting failure, i.e. using the
   Planner as a small model checker.
   ```go
   _, err := models.PlanTo("WorkflowUpdate", "accepted", planner.Shortest,
       planner.Constraints{DenyEvents: []string{"accept"}})
   require.Error(t, err) // "accepted" is unreachable without the accept event
   ```
3. **Coverage-goal tests** — "exercise *every* cell/edge in sub-graph Z at least once." The
   `Cells()` denominator from `UMPIRE_PRIOR_ART.md` (SAA) #2 becomes the goal; the Planner plans a covering set
   of routes. Directly attacks dead-rule detection.
4. **Fault overlays** — take any fixed or exploratory plan and inject faults (drop/delay/error,
   early timer) at chosen edges: "same route to completed, but crash between accept and complete."
   A cross-product of route × fault schedule, riding the Driver's `FaultInjector` seam.
5. **Differential / parity plans** — drive the *same* plan through two Drivers (two surfaces or
   two builds) and assert equivalence, as SAA does for SAA↔WFA.
6. **Soak / fuzz** — a long, loosely-constrained `Explore` walk to shake out rare interleavings;
   seed-logged so any failure replays as a fixed regression (Kind 1). This is the Planner's most
   advanced mode: coverage-guided choice steered by the Monitor's `Coverage.Unmet()`.

## Modes, mapped to the Monitor

The Planner scales from one concrete regression up to guided fuzzing, reusing the same core; each
mode has a Monitor counterpart that judges the result.

| Mode | What you provide | Runs | Monitor counterpart |
|---|---|---|---|
| **Fixed** | a target state (Kind 1) | per-PR | passive model (done) |
| **Coverage goal** | targets + a covering-set goal | per-PR / nightly | rules + coverpoints |
| **Exploration** | constraints + seed (Kind 2) | nightly GHA | coverage report |
| **Guided fuzz** | constraints + coverage hints | nightly GHA | `Coverage.Unmet()` as reward |

## Closing the loop with the Monitor

The active and passive halves feed each other:

- **Monitor → Planner:** the model (to plan routes over) and the coverage catalog
  (`Coverage.Unmet()` — the interesting states nobody reached, the target set the guided-fuzz
  mode optimizes toward).
- **Planner → Monitor:** it manufactures the *preconditions* the Monitor's rules need.
  `UMPIRE_PLAN.md` warns that a rule whose precondition is never reached "passes vacuously and
  gives false confidence" (it names `ContinueAsNew` as almost certainly never firing today). The
  Planner reaches those preconditions *on purpose* — turning "are our rules even exercised?" from
  hope into a driven, mechanical fact. That is the single highest-value thing it does before any
  fuzzing: **make the existing rules non-vacuous.**

## The randomization caveat (answering "can plans always be checked?")

There is a spectrum:

- **Fixed-target plans** (`Shortest`/`AllRoutes`) are concrete routes — fully pre-checkable: we
  verify they reach the target and use only allowed edges before running.
- **`Random` and `Explore`** cannot enumerate the future, so constraints are enforced *as the walk
  is built* (the Planner only ever steps along allowed edges) rather than checked afterward, and
  the **realized route is recorded** so it is both post-hoc checkable and replayable as a fixed
  plan. So even the random modes give a plan you can trust and reproduce — just enforced by
  construction rather than by enumeration.

## Status

- **Built and tested today** (`tests/umpire2/protocol` and the preserved
  `tests/umpirev1/planner` compatibility surface): fully-qualified `(entity, state)` targets,
  compiled lifecycle/action catalogs, and planning through
  `PlanTo` (Shortest/AllRoutes/Random), `Explore`, `Constraints` (enforced by construction),
  fail-fast reachability, and `Plan.Run` over a `Driver`. The examples above are taken from the
  package's passing unit tests.
- **The concrete seam is implemented**: Temporal action realizers drive RPC, SDK worker,
  kitchensink, timer/fault, and sparse-regression traffic in the same namespace the Monitor
  scopes. See [`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md).
- **The hard next step**: **cross-entity targets** ("an update `accepted` while its workflow is
  `completed`") — the interesting bugs. Planning over the *product* of entity graphs needs the
  cross-entity coupling that today's per-entity models don't carry; it ties into the "enrich the
  event alphabet / fold correlations into a model" work in `UMPIRE_PLAN.md`. Single-entity targets
  work now; the multi-entity Planner is future.
