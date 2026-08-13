# Umpire — The Test Matrix (spec & plan)

How Umpire specifies **what to test** across many dimensions — model coverage, input mutations,
fault injection, environment flags, and (eventually) model interaction — without either listing
every combination by hand (too specific) or waving at "explore everything" (too vague). For the
whole-system pitch read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for the pieces this composes read the
[Actions model](./UMPIRE_ACTIONS.md), the [Error/Divergence model](./UMPIRE_ERR.md), the
[Planner](./UMPIRE_PLANNER.md), and [Trace-derived faults](./UMPIRE_TRACING.md); for status read
[`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).

> **Status: design with an implemented core.** `common/testing/umpire` now provides a deterministic,
> constraint-aware, bounded pairwise generator with explicit invalid/unsatisfiable/limit errors.
> `tests/umpire2/protocol` adds a pure Temporal adapter over declaration-ordered entity, edge,
> hosting, action-or-gap, profile, and capability data. Sparse scenario assembly, environment flags,
> and deterministic fault scheduling remain separate follow-ups.

## Why

Umpire's coverage grows in layers of depth:

| # | Layer | Exhaustive? |
|---|-------|-------------|
| 1 | Happy path only | yes |
| 2 | Modelled states incl. failures | yes |
| 3 | (2) + input mutations | mostly |
| 4 | (3) + randomized faults on observed calls | **no** |
| 5 | Multiple models + their interaction | **no** |

At layers 4–5 the space stops being exhaustively testable — **and that is fine**. What is *not*
fine is having no principled way to say which slice of the space to run. We want two testing modes
over the *same* space:

- **(a) exhaustive, regression-focused** — deterministic, CI, catches "we broke a modelled edge".
- **(b) non-exhaustive, exploratory** — seeded sampling, soak/nightly, finds bugs and gaps.

For (a) we must be able to *specify* what to explore: all modelled states, plus failure cases,
plus fault injection, plus combination with other models — **selectively**, and **within an upper
bound**. The tension: enumerating every tuple ourselves is too specific and explodes on every new
axis value; a single "run everything" knob is too vague and unbounded.

## The core reframe: one space, two traversals

(a) and (b) are not two systems. They are **the same test space walked with different strategies**:

- exhaustive → **enumerate** every point (bounded, deterministic, snapshot-able)
- exploratory → **sample** points with a seed (budget-bounded, logged for replay)

Umpire already has this duality in miniature on one axis: `AutoCoverPlans()` *enumerates* the
model-coverage axis (one plan per settling edge), while `RandomPlan(seed)` *samples* it. The whole
job is to lift that enumerate-vs-sample split from one axis to the whole matrix — not to build a
second framework.

## The space: orthogonal axes

A **test point** is one value chosen per axis. The axes:

```
model      : {NexusOperation, …}                 layer 5 adds *sets* of models
coverage   : {HappyPath, AllSettling, …}         hierarchical: expands into a *list* of base plans
mutation   : {None, DeclaredRejects, ReflectedFields}
fault      : {None, DropEachObserved, RandomDrop}
env        : {chasm:on|off, callbacks:on|off, …} dynamic-config / feature flags
hosting    : {Standalone, Embedded}
```

One subtlety: `coverage` is not a scalar. `AllSettling` *expands* into N base plans (`AutoCoverPlans`
is exactly that list). So the matrix crosses that base-plan list with the scalar axes — each base
plan × each mutation × each fault × each env × hosting.

## The spec: constrained covering arrays (the algebra)

The "logic-based expression where the computer fills in the gaps" is a known technique:
**constrained combinatorial interaction testing** — covering arrays (the PICT / ACTS / pairwise
family). We declare the variables and let the engine compute the concrete tuples. Three primitives:

**1. Per-axis selection** — never write tuples; select per axis:

```
all               every value in the domain
only{a, b}        just these
except{c}         all but these
upTo(DeclaredRejects)   an ordered prefix of the domain (None, DeclaredRejects)
```

"Test failure cases too" is `mutation: upTo(DeclaredRejects)`. "Toggle CHASM" is `env.chasm: all`.
Add a new fault kind and the matrix grows automatically — no tuple to update.

**2. Constraints** — a boolean formula over axis values that prunes illegal or uninteresting
combos. This is both correctness (some combos are nonsense) *and* the "but limit that to a subset"
lever. Two equivalent spellings; the guarded form reads better for a scoped toggle:

```
Forbid(chasm == off  AND  model ∉ {NexusOperation})
```
```
env.chasm: all   when model in {NexusOperation}
env.chasm: on    otherwise
```

The guarded selection — an axis value that depends on other axes — is the escape hatch for "not
too broad".

**3. Strength `t`** — the exhaustiveness dial:

```
Full        t = ∞   full Cartesian product
Pairwise    t = 2   cover every *pair* of axis values
Each        t = 1   cover every value at least once
Sample{seed, N}     draw N random points
```

Neither too specific (per-axis selectors + a few constraints, no tuples) nor too vague (every
domain and constraint is explicit, reviewable, and the generated point list is inspectable — the
"plan before run" property, at the suite level).

## Strength unifies exhaustive ↔ exploratory

- Layers 1–3 (small, stable): `Strength: Full` → genuinely exhaustive regression.
- Layers 4–5 (explosive): `Strength: Pairwise` or `Sample{seed}` → bounded, still high-yield.
  Pairwise catches the large majority of real interaction defects at a fraction of the product.

Same spec object; dial `t` per suite. That single knob is what makes (a) and (b) one framework.

## Bounding — three levers, and *no silent truncation*

1. **Strength** (biggest lever): pairwise instead of full product.
2. **Constraints**: prune illegal / uninteresting combos.
3. **Hard budget `N`**: in *exhaustive* mode, **assert** `enumerated ≤ N`. If the space outgrows
   the bound the suite **fails loudly** — you then either raise the bound deliberately or lower the
   strength. Growth of the test space becomes visible and intentional, never a silent explosion or a
   silent cap. (Same discipline as the `ValidateKitchensinkMappings` / `ValidateMutationCoverage`
   completeness gates, applied to matrix size.) In *exploratory* mode, `N` is just the sample count.

## A suite, sketched

```go
// (a) exhaustive regression — small, stable, deterministic, runs in CI.
Suite{
    Models:     Only(NexusOperation),
    Coverage:   AllSettling,               // expands to the settling-edge base plans
    Mutation:   UpTo(DeclaredRejects),     // {None, DeclaredRejects}
    Fault:      Only(None),
    Env:        Axis{Chasm: Both},         // {on, off}
    Hosting:    Both,
    Strength:   Full,
    Budget:     200,                        // assert: product ≤ 200, else fail
    Constraints: []Constraint{
        Forbid(Chasm(Off), Hosting(Standalone)),   // CHASM-off only meaningful embedded
    },
}

// (b) exploratory — large, seeded, budget-bounded, runs nightly/soak.
Suite{
    Models:   All,
    Coverage: AllSettling,
    Mutation: All,                          // + ReflectedFields
    Fault:    Only(RandomDropObserved),
    Env:      Axis{Chasm: Both},
    Hosting:  Both,
    Strength: Sample{Seed: 0x5eed, N: 50},  // or Pairwise
    Budget:   50,
}
```

Both produce an inspectable `[]Scenario`; the only difference is `Enumerate()` vs `Sample()`.

## Pinned coverage — anchors and the specificity ladder

Generation answers "cover the space"; it does **not** answer "guarantee *this exact path* is always
exercised" — e.g. *"start the operation, go into backoff, reschedule, backoff again, then cancel,
and fail the request call (or a persistence call)."* For that a suite also takes **anchors**:
literal, hand-written scenarios that are **always run** and **asserted covered**, with the generator
filling the rest of the space *around* them. This is the "seeding" feature of covering-array tools
(PICT supplies mandatory rows): the anchor is pinned, the array is completed to hit t-way coverage
including it. Anchors are the regression backbone; generation is the exploration around it.

There is a **specificity ladder** — pick the rung per case, and mix freely in one suite:

- **Level 1 — fully pinned.** Exact event route + exact fault. Maximum specificity, zero
  generation.
  ```go
  Anchor{
      Name:  "retry-twice-then-cancel-drop-persistence",
      Route: []string{schedule, attempt_failed, schedule, attempt_failed, schedule, cancel},
      Fault: DropAt{Call: "UpdateWorkflowExecution", Occurrence: 2}, // a persistence write, 2nd time
      Env:   Env{Chasm: On},
  }
  ```
- **Level 2 — parameterized.** A knob instead of a literal count; the "or" is a tiny enumerated
  selection (two rows). This is the sweet spot for the example above.
  ```go
  Anchor{
      Name:  "backoff-cycle-then-cancel",
      Route: RetryCycles(2).Then(Cancel),                 // expands the loop N times
      Fault: OneOf(DropRequestCall, DropPersistenceCall), // the "or" → 2 scenarios
  }
  ```
- **Level 3 — constrained.** Force routes *through* states without naming every event; the planner
  fills the exact sequence. Specific, but the computer fills the gaps.
  ```go
  Coverage: Reach(cancelled, MustPass{backing_off: AtLeast(2)}, MaxDepth(8))
  ```
- **Level 4 — generated.** `AllSettling` / `Pairwise` / `Sample` — the matrix above.

### Enforcement — "ensure it's covered" means *verified*

An anchor is not merely *included*; it is *checked*:

- The literal route is **validated against the model first** — `PlanRoute` fails fast if the
  sequence is not a legal walk. So writing a specific path doubles as a **reachability assertion**:
  if `backing_off → scheduled → backing_off → cancel` is not a legal walk, the framework says so, and
  you either add the edge or fix the route (never a silent skip).
- After the run, the coverage report + `Reconcile` **assert the anchor's route and fault actually
  fired** — not "we queued it" but "it ran and conformed".
- Anchors are **exempt from the sampling budget** (always run) and tracked separately, so
  exploration can never crowd out a pinned regression.

### Two gaps this exposes

The example lands squarely on the paths that are not atomically drivable today — worth stating:

1. **Cyclic / retry routes bind a reactive handler policy, not an atomic action.** `backing_off →
   scheduled` (the retry reschedule) and `started → timed_out` have no atomic `actionFor` mapping —
   they are driven by the programmable handler (`ResponsePolicy`) cycling K times. So "loop twice"
   is a *handler parameter* (`retryableTimes: 2`), which `ResponsePolicy` can carry via an attempt
   counter. `PlanRoute` must recognize these edges and bind the policy instead of an `actionFor`
   lookup.
2. **Occurrence-targeted faults need an index.** Faults today are method-scoped and *first
   occurrence* (`armFault`'s `seen.Add(1) > 1`). "Fail the request call *on the second attempt*" or
   "fail a specific persistence op" needs targeting by `(method, occurrence)` or `(method,
   at-state)` — a small, bounded extension to `armFault`.

### `PlanRoute` — the first primitive (sketch)

`PlanEdge(from, event, hosting)` already maps a *computed* event list to actions; generalize it to a
**literal, possibly cyclic** sequence — the caller supplies the whole walk, validated step by step:

```go
// PlanRoute maps a literal event sequence (which may revisit states — a cyclic walk the
// shortest-route planners never emit) to an action sequence, validating each step against the
// model as it goes. It is PlanEdge without the "route to `from`" prefix: the caller pins the whole
// walk, so "schedule, attempt_failed, schedule, attempt_failed, schedule, cancel" is expressed
// verbatim. Edges with no atomic action (the retry reschedule, started→timed_out) bind a reactive
// handler policy instead of an actionFor lookup.
func PlanRoute(events []string, hosting umpire.Hosting) ([]umpire.Action, error) {
    lc, _ := planner.DefaultModels().Lifecycle(string(model.NexusOperationType))
    state := lc.Initial()
    seq := make([]umpire.Action, 0, len(events))
    for _, ev := range events {
        e, ok := lc.Edge(state, ev) // the (from,event) edge; !ok = illegal walk → fail fast
        if !ok {
            return nil, fmt.Errorf("route illegal: %s has no --%s--> edge", state, ev)
        }
        a, ok := realize(state, ev, hosting) // actionFor, else a reactive policy (retry/timeout)
        if !ok {
            return nil, fmt.Errorf("no realization for %s --%s-->", state, ev)
        }
        seq = append(seq, a)
        state = e.To // thread state via the edge, not a global Destination (correct under cycles)
    }
    return seq, nil
}
```

## Mapping onto existing code — an addition, not a rewrite

The matrix layer sits *above* the plan layer and reuses every existing mechanism as an axis:

| Axis | Already exists as | Role |
|------|-------------------|------|
| model | `model.NexusOperation` lifecycle | which FSM |
| coverage | `action.AutoCoverPlans()` (enumerate) / `action.RandomPlan(seed)` (sample) | base-plan list |
| mutation | `action.StartFieldVariants()`, `StartUnknownEndpoint` ([`UMPIRE_ERR.md`](./UMPIRE_ERR.md)) | plan decorator |
| fault | probe `FaultEachObservedCall()` / `MaxFaults()`; `action.Drop/Hold` ([`UMPIRE_TRACING.md`](./UMPIRE_TRACING.md)) | plan decorator |
| env | `testcore.WithDynamicConfig(dynamicconfig.EnableChasm, …)` | env builder |
| hosting | `umpire.Standalone` / `umpire.Embedded` | plan constraint |
| anchor | `action.PlanEdge()` → generalize to `PlanRoute()`; `ResponsePolicy` retry knob; `probe.InjectDropOn()` | pinned literal scenario |

The planner's `Constraints{AllowEvents, DenyEvents, DenyStates, Grants, Hosting}` is *already*
axis-selection for the model dimension — the matrix is the same idea one level up. `TestProbeNexus`
`Exploration` (near-exhaustive enumerate) and `TestProbeNexusRandomized` (sample) are the two
traversals done by hand today; the matrix generalizes them.

New pieces only: an `Axis`/`Selector`/`Constraint`/`Suite` core (pure, no server), and a
`Scenario = base plan + mutation decorator + fault decorator + env` assembler that hands each
scenario to the runtime that already exists.

## The layer-5 caveat: model interaction

Combining models needs genuinely new machinery: the base "plan" becomes an *interleaving schedule*
of two entities' actions, and interleavings explode — exhaustiveness truly dies here, so it is
sampling-only, and the covering-array frame degrades to property-based generation over schedules.
The axis frame still holds (a `models: SetOf{…}` axis), but the cross-model scheduler is the hard,
separate design. Deliberately out of scope for the first cut.

## Prior art

- **Combinatorial interaction testing / covering arrays** (PICT, ACTS/NIST): the per-axis +
  constraints + strength model, and t-way (pairwise) reduction. This is the direct ancestor.
- **Property-based testing** (QuickCheck lineage): the sample side — seeded generators, shrinking,
  replay from seed. See the schedule PBT in [`UMPIRE_PRIOR_ART.md`](./UMPIRE_PRIOR_ART.md).
- **Bounded model checking vs. randomized testing**: the same enumerate-vs-sample duality this doc
  is built on.

## Plan

Phased, each phase independently useful and testable. Pure core first, server wiring later.

- **Phase 1 — Matrix core (pure).** `Axis`, `Selector` (`All/Only/Except/UpTo`), `Constraint`
  (`Forbid` + guarded selection), `Strength` (`Full/Pairwise/Each/Sample`), `Budget`, and
  **anchors** (mandatory pinned rows, included before generation and asserted separately).
  `Enumerate()` and `Sample(seed)` return an ordered, inspectable `[]Point`. Unit tests + a golden
  test of a generated point list (a matrix change shows up as a reviewable diff). No server.
  Budget-assert built in.
- **Phase 2 — Scenario assembler + `PlanRoute`.** `Point → Scenario`: cross `AutoCoverPlans()` base
  plans with the mutation and fault decorators and the hosting constraint; reuse
  `Drive`/`Reconcile`/probe. Generalize `PlanEdge` → **`PlanRoute(events, hosting)`** for literal,
  cyclic anchor routes (validated against the model — an illegal walk fails fast), binding a
  reactive `ResponsePolicy` for the retry / `started→timed_out` edges that have no atomic action.
  Prove one exhaustive suite (layers 1–3, `Strength: Full`) reproduces today's exploration coverage,
  and one anchor pins a specific multi-cycle route.
- **Phase 3 — Env-flag axis, guarded constraints, targeted faults.** Add the `env` axis (CHASM
  on/off, callbacks) wired to `WithDynamicConfig`, make the guarded-selection / `Forbid` constraints
  real, and extend `armFault` to **occurrence / at-state targeting** (`DropAt{Call, Occurrence}`) so
  an anchor can fail a specific call. The CHASM-toggle-limited-to-a-subset example and the
  "retry-twice-then-cancel-drop-persistence" anchor both generate and run.
- **Phase 4 — Two suites, one space.** Stand up the exhaustive regression suite (CI, `Full`, small
  layers, hard budget) and the exploratory suite (soak, `Sample`/`Pairwise`, budget). Coverage
  accumulates across the whole matrix; a matrix-size gate mirrors the completeness gates.
- **Phase 5 — Pairwise generator.** A real t-way covering-array builder (constraint-aware) for the
  large layer-4 space, so bounded runs still hit every pair.
- **Phase 6 (future) — Model interaction.** The `models: SetOf{…}` axis and a cross-model
  interleaving scheduler (layer 5). Sampling-only; property-based over schedules.

## Open questions

- **Shrinking.** When an exploratory sample fails, do we minimize the failing point (drop axes
  toward the smallest still-failing scenario) before reporting? High value for bug triage; not
  needed for the first cut.
- **Golden matrices vs. drift.** Snapshot the enumerated point list of each exhaustive suite? Makes
  intent-changes reviewable, but adds a fixture to maintain. Lean yes for regression suites.
- **Constraint expressiveness.** Start with `Forbid(term…)` conjunctions + guarded selection; only
  add full boolean formulae if a real case needs it.
- **Coverage-guided sampling.** Should `Sample` bias toward points that would exercise uncovered
  edges (tie-in with the Planner's coverage-guided mode, [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md))
  rather than uniform random? A natural Phase-5+ upgrade.
