# Umpire — Spec

> **Status: current architecture.** The monitor, per-entity planner/driver, action realizers,
> sparse regression compiler/executor, typed relations, semantic coverage, pairwise generation,
> and bounded trace refinement are implemented. Protocol-backed v2 is the suite default; v1
> remains explicitly selectable, while broader adapters and model-completeness work continue.

Umpire is model-based acceptance testing for Temporal: a closed loop that **drives** a running
server, **observes** it, builds an *executable model* of its entities, **judges** that model,
and **steers** toward the states worth judging — without tests hand-writing assertions.

Umpire is one system with three parts that share one model — a passive judge and an active
pair that drives:

- **Monitor — the passive judge (built, enforced).** Observes (gRPC + OTEL), models (entity
  FSMs that are executable oracles), and judges (safety/liveness rules → violations). It never
  drives. See [`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md) / [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).
- **Planner — the active brains (planning core built).** Given a target state or constraints,
  plans routes over the Monitor's model; coverage-guided fuzzing is its most advanced mode. See
  [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md).
- **Driver — the active mechanics (implemented for current workflows).** Realizes each planned
  route step as real traffic against the server through pluggable **realizers** (raw RPC/SDK,
  Omes-kitchensink in any
  SDK language, direct CHASM), injects faults, and applies input mutations. See [`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md).

The parts close a cycle: **plan → drive → observe → model → judge → steer**. The Planner plans
over the same model the Monitor builds; the Driver realizes those routes as traffic; the Monitor
judges the result. The compiled v2 protocol derives fact, lifecycle-transition, relation, and
executable-action coverage catalogs consumed through `Coverage.Unmet()`; novelty-guided fault
scheduling also exists. The server under test is the SUT.
Workloads are reused from **Omes** (kitchensink workflows; see [`UMPIRE_PRIOR_ART.md` (Omes)](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-omes-kitchen-sink-approach)) rather
than a bespoke DSL.

## Glossary

*Naming: **`*Registry`** holds what's declared before a run (rules, coverpoints, entity types); **`*State`** and logs hold what accumulates during one (the live model, facts, coverage).*

**Parts**
- **Umpire** — the whole system: the closed loop of Planner + Driver + Monitor over one shared model.
- **Monitor** — the passive half; observes traffic, maintains the model, and judges it with rules. Never drives.
- **Planner** — the active brains; plans routes over the model to reach target states, and hosts coverage-guided fuzzing.
- **Driver** — the active mechanics; realizes each planned event as real traffic and injects faults.

**Model (Monitor)**
- **Model** — the whole: every entity and its current state at runtime (the `ModelState`). What the Planner plans over and what rules read.
- **Entity** — one piece of the model: a single executable state machine / *oracle* (Workflow, WorkflowUpdate, WorkflowTask, TaskQueue, NexusOperation, …), built up from facts.
- **Fact** — a normalized unit of observation (a request, response, span event, or history event) addressed to one entity.
- **Classify** — an entity's total transition function: every (state, event) → `Advance` / `NoOp` / `Illegal`. The source of "no vacuous pass."
- **EntityRegistry** *(declared)* — the registered entity types/factories + fact importers: the model's declared shape.
- **ModelState** *(runtime)* — holds the live entities and their FSM state, routes each fact to its entity, and tracks changes by a generation counter.
- **FactLog** *(runtime)* — an append-only, queryable record of every fact.

**Judging (Monitor)**
- **Rule** — an invariant over model state. **Safety** rules must hold at every observation; **Liveness** rules must eventually hold.
- **RuleRegistry** *(declared)* — the name-validated registry of rules.
- **Violation** — a rule's output when an invariant fails; the Monitor's only product.
- **Coverpoint** — a named, interesting condition worth reaching at least once (e.g. a rule's precondition, or a notable state).
- **CoverpointRegistry** *(planned)* — a name-validated registry of arbitrary predicate coverpoints.
- **Coverage** *(implemented substrate)* — a thread-safe declared-versus-observed semantic catalog
  with deterministic snapshots and `Unmet()`; v2 derives fact, transition, relation, and action
  denominators from the compiled protocol.

**Planning (Planner) — high-level, over the model**
- **target** — the state you ask the Planner to reach, fully-qualified by entity (e.g. `WorkflowUpdate:completed`).
- **Plan** — the Planner's output: a validated set of routes to a target, checked (reviewable, replayable) before any traffic runs.
- **route** — one event-sequence within a `Plan`: a single way to reach the target.
- **Constraints** — allow/deny events or states to carve the sub-graph a `Plan` may use.

**Driving (Driver) — the primitives, against the SUT**
- **event** — an abstract model transition (`admit`, `accept`); the atomic unit the Planner sequences and the Driver realizes.
- **action** — the real traffic (an RPC, worker poll, or injected fault) the Driver produces to realize one event.
- **`Do(ctx, event)`** — the Driver's single seam: realize one event as traffic against the SUT.
- **realizer** — a concrete backend that turns an event into an action: a raw frontend **RPC / SDK** call, an **Omes-kitchensink** `TestInput` run by a worker in any SDK language, or a **direct CHASM** transition. One event alphabet, several realizers (see [`UMPIRE_PRIOR_ART.md` (Omes)](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-omes-kitchen-sink-approach)).
- **FaultInjector** — the interceptor hook the Driver uses to drop/delay/corrupt requests.
- **mutation** — a small, *labeled* perturbation of an otherwise-valid input (a malformed-but-plausible request field), used to exercise server-side validation and edge cases; distinct from a transport fault — it perturbs the *input*, not the wire.

**Cross-cutting**
- **Environment** — a named capability profile (`local-chasm`, `local-rpc`, `cicd`, `canary`) granting a subset of capabilities.
- **Capability** — a flag on the observe (`rpc`/`traces`/`internals`), drive (`rpcDrive`/`faults`/`directDrive`), or transport axis.
- **SUT** — the system under test: the real Temporal server.
- **Omes** — external kitchen-sink workflows, reused as the workload substrate.

## Goals

- **Separate actions from assertions.** The Driver drives; the Monitor judges. Each is reusable
  independently, so the same *actions* and the same *rules* run across functional tests, nightly
  runs, and canary.
- **Terse tests.** Replace per-test boilerplate — both the hand-written driver and the
  hand-written assertions — with reusable Plans and reusable rules over one model.
- **Tests as living docs.** The model + rulebook describe how a feature behaves; the coverpoint
  catalog + its coverage describe what it can be made to do.
- **Find bugs earlier.** Cheap enough to run per-PR; a foundation for later fuzzing.
- **Fault injection is first-class.** Faults (latency, drops, errors, early timers) are ordinary
  actions, like any RPC, not a bolt-on — the `FaultInjector` hook is built into the interceptor
  for exactly this. Steering the SUT into rare states is where the interesting invariants get
  exercised.
- **Negative-space coverage via input mutation.** Beyond well-formed traffic, the Driver applies
  **mutations** — small, labeled perturbations of a valid input (a malformed-but-plausible field,
  randomized within bounds) — to exercise the server's *validation* surface and edge cases. This
  is distinct from a transport fault: a fault perturbs the wire (drop/delay/corrupt), a mutation
  is a structurally-different-but-plausible *input*. The expected rejection is the oracle — the
  total model predicts it — so no hand-written per-case assertion is needed.
- **Close the loop.** The Driver manufactures the preconditions the Monitor's rules need, turning
  "are our rules even exercised?" from hope into a driven, mechanical fact.

## Non-goals (for now)

- **Coverage-guided fuzzing (the Planner's guided mode).** The Planner's fixed and exploratory
  planning is built; the guided, coverage-optimizing mode is deferred — build the deterministic
  core first, add guided fuzzing once the coverage signal it steers toward is trustworthy. The
  coverpoint catalog it needs is specced (`UMPIRE_PLAN.md`) but unbuilt.
- **Persistence / durable stores.** Model state is in-memory and per-test; a durable coverage
  store is a later concern.
- **A new proxy/interception stack.** Reuse the existing gRPC interceptor + OTEL processor seams
  and the dormant `FaultInjector`.

## Constraints

- **Observe-only judging.** The model and rules never change SUT behavior — they only read
  gRPC traffic and OTEL spans. Changing behavior is the Driver's job alone, through an
  **explicit** action channel, never a side effect of observation.
- **Rules stay dumb.** A rule queries entity state; it knows nothing of wire formats, change
  tracking, or how facts arrive. Symmetrically, an action declares intent; it knows nothing of
  how a fact is decoded.
- **Capability-honest, both directions.** Facts and actions each declare the capability they
  require; a run enables only what its **environment** grants (see *Environments & capabilities*)
  and **skips the rest explicitly** — never a silent pass, never a silently-dropped action.
- **No SDK requirement** to describe behavior — facts come from the wire/spans, not test code.
- **Deterministic & replayable.** Same seed + inputs ⇒ a run reproduces. Separate **run** from
  **eval**: capture the run, re-check offline.
- **Cheap.** Manual mode must run per-PR: synchronous span processing, no external services.
  Heavier modes (sweeps, fuzzing) are opt-in, never on the PR hot path.

## Design decisions

### Model (Monitor)

- **Facts, not calls.** Everything observed (requests, responses, span events, history events)
  is normalized into a `Fact` targeting one entity. One decoder owns wire→fact.
- **Entities are executable models, not just FSMs.** Each entity is a total transition function
  `Classify(event) → Advance | NoOp | Illegal` (the oracle inversion in `UMPIRE_PRIOR_ART.md` (SAA)). A total
  model has **no vacuous pass**: an unanticipated state or an illegal edge is a diff against the
  model, caught by one generic conformance rule — not something a human had to foresee and
  hand-write. Rules read; the model judges its own transitions.
- **Generation-based dirty tracking.** Each fact delivery bumps a counter; rules only re-examine
  entities changed since their last check. No per-tick history retained.

### Judge (Monitor)

- **Safety vs. Liveness split** (maps strong vs. eventual consistency):
  - *Safety* — must hold at every observation; violated ⇒ immediate failure.
  - *Liveness* — must eventually hold; tracked as `Pending`/`Resolve`, unresolved items become
    violations at teardown. Both derive from model annotations (terminal states, `MustProgress`)
    where possible.
- **Models plus relational invariants.** A complete model is *per-entity*, but the most valuable
  invariants are *cross-entity* (speculative task ↔ update, update ↔ its workflow's close). So
  the rulebook splits: single-entity conformance and liveness collapse into generic,
  model-derived checks; genuinely relational invariants stay bespoke rules. That cross-entity
  reach is Umpire's differentiator — a single-archetype model (SAA) has no such story.

### Plan (Planner)

- **Describe states, not steps.** You name a target state (or constraints); the Planner computes
  routes over the model graph (`Reachable`/`Cells`), failing fast if the target is unreachable
  under those constraints. A `Plan` is validated before any traffic — reviewable, diff-able,
  replayable.
- **Route modes & constraints, for coverage.** When several routes reach a target, an explicit
  mode picks which run — `shortest` (one canonical), `all` (every simple route, for route-dependent
  bugs), or `random`+seed (reproducible variation) — and `explore` roams a constrained sub-graph
  instead of naming a target. `Constraints` (allow/deny events or states, e.g. restrict a run to a
  family like Nexus + Update, or forbid a branch) carve that sub-graph and are enforced **by
  construction**, so a plan provably covers only what you asked for. Every mode is seeded, so any
  run — even a random walk — replays as a fixed regression. See [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md).
- **Planned and judged from one declaration.** The Planner's model catalog and the Monitor's
  fact-routing derive from the same entity declaration, so the drive side and the judge side can
  never disagree about which entities and states exist.

### Drive (Driver)

- **Realize events as traffic.** A route is abstract events (`admit`, `accept`); the Driver's
  single seam, `Do(ctx, event)`, maps each to real traffic (RPC / worker poll / fault).
  Eventual-consistency waits are polled to a *predicate over the model*, never slept — driven
  concurrency stays deterministic.
- **One event alphabet, several realizers.** The Driver is not a single backend. An event realizes
  through a **raw-RPC / SDK realizer** (frontend calls — the widest reach, runs in `canary`), an
  **Omes-kitchensink realizer** (compile the route to a `TestInput` executed by a worker in *any*
  SDK language — free cross-language reach without a bespoke workflow), or a **direct-CHASM realizer**
  (`directDrive`, no wire). Same planned event, a different realizer per environment or goal; the
  Monitor observes whatever ran on the wire and judges it against the *same* model. This is the
  model / realizer / observer split (see [`UMPIRE_PRIOR_ART.md` (Omes)](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-omes-kitchen-sink-approach)).
- **Events and facts are symmetric.** The Monitor decodes wire → facts; the Driver encodes a
  planned event → wire. They meet at the same `EntityPath` addressing and the same deterministic
  identifiers, so an event and the fact it provokes name the same entity.
- **Fault injection rides the existing hook.** The dormant `FaultInjector.Inject` and the
  interceptor's `inj` slot are built and wired but no-op today; the Driver is the first real
  injector (drop → error, delay → sleep-then-proceed, corrupt → mutate). Faults are events with a
  grey-box reach.
- **Input mutation is its own action class.** Separate from faults: a **mutation** replaces a valid
  request with a malformed-but-plausible one (a field perturbed, randomized within bounds) to probe
  the server's validation surface, carrying its *expected rejection* as the oracle — the total model
  predicts the reject, so negative-space coverage needs no hand-written per-case assertion. Invalid
  cases are a valid base plus exactly one *labeled* mutation, so the expected error is unambiguous
  and any failure minimizes cleanly (the discipline `UMPIRE_PRIOR_ART.md` borrows from `rapid`).

### Shared

- **One model, shared by all three.** No second state store: the `ModelState` the Monitor fills is
  what the Planner plans routes over and the Driver polls while realizing them. This is why the
  parts live together.
- **Environments & capabilities.** The model, rules, planned routes, and coverage catalog are all
  *environment-independent*. An **environment** is a named profile granting a subset of
  capabilities along three axes — **observe** (`rpc` / `traces` / `internals`), **drive**
  (`rpcDrive` / `faults` / `directDrive`), and **transport** (`inproc-noserver` / `inproc-server`
  / `remote`). Each fact source, rule, and action declares the capability it needs; a run enables
  only what its profile grants and **explicitly reports what it skips** — "not available here,"
  never a silent pass. Only the *edges* change per environment: the same event `admit` is realized
  as a CHASM transition, a frontend RPC, or a remote client call (a different `Driver`); the same
  fact `Update.admitted` is sourced from a direct component read, an OTEL span, or an RPC response
  (a different importer). black/grey/white survive only as shorthand for common observe-bundles.

  | Environment | observe | drive | transport |
  |---|---|---|---|
  | `local-chasm` | `internals` | `directDrive` (+`faults`) | in-process, no server |
  | `local-rpc` | `rpc`+`traces`+`internals` | `rpcDrive`+`faults` | in-process server |
  | `cicd` | `rpc`+`traces` | `rpcDrive` | remote |
  | `canary` | `rpc` | observe-only (opt. `rpcDrive`) | remote |

  Portability falls out: push each rule and action to the widest set of environments its
  capabilities allow.
- **Coverage is the reward signal.** Protocol-derived semantic catalogs make
  `Coverage.Unmet()` the list of declared facts, transitions, relations, or actions nobody
  reached. Arbitrary rule-precondition coverpoints remain a later extension.
- **Pluggable registries.** Rules register in a name-validated `RuleRegistry`, coverpoints in a
  `CoverpointRegistry`, and routes in a `RouteRegistry`. Adding one ≠ touching the framework.
- **Framework / domain split.** `common/testing/umpire` is generic and reusable;
  `tests/umpire2` holds the current Temporal facts, entities, protocol, actions, and rules, while
  `tests/umpirev1` remains an explicit compatibility/reference implementation.

## Shape

```
   ┌─────────┐ routes  ┌─────────┐ actions ┌──────────┐ gRPC+OTEL+(persist) ┌─────────┐
   │ Planner │────────▶│ Driver  │────────▶│  Server  │────────────────────▶│ Decoder │
   │ (brains)│(events) │(mechanic)│ +faults │  (SUT)   │  tier-gated sources │wire→fact│
   └─────────┘         └─────────┘         └──────────┘                     └─────────┘
        ▲                                                                        │ Facts
        │                                                                        ▼
        │                                       ┌──────────────────────────────────┐
        │  plans over the SAME model            │  ModelState (entity models)             │
        │                                       │  Classify: Advance/NoOp/Illegal    │
        │                                       │  (FactLog: record of every fact)   │
        │                                       └──────────────────────────────────┘
        │                                                                        │
        │                                                                        ▼
        │                                       ┌──────────────────────────────────┐
        └──── Coverage.Unmet() (reward) ◀────── │  RuleRegistry: conformance + liveness │──▶ Violations
                                                │  + relational rules                │
                                                └──────────────────────────────────┘
```
