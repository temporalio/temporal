# Umpire — Spec

Umpire is model-based acceptance testing for Temporal: a closed loop that **drives** a running
server, **observes** it, builds an *executable model* of its entities, **judges** that model,
and **steers** toward the states worth judging — without tests hand-writing assertions.

Umpire is one system with three parts that share one model — a passive judge and an active
pair that drives:

- **Monitor — the passive judge (built, enforced).** Observes (gRPC + OTEL), models (entity
  FSMs that are executable oracles), and judges (safety/liveness rules → violations). It never
  drives. See [`UMPIRE_ABOUT.md`](./UMPIRE_ABOUT.md) / [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).
- **Planner — the active brains (planning core built).** Given a target state or constraints,
  plans routes over the Monitor's model; coverage-guided fuzzing is its most advanced mode. See
  [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md).
- **Driver — the active mechanics (specced, unbuilt).** Realizes each planned route step as real
  traffic against the server, and injects faults. See [`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md).

The parts close a cycle: **plan → drive → observe → model → judge → steer**. The Planner plans
over the same model the Monitor builds; the Driver realizes those routes as traffic; the Monitor
judges the result; and the Monitor's coverage catalog (`Coverage.Unmet()` — the states nobody
reached) is what the Planner's guided mode steers toward. The server under test is just the SUT.
Workloads are reused from **Omes** (kitchensink workflows; see [`OMES.md`](./OMES.md)) rather
than a bespoke DSL.

## Goals

- **Separate actions from assertions.** The Driver drives; the Monitor judges. Each is reusable
  independently, so the same *actions* and the same *rules* run across functional tests, nightly
  runs, and canary.
- **Terse tests.** Replace per-test boilerplate — both the hand-written driver and the
  hand-written assertions — with reusable scenarios and reusable rules over one model.
- **Tests as living docs.** The model + rulebook describe how a feature behaves; the scenario
  catalog + coverage catalog describe what it can be made to do.
- **Find bugs earlier.** Cheap enough to run per-PR; a foundation for later fuzzing.
- **Fault injection is first-class.** Faults (latency, drops, errors, early timers) are ordinary
  actions, like any RPC, not a bolt-on — the `FaultInjector` hook is built into the interceptor
  for exactly this. Steering the SUT into rare states is where the interesting invariants get
  exercised.
- **Close the loop.** The Driver manufactures the preconditions the Monitor's rules need, turning
  "are our rules even exercised?" from hope into a driven, mechanical fact.

## Non-goals (for now)

- **Coverage-guided fuzzing (the Planner's guided mode).** The Planner's fixed and exploratory
  planning is built; the guided, coverage-optimizing mode is deferred — build the deterministic
  core first, add guided fuzzing once the coverage signal it steers toward is trustworthy. The
  Scenario/Coverage catalog it needs is specced (`UMPIRE_PLAN.md`) but unbuilt.
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
  `Classify(event) → Advance | NoOp | Illegal` (the oracle inversion in `SAAMODEL.md`). A total
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
- **Planned and judged from one declaration.** The Planner's model catalog and the Monitor's
  fact-routing derive from the same entity declaration, so the drive side and the judge side can
  never disagree about which entities and states exist.

### Drive (Driver)

- **Realize events as traffic.** A route is abstract events (`admit`, `accept`); the Driver's
  single seam, `Do(ctx, event)`, maps each to real traffic (RPC / worker poll / fault).
  Eventual-consistency waits are polled to a *predicate over the model*, never slept — driven
  concurrency stays deterministic.
- **Events and facts are symmetric.** The Monitor decodes wire → facts; the Driver encodes a
  planned event → wire. They meet at the same `EntityPath` addressing and the same deterministic
  identifiers, so an event and the fact it provokes name the same entity.
- **Fault injection rides the existing hook.** The dormant `FaultInjector.Inject` and the
  interceptor's `inj` slot are built and wired but no-op today; the Driver is the first real
  injector (drop → error, delay → sleep-then-proceed, corrupt → mutate). Faults are events with a
  grey-box reach.

### Shared

- **One model, shared by all three.** No second state store: the `Registry` the Monitor fills is
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
- **Coverage is the reward signal.** The Scenario/Coverage catalog (planned, `UMPIRE_PLAN.md`)
  turns a rule's precondition into a coverage target; `Coverage.Unmet()` is the list of
  interesting states nobody reached — the seam the Planner's guided-fuzz mode steers toward.
- **Pluggable registries.** Rules register in a name-validated `Rulebook`; actions and scenarios
  get parallel registries. Adding one ≠ touching the framework.
- **Framework / domain split.** `common/testing/umpire` is generic and reusable; `tests/umpire`
  holds all Temporal specifics (entities, facts, rules, and — later — actions).

## Shape

```
   ┌─────────┐ routes  ┌─────────┐ actions ┌──────────┐ gRPC+OTEL+(persist) ┌─────────┐
   │ Planner │────────▶│ Driver  │────────▶│  Server  │────────────────────▶│ Decoder │
   │ (brains)│(events) │(mechanic)│ +faults │  (SUT)   │  tier-gated sources │wire→fact│
   └─────────┘         └─────────┘         └──────────┘                     └─────────┘
        ▲                                                                        │ Facts
        │                                                                        ▼
        │                                       ┌──────────────────────────────────┐
        │  plans over the SAME model            │  Registry (entity models)         │
        │                                       │  Classify: Advance/NoOp/Illegal    │
        │                                       │  (FactLog: record of every fact)   │
        │                                       └──────────────────────────────────┘
        │                                                                        │
        │                                                                        ▼
        │                                       ┌──────────────────────────────────┐
        └──── Coverage.Unmet() (reward) ◀────── │  Rulebook: conformance + liveness │──▶ Violations
                                                │  + relational rules                │
                                                └──────────────────────────────────┘
```
