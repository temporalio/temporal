# Umpire — Spec

Umpire is model-based acceptance testing for Temporal: a closed loop that **drives** a running
server, **observes** it, builds an *executable model* of its entities, **judges** that model,
and **steers** toward the states worth judging — without tests hand-writing assertions.

Umpire has two halves that share one model:

- **Monitor — the passive half (built, enforced).** Observes (gRPC + OTEL), models (entity
  FSMs that are executable oracles), and judges (safety/liveness rules → violations). It never
  drives. See [`UMPIRE_ABOUT.md`](./UMPIRE_ABOUT.md) / [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).
- **Driver — the active half (specced, unbuilt).** Drives the server, injects faults, and
  steers it into the states the Monitor rules on. See [`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md).

The halves close a cycle: **drive → observe → model → judge → steer**. The Driver reads the
same model the Monitor builds (action guards are predicates over entity state, not sleeps), and
the Monitor's coverage catalog is the Driver's reward signal (what states nobody has reached
yet). A deferred **strategist** (a coverage-guided fuzzer) will later choose which actions to
drive; the server under test is just the SUT. Workloads are reused from **Omes** (kitchensink
workflows; see [`OMES.md`](./OMES.md)) rather than a bespoke DSL.

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

- **Smart, coverage-guided exploration (the strategist).** Build the deterministic driver
  (replayable, seeded actions) first; add the strategist once there is a reward signal to
  optimize toward.
- **Coverage-guided fuzzing.** The Scenario/Coverage catalog is specced (`UMPIRE_PLAN.md`) and is
  the seam the strategist will optimize toward, but it is unbuilt.
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
- **Tier-honest, both directions.** Facts carry an observation *tier* and actions carry a matching
  *reach* (black / grey / white box). A run runs only what its deployment supports and **skips
  the rest explicitly** — never a silent pass, never a silently-dropped action.
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

### Drive (Driver)

- **Actions are predicate-guarded, not clocked.** An action fires when a *predicate over the
  Monitor's model* is true (`Update.Reached("admitted")`), not after a timer. Eventual-consistency
  waits become structural, not flaky sleeps.
- **Actions and facts are symmetric.** The Monitor decodes wire → facts; the Driver encodes
  intent → wire. They meet at the same `EntityPath` addressing and the same deterministic
  identifiers, so an action and the fact it provokes name the same entity.
- **Fault injection rides the existing hook.** The dormant `FaultInjector.Inject` and the
  interceptor's `inj` slot are built and wired but no-op today; the Driver is the first real
  injector (drop → error, delay → sleep-then-proceed, corrupt → mutate). A grey-box reach.

### Shared

- **One model, two consumers.** No second state store: the `Registry` the Monitor fills is the
  `Registry` the Driver queries. This is why the two halves live together.
- **Observation tiers — black / grey / white box.** Facts carry a provenance *tier* (frontend
  gRPC = black; internal RPC + OTEL = grey; persistence = white); actions carry a matching
  *reach*. A run enables only channels ≤ its tier: the flagship lifecycle rules and black-box
  actions run in canary/Cloud, while grey/white channels stay functional-test-only. One model,
  tier-gated — portability is the axis a white-box-only model doesn't address.
- **Coverage is the reward signal.** The Scenario/Coverage catalog (planned, `UMPIRE_PLAN.md`)
  turns a rule's precondition into a coverage target; `Coverage.Unmet()` is the list of
  interesting states nobody reached — the seam the Driver (and later the strategist) steers
  toward.
- **Pluggable registries.** Rules register in a name-validated `Rulebook`; actions and scenarios
  get parallel registries. Adding one ≠ touching the framework.
- **Framework / domain split.** `common/testing/umpire` is generic and reusable; `tests/umpire`
  holds all Temporal specifics (entities, facts, rules, and — later — actions).

## Shape

```
        ┌────────── strategist (deferred: coverage/fuzz-guided) ─────────────┐
        ▼                                                                     │
   ┌─────────┐  actions (RPCs)   ┌──────────┐  gRPC + OTEL + (persist)  ┌─────────┐
   │ Driver  │ ────────────────▶ │  Server  │ ────────────────────────▶ │ Decoder │
   │ (drive) │  faults (Inject)  │  (SUT)   │   tier-gated fact sources │ wire→fact│
   └─────────┘                   └──────────┘                           └─────────┘
        ▲                                                                     │ Facts
        │                                                                     ▼
        │                                          ┌──────────────────────────────────┐
        │                                          │  Registry (entity models)         │
        │                                          │  Classify: Advance/NoOp/Illegal    │
        │                                          │  (FactLog: record of every fact)   │
        │                                          └──────────────────────────────────┘
        │                                                                     │
        │                                                                     ▼
        │                                          ┌──────────────────────────────────┐
        └───── Coverage.Unmet() (reward signal) ◀── │  Rulebook: generic conformance    │──▶ Violations
                guards read the SAME model          │  + liveness + relational rules     │
                                                    └──────────────────────────────────┘
```
