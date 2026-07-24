# Umpire — Driver (the active half): spec & plan

The active half of Umpire. Where the **Monitor** (the passive half) **observes, models, and
judges** a running Temporal server, the **Driver** **drives** it: it generates actions, injects
faults, and steers the system into the states the Monitor is there to rule on.

For the whole-system pitch read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for the passive half read
[`UMPIRE_ABOUT.md`](./UMPIRE_ABOUT.md) and [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).
This document is a plan, not built code — it names the pieces, the seams they plug
into, and the order to build them.

## The one sentence

> The Monitor judges behaviour; the Driver is the half that drives it — made reusable,
> generative, and fault-aware.

Today "the driver" is hand-written functional-test code plus SDK pollers. The
Driver's job is to become that driver as a first-class, reusable subsystem so the
same *actions* run across functional tests, nightly exploration, and canary — the
same way the same *rules* already do.

---

## Where it fits: Umpire's two halves

Umpire is one system with two halves that share one model. The Monitor is built and enforced;
the Driver is this document.

| Part | Role | Status |
|---|---|---|
| **Monitor** | observe → model → judge; its `Registry` *is* the shared model | built |
| **Driver** | drives actions, injects faults, steers the SUT into judged states | **this doc** |
| **strategist** | later: chooses which actions to drive (coverage-guided fuzzing) | deferred |
| **Omes** | external kitchensink workloads, reused as the driving substrate | reused |

The Driver's vocabulary, plainly:

| Term | Meaning |
|---|---|
| **action** | the atomic thing the Driver does — one RPC, one injected latency, one dropped request, one fired timer. |
| **flow** | a composed, guarded sequence of actions ("Start → admit Update → drop WFT → poll"), with input parameters. |
| **catalog** | the whole library of flows the Driver can run (name-validated, mirrors the `Rulebook`). |
| **run** | one full driving session against a live server, under a seed, evaluated against a coverage goal. |
| **coverage goal** | the set of scenarios a run *must* exercise. |
| **strategist** | the deferred chooser that decides which flows/actions to run (fuzzing/coverage-guided). |

> The Driver is a **mechanical executor**; the strategist is the brain. Build the executor
> first (deterministic, replayable actions), grow the brain (strategist) once there is
> a reward signal to be smart about — which is exactly what the Monitor's coverage
> catalog provides (see *Closing the loop*).

## The loop the Driver closes

The Monitor's pipeline runs left-to-right and stops at violations. The Driver wraps it
into a cycle: **drive → observe → model → judge → steer**.

```
        ┌──────────────────── strategist (later: coverage/fuzz-guided) ──────────────────────┐
        │                                                                                     │
        ▼                                                                                     │
   ┌─────────┐  actions (RPCs)   ┌──────────┐   gRPC+OTEL   ┌─────────┐  Facts  ┌──────────┐  │
   │ Driver  │ ────────────────▶ │  Server  │ ────────────▶ │ Monitor │ ──────▶ │ Registry │  │
   │ (drive) │  faults (Inject)  │  (SUT)   │               │(observe)│         │  (model) │  │
   └─────────┘ ◀──── predicates read the SAME model ──────────────────────────  └──────────┘  │
        │                                                              │                       │
        │                                                          Rulebook ─▶ Violations      │
        └───────── Coverage.Unmet() = "what states we still haven't reached" ──────────────────┘
```

Two properties fall out of this shape and drive the whole design:

1. **The Driver reads the model the Monitor builds.** Action guards are *predicates
   over entity state* (`Update.Reached("admitted")`), not sleeps. This is the
   TigerBeetle/VOPR idea: run a "perfect network" that handles the boring auxiliary
   traffic, then write the interesting scenario in a prolog-ish *predicate* style —
   "drop all WFTs, wait until the update is admitted, then …".
2. **The Monitor's coverage catalog is the Driver's reward signal.** The Scenario /
   Coverage subsystem planned in `UMPIRE_PLAN.md` exists precisely so the active side
   has targets: `Coverage.Unmet()` is the list of interesting states nobody reached.
   That is the seam the strategist optimises toward.

---

## Goals

- **Actions as a reusable subsystem.** Lift "the driver" out of per-test code into
  composable actions/flows, so one scenario runs per-PR, nightly, and in canary —
  mirroring the reuse the rulebook already has.
- **Separate action from assertion (the other half).** The Monitor delivered the
  assertion half of this split; the Driver delivers the action half. Together they
  make STAMP's "deconstructed test" real: Scenario (given) / Action (when) /
  Property (then) are three independent, recombinable things.
- **Predicate-guarded driving.** An action fires when a *predicate over the model* is
  true, not after a timer. This is what makes concurrency scenarios expressible and
  deterministic instead of flaky.
- **Fault injection as a native action.** Latency, errors, drops, reordering,
  premature deadline/timer firing are actions like any RPC — not a separate bolt-on.
- **Terse, parameterised scenarios.** A flow is data. Parameterisation (the STAMP
  pain point) becomes "iterate the flow over an input range," not copy-paste tests.
- **Generative on ramp.** The same flow definitions feed: one concrete example
  (manual), a sampled sweep (nightly), and eventually fuzzing (strategist). No rewrite
  between modes.
- **Reuse Omes kitchensink workflows** as the workload, rather than a bespoke DSL, to
  keep long-term maintenance cost down.

## Non-goals (for now)

- **The strategist / smart exploration.** Coverage- and code-guided fuzzing is the
  end state (STAMP phase 4), not the start. Build the deterministic executor first.
- **Replacing the Monitor's rules.** The Driver drives and may *declare* which
  scenarios it intended to hit; it does not judge. Judgement stays in the rulebook.
- **A new persistence/proxy stack.** Reuse the existing gRPC interceptor + OTEL
  processor seams. (A durable coverage store — e.g. SQLite — is a later concern, not v1.)
- **Rewriting every functional test at once.** Prove terseness on one, like the
  Monitor's plan does, before a sweep.

## Constraints

- **Deterministic & replayable.** Given the same seed and inputs, a run must
  reproduce. This is what buys flaky-test *verification* and reproducible bug repro.
  Requires: seeded randomness, stable/derived identifiers (as the Monitor already
  has), and separating **run** from **eval** (capture the run, re-check offline).
- **Tier-honest (same discipline as the Monitor).** Actions have a *reach* just as
  facts have a *provenance tier*:
  - **Black-box actions** — only the public frontend API (Start/Signal/Update/Poll/
    Describe/GetHistory). Runnable **anywhere**, including canary/Cloud.
  - **Grey/White-box actions** — fault injection at internal RPC / persistence /
    timing seams. Functional-test & test-cell only.
  A flow must declare its max reach so a canary run literally cannot schedule a
  white-box action — the exact mirror of the Monitor's "never silently skip" rule.
- **Cheap enough for per-PR** in its manual mode; heavier modes are opt-in
  (nightly/GHA), never on the PR hot path.
- **Must not fight the SUT's own consistency model.** Every action/result is labelled
  **strong** or **eventual**; eventual results are polled to a predicate, never slept
  on. (STAMP: this label may live at the var level.)
- **No new third-party libraries** without asking; reuse the SDK, the existing
  interceptor, and Omes.

---

## Core concepts

```
Action  = { Reach (black/grey/white), Guard predicate(model)->bool, Do(ctx, client, faults) }
Flow    = ordered/guarded []Action + input parameters + declared Reach + intended Scenarios
Catalog = registry of Flows (name-validated, mirrors Rulebook/ScenarioBook)
Run     = one execution of a selected set of Flows against a live server, under a seed
Goal    = the Scenarios a Run must cover (coverage goal); asserted against the Monitor's Coverage
strategist = (later) chooses Flows/actions/params to maximise Coverage.Unmet() reduction
```

- **An action is guarded by a predicate, not a clock.** `Guard` reads the Monitor's
  `Registry` (the shared model) and returns true when the system is in the state
  this action wants to act on. The runner polls guards; eventual-consistency waits
  are structural, not `time.Sleep`. (Directly the STAMP note: *"actions are guarded
  by props; they can only fire when all props are true."*)
- **An action declares its reach** so tier-gating is mechanical: a black-box run
  filters the catalog to black-box flows.
- **Invalid variants for validation.** Each action trigger exposes an `Invalid…`
  form (STAMP note): where a flow would send a valid request, send the malformed one
  instead and assert the specific service error. Free negative-space coverage.
- **A flow declares the scenarios it intends to reach.** At run end this is
  cross-checked against the Monitor's `Coverage`: declared-but-never-detected ⇒ the
  flow's model of the world is wrong (same check the Monitor already specs for
  declared scenarios).

---

## Design decisions

- **Actions and facts are symmetric.** The Monitor has a `FactDecoder` that turns
  wire/spans **into** facts. The Driver has the inverse: actions turn intent
  **into** wire calls. They meet at the same `EntityPath` addressing and the same
  deterministic identifiers, so an action on `Workflow(id).Update(id)` and the fact it
  provokes name the same entity. Reuse `entity_key.go`.
- **Guards read the shared model directly — one model, two consumers.** No second
  state store. The `Registry` the Monitor fills is the `Registry` the Driver queries.
  This is the whole reason the two halves live together.
- **Fault injection rides the dormant hook that already exists.** The framework's
  `FaultInjector.Inject(ctx, info, request) error` and the interceptor's
  `inj` slot (`common/testing/umpire/interceptor.go`, `NewUnaryServerInterceptor`)
  are built and wired but no-op. The Driver is the first real `FaultInjector`:
  drop → return error; delay → sleep-then-proceed; corrupt → mutate request. This is
  a *grey-box* reach (server-side RPC seam).
- **Timing control is an action class of its own.** Remove client deadlines to hold a
  request open; trip an in-flight context deadline early; fire a timer task before it
  is due. These need paired client+server interceptors and are white/grey-box.
- **Separate run from eval.** A run records its actions + the Monitor's `FactLog`; the
  rulebook check can run inline *or* be replayed offline against the capture. This
  makes checks re-runnable and tweakable without re-driving the server (STAMP: *"run
  scenarios, capture data — then check; re-run checks much faster"*).
- **Known-bug dismissal.** A run can mark a specific expected violation as a known
  bug so an unrelated defect doesn't block a developer (STAMP note).
- **Prefer the SDK worker over a raw poller** where possible, hooking "before/after
  worker receives WFT" so one flow works with and without a real worker and stubs
  work that isn't built yet (STAMP note). Keeps black-box flows honest.

---

## Integration needs (the seams it plugs into)

These already exist or are half-built in the Umpire code; the Driver consumes them.

1. **`FaultInjector` (the active hook).** `interceptor.go` already threads an
   `inj FaultInjector` through `NewUnaryServerInterceptor`; `tests/umpire`'s
   `NewUnaryServerInterceptor(u, inj)` already accepts it and passes `nil` today.
   *Need:* a `Driver` type implementing `Inject`, and to pass it where `nil` is
   passed now. **No framework change to start** — this is the cleanest entry point.
2. **Read access to the model.** The Driver needs `Registry.QueryEntities` /
   `ChangedEntities[T]` to evaluate guards. `Umpire.Registry()` already exposes it.
   *Need:* a small read-only façade so flows express guards without importing the
   whole registry (e.g. `model.Update(id).Reached("admitted")`).
3. **A client handle.** Actions that are RPCs need a frontend client (and, for
   worker-based flows, an SDK worker hook). *Need:* the Driver owns/receives the
   same client the test would have used; per-namespace, matching the Monitor's
   namespace scoping so a run and its checks share a namespace.
4. **Coverage feedback (the reward signal).** The Scenario/Coverage subsystem in
   `UMPIRE_PLAN.md` (phases 0–3) must land for the Driver's generative modes to have
   targets. `Coverage.Unmet()` is explicitly called out there as the *"generation
   seam … expose as targets for the active side; don't build it (yet)."* The Driver
   is the thing that was being deferred to.
5. **Namespace lifecycle.** Reuse `CheckNamespace` / `PurgeNamespace`: a run executes in
   a namespace, the Monitor checks it, then it's purged — coverage survives the purge
   (already specced). The Driver must create/own that namespace per run.
6. **Timing interceptors (later).** Client+server interceptors for deadline/timer
   control don't exist yet — a genuine new seam, needed only for white-box timing
   flows.

---

## Modes & phases

Mapped onto STAMP's four phases and Umpire's current state (the Monitor is at STAMP
phase 1: passive models, enforced but not yet driving).

| Phase | Driver mode | What the user provides | Runs | Monitor counterpart |
|---|---|---|---|---|
| **P1 — Manual** | one concrete flow | exact inputs, one path | per-PR | passive model (done) |
| **P2 — Acceptance** | parameterised flow | inputs + a coverage goal (scenarios to cover) | per-PR / nightly | rules + scenarios |
| **P3 — Exploration** | sampled sweep | input *ranges* | nightly GHA | scenario coverage report |
| **P4 — Guided (strategist)** | fuzz | ranges + `sometimes`/coverage hints | nightly GHA | `Coverage.Unmet()` as reward |

**Build order:**

0. **Deterministic executor.** A `Driver` that runs a single hand-written flow (RPC
   actions only, black-box, guards over the shared model). Wire it as the driver for
   *one* existing functional test; let the Monitor judge. Proves drive+judge in one
   process with zero framework change beyond a read façade. *(This is the mirror of
   the Monitor's own "gate run": pick one test, make it green end-to-end.)*
1. **Fault actions.** Implement `FaultInjector`; add drop/delay/error actions; run a
   flow that provokes a rule the Monitor already has (e.g. task starvation via dropped
   WFT). First real chaos, grey-box, functional-test only.
2. **Parameterise & declare (P2).** Flows take inputs; a flow declares intended
   scenarios; cross-check against the Monitor's `Coverage` at run end. Prove terseness:
   delete one functional test's hand-written driver *and* assertions, keep the flow.
3. **Sampled sweep (P3).** Iterate a flow over an input range under a seed; nightly
   GHA; capture runs; report coverage. Separate run from eval here (offline
   re-check).
4. **strategist (P4).** Only once Coverage is trustworthy: a chooser that targets
   `Unmet()`. Out of scope to build now; the seam is reserved.

---

## Closing the loop with the Monitor

The two halves are designed to feed each other (*"passive observations fed
back into the active part to inform its effectiveness"*):

- **Monitor → Driver:** the model (for guards) and the coverage catalog (for targets
  and for the declared-vs-detected sanity check).
- **Driver → Monitor:** it manufactures the *preconditions* the Monitor's rules need.
  `UMPIRE_PLAN.md` warns that a rule whose precondition is never reached "passes
  vacuously and gives false confidence" (it names `ContinueAsNew` as almost certainly
  never firing today). The Driver is the mechanism that *reaches those
  preconditions on purpose* — turning "are our rules even exercised?" from hope into a
  driven, mechanical fact. That is the single highest-value thing the Driver does
  before any fuzzing: **make the existing rules non-vacuous.**

---

## Open questions / risks

1. **Guard polling vs. event-driven.** Do guards poll the `Registry`, or does the
   `Registry` notify on generation bumps? Polling is simplest and matches the existing
   generation watermark; a notify seam may be needed for tight timing flows.
2. **Where does the Driver live relative to the proxy?** In-process functional tests
   have no proxy. Decide the canary deployment shape (sidecar proxy driving black-box
   actions) separately from the in-process one — but keep flow definitions identical
   across both (tier is a filter, not a fork — same discipline as the Monitor).
3. **Determinism under real concurrency.** Seeded choice is easy; a *reproducible*
   interleaving of injected faults against a live multi-goroutine server is the hard
   part (the STAMP linearizability note). May need the fault seams to expose
   synchronization points ("hold request between points X and Y").
4. **Omes coupling.** Reusing kitchensink lowers maintenance but couples the Driver
   to Omes' model; confirm the dependency direction is acceptable.
5. **Overlap with existing pollers/testvars.** The Driver should *replace* the
   fragmented poller/testvars style, not become a fourth style beside them. Plan a
   migration, not an addition.
6. **Coverage must land first for P2+.** The generative modes are blocked on the
   Monitor's Scenario/Coverage phases (0–3). P0/P1 (manual + faults) are not — start
   there.
```
