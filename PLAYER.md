# Player — the active side (spec & plan)

The flip side of the Umpire. Where the Umpire **observes, models, and judges** a
running Temporal server, the Player **drives** it: it generates actions, injects
faults, and steers the system into the states the Umpire is there to rule on.

For the passive half read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md),
[`UMPIRE_ABOUT.md`](./UMPIRE_ABOUT.md), and [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).
This document is a plan, not built code — it names the pieces, the seams they plug
into, and the order to build them.

## The one sentence

> Tests drive behaviour and Umpire judges it — the Player **is** the test that
> drives, made reusable, generative, and fault-aware.

Today "the driver" is hand-written functional-test code plus SDK pollers. The
Player's job is to become that driver as a first-class, reusable subsystem so the
same *actions* run across functional tests, nightly exploration, and canary — the
same way the same *rules* already do.

---

## Where it fits in the vision (BATS)

BATS (*Bespoke Acceptance TeSting*) names five subsystems. The Umpire is one of
them; the Player is its active counterpart. Baseball naming from the BATS doc:

| Subsystem | BATS role | Half |
|---|---|---|
| **Omes / Workflows** | the workloads & inputs (kitchensink workflows) | shared |
| **Lineup** (Entity Model) | model of entities & relationships | passive — *Umpire's `Registry`* |
| **Catcher** (Middleman) | observes everything; hook point for chaos | the seam between halves |
| **Umpire** (Model Checker) | updates the model, makes rulings | **passive (built)** |
| **Player** (Scenario Runner; BATS: *Pitcher*) | throws pitches — creates plays / scenarios | **active (this doc)** |

And the Player's own vocabulary (baseball, from the BATS glossary):

| Term | Meaning |
|---|---|
| **Pitch** | the atomic action — one RPC, one injected latency, one dropped request, one fired timer. |
| **Play** | a composed scenario: an ordered/guarded sequence of Pitches ("Start → admit Update → drop WFT → poll"). |
| **Playbook** | the whole library of Plays the system can run. |
| **Game** | one full test run / session, evaluated against a Game Plan. |
| **Game Plan** | the set of Plays that *must* be exercised this Game — coverage goals. |
| **Skipper** | the strategist that *chooses* which Plays/Pitches to throw (fuzzing/coverage-guided). Deliberately **later**. |
| **Batter** | the SUT — the real Temporal server under injected conditions. |

> The Player is a **mechanical executor**; the Skipper is the brain. Build the arm
> first (deterministic, replayable Pitches), grow the brain (Skipper) once there is
> a reward signal to be smart about — which is exactly what the Umpire's coverage
> catalog provides (see *Closing the loop*).

## The loop the Player closes

The Umpire diagram runs left-to-right and stops at Violations. The Player wraps it
into a cycle: **drive → observe → model → judge → steer**.

```
        ┌──────────────────────── Skipper (later: coverage/fuzz-guided) ─────────────────────┐
        │                                                                                     │
        ▼                                                                                     │
   ┌─────────┐  Pitches (RPCs)   ┌──────────┐   gRPC+OTEL   ┌─────────┐  Facts  ┌──────────┐  │
   │ Player │ ────────────────▶ │  Server  │ ────────────▶ │ Umpire  │ ──────▶ │ Registry │  │
   │ (drive) │  faults (Inject)  │ (Batter) │               │(observe)│         │  (model) │  │
   └─────────┘ ◀──── predicates read the SAME model ──────────────────────────  └──────────┘  │
        │                                                              │                       │
        │                                                          Rulebook ─▶ Violations      │
        └───────── Coverage.Unmet() = "what states we still haven't reached" ──────────────────┘
```

Two properties fall out of this shape and drive the whole design:

1. **The Player reads the model the Umpire builds.** Action guards are *predicates
   over entity state* (`Update.Reached("admitted")`), not sleeps. This is the
   TigerBeetle/VOPR idea from the BATS notes: run a "perfect network" that handles
   the boring auxiliary traffic, then write the interesting scenario in a prolog-ish
   *predicate* style — "drop all WFTs, wait until the update is admitted, then …".
2. **The Umpire's coverage catalog is the Player's reward signal.** The Scenario /
   Coverage subsystem planned in `UMPIRE_PLAN.md` exists precisely so the active side
   has targets: `Coverage.Unmet()` is the list of interesting states nobody reached.
   That is the seam a Skipper optimises toward.

---

## Goals

- **Actions as a reusable subsystem.** Lift "the driver" out of per-test code into
  composable Pitches/Plays, so one scenario runs per-PR, nightly, and in canary —
  mirroring the reuse the rulebook already has.
- **Separate action from assertion (the other half).** The Umpire delivered the
  assertion half of this split; the Player delivers the action half. Together they
  make STAMP's "deconstructed test" real: Scenario (given) / Action (when) /
  Property (then) are three independent, recombinable things.
- **Predicate-guarded driving.** A Pitch fires when a *predicate over the model* is
  true, not after a timer. This is what makes concurrency scenarios expressible and
  deterministic instead of flaky.
- **Fault injection as a native action.** Latency, errors, drops, reordering,
  premature deadline/timer firing are Pitches like any RPC — not a separate bolt-on.
- **Terse, parameterised scenarios.** A Play is data. Parameterisation (the STAMP
  pain point) becomes "iterate the Play over an input range," not copy-paste tests.
- **Generative on ramp.** The same Play definitions feed: one concrete example
  (manual), a sampled sweep (nightly), and eventually fuzzing (Skipper). No rewrite
  between modes.
- **Reuse Omes kitchensink workflows** as the workload (BATS observation #2), rather
  than a bespoke DSL, to keep long-term maintenance cost down.

## Non-goals (for now)

- **The Skipper / smart exploration.** Coverage- and code-guided fuzzing is the
  end state (STAMP phase 4), not the start. Build the deterministic arm first.
- **Replacing the Umpire's rules.** The Player drives and may *declare* which
  scenarios it intended to hit; it does not judge. Judgement stays in the rulebook.
- **A new persistence/proxy stack.** Reuse the existing gRPC interceptor + OTEL
  processor seams. (BATS notes lean toward SQLite for durable coverage/almanac — a
  later concern, not v1.)
- **Rewriting every functional test at once.** Prove terseness on one, like the
  Umpire's plan does, before a sweep.

## Constraints

- **Deterministic & replayable.** Given the same seed and inputs, a Game must
  reproduce. This is what buys flaky-test *verification* and reproducible bug repro.
  Requires: seeded randomness, stable/derived identifiers (as the Umpire already
  has), and separating **run** from **eval** (capture the Game, re-check offline).
- **Tier-honest (same discipline as the Umpire).** Actions have a *reach* just as
  facts have a *provenance tier*:
  - **Black-box Pitches** — only the public frontend API (Start/Signal/Update/Poll/
    Describe/GetHistory). Runnable **anywhere**, including canary/Cloud.
  - **Grey/White-box Pitches** — fault injection at internal RPC / persistence /
    timing seams. Functional-test & test-cell only.
  A Play must declare its max reach so a canary Game literally cannot schedule a
  white-box Pitch — the exact mirror of the Umpire's "never silently skip" rule.
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
Pitch    = { Reach (black/grey/white), Guard predicate(model)->bool, Do(ctx, client, faults) }
Play     = ordered/guarded []Pitch + input parameters + declared Reach + intended Scenarios
Playbook = registry of Plays (name-validated, mirrors Rulebook/ScenarioBook)
Game     = one execution of a selected set of Plays against a live server, under a seed
GamePlan = the Scenarios a Game must cover (coverage goal); asserted against Umpire's Coverage
Skipper  = (later) chooses Plays/Pitches/params to maximise Coverage.Unmet() reduction
```

- **A Pitch is guarded by a predicate, not a clock.** `Guard` reads the Umpire's
  `Registry` (the shared model) and returns true when the system is in the state
  this Pitch wants to act on. The runner polls guards; eventual-consistency waits
  are structural, not `time.Sleep`. (Directly the STAMP note: *"actions are guarded
  by props; they can only fire when all props are true."*)
- **A Pitch declares its reach** so tier-gating is mechanical: a black-box Game
  filters the Playbook to black-box Plays.
- **Invalid variants for validation.** Each action trigger exposes an `Invalid…`
  form (STAMP note): where a Play would send a valid request, send the malformed one
  instead and assert the specific service error. Free negative-space coverage.
- **A Play declares the Scenarios it intends to reach.** At Game end this is
  cross-checked against the Umpire's `Coverage`: declared-but-never-detected ⇒ the
  Play's model of the world is wrong (same check the Umpire already specs for
  declared scenarios).

---

## Design decisions

- **Actions and facts are symmetric.** The Umpire has a `FactDecoder` that turns
  wire/spans **into** facts. The Player has the inverse: Pitches turn intent
  **into** wire calls. They meet at the same `EntityPath` addressing and the same
  deterministic identifiers, so a Pitch on `Workflow(id).Update(id)` and the fact it
  provokes name the same entity. Reuse `entity_key.go`.
- **Guards read the Umpire model directly — one model, two consumers.** No second
  state store. The Registry the Umpire fills is the Registry the Player queries.
  This is the whole reason the two halves live together.
- **Fault injection rides the dormant hook that already exists.** The framework's
  `FaultInjector.Inject(ctx, info, request) error` and the interceptor's
  `inj` slot (`common/testing/umpire/interceptor.go`, `NewUnaryServerInterceptor`)
  are built and wired but no-op. The Player is the first real `FaultInjector`:
  drop → return error; delay → sleep-then-proceed; corrupt → mutate request. This is
  a *grey-box* reach (server-side RPC seam).
- **Timing control is a Pitch class of its own.** From the BATS notes: remove client
  deadlines to hold a request open; trip an in-flight context deadline early; fire a
  timer task before it is due. These need paired client+server interceptors and are
  white/grey-box.
- **Separate run from eval.** A Game records its Pitches + the Umpire's FactLog; the
  rulebook check can run inline *or* be replayed offline against the capture. This
  makes checks re-runnable and tweakable without re-driving the server (STAMP: *"run
  scenarios, capture data — then check; re-run checks much faster"*).
- **Known-bug dismissal.** A Game can mark a specific expected violation as a known
  bug so an unrelated defect doesn't block a developer (STAMP note).
- **Prefer the SDK worker over a raw poller** where possible, hooking "before/after
  worker receives WFT" so one Play works with and without a real worker and stubs
  work that isn't built yet (STAMP note). Keeps black-box Plays honest.

---

## Integration needs (the seams it plugs into)

These already exist or are half-built in the Umpire code; the Player consumes them.

1. **`FaultInjector` (the active hook).** `interceptor.go` already threads an
   `inj FaultInjector` through `NewUnaryServerInterceptor`; `tests/umpire`'s
   `NewUnaryServerInterceptor(u, inj)` already accepts it and passes `nil` today.
   *Need:* a `Player` type implementing `Inject`, and to pass it where `nil` is
   passed now. **No framework change to start** — this is the cleanest entry point.
2. **Read access to the model.** The Player needs `Registry.QueryEntities` /
   `ChangedEntities[T]` to evaluate guards. `Umpire.Registry()` already exposes it.
   *Need:* a small read-only façade so Plays express guards without importing the
   whole registry (e.g. `model.Update(id).Reached("admitted")`).
3. **A client handle.** Pitches that are RPCs need a frontend client (and, for
   worker-based Plays, an SDK worker hook). *Need:* the Player owns/receives the
   same client the test would have used; per-namespace, matching the Umpire's
   namespace scoping so a Game and its checks share a namespace.
4. **Coverage feedback (the reward signal).** The Scenario/Coverage subsystem in
   `UMPIRE_PLAN.md` (phases 0–3) must land for the Player's generative modes to have
   targets. `Coverage.Unmet()` is explicitly called out there as the *"generation
   seam … expose as targets for the active side; don't build it (yet)."* The Player
   is the thing that was being deferred to.
5. **Namespace lifecycle.** Reuse `CheckNamespace` / `PurgeNamespace`: a Game runs in
   a namespace, the Umpire checks it, then it's purged — coverage survives the purge
   (already specced). The Player must create/own that namespace per Game.
6. **Timing interceptors (later).** Client+server interceptors for deadline/timer
   control don't exist yet — a genuine new seam, needed only for white-box timing
   Plays.

---

## Modes & phases

Mapped onto STAMP's four phases and the Umpire's current state (Umpire is at STAMP
phase 1: passive models, enforced but not yet driving).

| Phase | Player mode | What the user provides | Runs | Umpire counterpart |
|---|---|---|---|---|
| **P1 — Manual** | one concrete Play | exact inputs, one path | per-PR | passive model (done) |
| **P2 — Acceptance** | parameterised Play | inputs + a Game Plan (scenarios to cover) | per-PR / nightly | rules + scenarios |
| **P3 — Exploration** | sampled sweep | input *ranges* | nightly GHA | scenario coverage report |
| **P4 — Guided (Skipper)** | fuzz | ranges + `sometimes`/coverage hints | nightly GHA | `Coverage.Unmet()` as reward |

**Build order:**

0. **Deterministic arm.** A `Player` that runs a single hand-written Play (RPC
   Pitches only, black-box, guards over the Umpire model). Wire it as the driver for
   *one* existing functional test; let the Umpire judge. Proves drive+judge in one
   process with zero framework change beyond a read façade. *(This is the mirror of
   the Umpire's own "gate run": pick one test, make it green end-to-end.)*
1. **Fault Pitches.** Implement `FaultInjector`; add drop/delay/error Pitches; run a
   Play that provokes a rule the Umpire already has (e.g. task starvation via dropped
   WFT). First real chaos, grey-box, functional-test only.
2. **Parameterise & declare (P2).** Plays take inputs; a Play declares intended
   Scenarios; cross-check against Umpire `Coverage` at Game end. Prove terseness:
   delete one functional test's hand-written driver *and* assertions, keep the Play.
3. **Sampled sweep (P3).** Iterate a Play over an input range under a seed; nightly
   GHA; capture Games; report coverage. Separate run from eval here (offline
   re-check).
4. **Skipper (P4).** Only once Coverage is trustworthy: a chooser that targets
   `Unmet()`. Out of scope to build now; the seam is reserved.

---

## Closing the loop with the Umpire

The two halves are designed to feed each other (BATS: *"passive observations fed
back into the active part to inform its effectiveness"*):

- **Umpire → Player:** the model (for guards) and the coverage catalog (for targets
  and for the declared-vs-detected sanity check).
- **Player → Umpire:** it manufactures the *preconditions* the Umpire's rules need.
  `UMPIRE_PLAN.md` warns that a rule whose precondition is never reached "passes
  vacuously and gives false confidence" (it names `ContinueAsNew` as almost certainly
  never firing today). The Player is the mechanism that *reaches those
  preconditions on purpose* — turning "are our rules even exercised?" from hope into a
  driven, mechanical fact. That is the single highest-value thing the Player does
  before any fuzzing: **make the existing rules non-vacuous.**

---

## Open questions / risks

1. **Guard polling vs. event-driven.** Do guards poll the Registry, or does the
   Registry notify on generation bumps? Polling is simplest and matches the existing
   generation watermark; a notify seam may be needed for tight timing Plays.
2. **Where does the Player live relative to the proxy?** BATS puts Skipper+Umpire
   "inside the proxy." In-process functional tests have no proxy. Decide the canary
   deployment shape (sidecar proxy driving black-box Pitches) separately from the
   in-process one — but keep Play definitions identical across both (tier is a
   filter, not a fork — same discipline as the Umpire).
3. **Determinism under real concurrency.** Seeded choice is easy; a *reproducible*
   interleaving of injected faults against a live multi-goroutine server is the hard
   part (the STAMP linearizability note). May need the fault seams to expose
   synchronization points ("hold request between points X and Y").
4. **Omes coupling.** Reusing kitchensink lowers maintenance but couples the Player
   to Omes' model; confirm the dependency direction is acceptable.
5. **Overlap with existing pollers/testvars.** The Player should *replace* the
   fragmented poller/testvars style (a stated BATS goal), not become a fourth style
   beside them. Plan a migration, not an addition.
6. **Coverage must land first for P2+.** The generative modes are blocked on the
   Umpire's Scenario/Coverage phases (0–3). P0/P1 (manual + faults) are not — start
   there.
```
