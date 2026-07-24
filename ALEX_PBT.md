# Alex's schedule property-testing approach vs. Umpire

Notes from reading `main...sch-property` on `chaptersix/temporal`
([compare](https://github.com/chaptersix/temporal/compare/main...sch-property)).
The branch adds a **property-based analysis harness** for the Temporal *scheduler's*
matching-time computation. It overlaps Umpire's territory — both are model-first,
deterministic, evidence-producing test systems — but attacks it from the opposite end of
the scope/depth axis. This doc summarizes the branch, then compares it against Umpire
(`UMPIRE_SPEC.md` / `UMPIRE_MONITOR.md` / `UMPIRE_PLAN.md` / `UMPIRE_DRIVER.md`). For the
other design references see [`SAAMODEL.md`](./SAAMODEL.md) and
[`STAMP_IMPORT.md`](./STAMP_IMPORT.md).

## What the branch is

An **analysis-only, test-only** property campaign against one narrow, deep computation:
`ListScheduleMatchingTimes` — "given a `ScheduleSpec` and a query range, what times
match, and how much work does it take?" It is explicitly *not* a production change: nothing
is wired into the legacy or CHASM scheduler, no proto changes, no guard is selected. The
deliverable is **evidence** — findings, decision records, and a replayable corpus — for a
*later, separate* implementation project.

113 files, all under `service/worker/scheduler/propertytest/` (plus a top-level spec doc
and `go.mod`/`go.sum` for `pgregory.net/rapid`). The shape:

- **`schedule-matching-times-property-analysis-spec.md`** (903 lines) — the execution
  spec: scope, the 12 questions the analysis must answer, the calculator contract, the
  error taxonomy, the semantic model, generator rules, and library choice.
- **Copied computation** — the production matching computation is *copied* into
  `_test.go`-only files (an `analysis` prefix where names would clash) so it can be
  instrumented and its signatures changed without touching production and without being
  importable by production packages.
- **`calculator_*` / `spec_copy_test.go` / `calendar_copy_test.go`** — the copied
  calculator plus a `ComputeMatchingTimes(ctx, spec, start, end, jitterSeed, options)`
  contract returning `ComputeResult{Times, Work}` where `Work` is a `WorkBreakdown`
  (next-time calls, calendar search steps, interval/exclusion checks, excluded-candidate
  retries, result-loop steps).
- **`generators_test.go` / `model_test.go`** — a **semantic `ScheduleModel`** generated
  *first* and rendered into protobuf forms (structured calendar, calendar string, cron,
  mixed). The model exposes `MatchesNominal(time.Time)` as an **independent oracle** that
  never calls the copied calendar search.
- **`properties_test.go` / `validation_properties_test.go` / `calculator_contract_test.go`**
  — the properties, checked with `rapid`.
- **`redteam_*` / `operational_*` / `experiment_test.go` / `parity_test.go`** — the two
  red-team campaigns (computational + operational) and parity vs. the untouched production
  computation.
- **`plans/01..04.md`** — staged execution plans (validity hardening → computational red
  team → operational red team → results processing), gated so each produces inputs the
  next needs.
- **`FINDINGS.md` + `results/` + `testdata/`** — reviewed campaign evidence, JSON result
  bundles with schemas, and minimized `rapid` counterexample failfiles committed as
  regressions.

### The method, in five moves

1. **Copy, don't refactor.** Copy the minimum complete computation, preserve behavior,
   add parity tests against the original *before* changing anything. Copying buys the
   freedom to change signatures and collect diagnostics; it explicitly does **not** make
   the copy a correctness oracle — semantics are judged by a *separate* model.
2. **Generate valid-by-construction from a semantic model.** Build a `ScheduleModel`, then
   render it to protobuf. No "generate arbitrary protobuf and discard 99%." Invalid cases
   are a valid base plus **exactly one labeled mutation**, so the expected error is
   unambiguous and shrinking is clean.
3. **Three independent oracles.** (a) the semantic model's `MatchesNominal`; (b) a
   brute-force reference over reduced finite horizons; (c) metamorphic relations and
   parity against the production computation. The copy is never its own oracle.
4. **Deterministic work budget.** A versioned (`v1`) "iteration" is a budget tick per unit
   of repeated search work — never wall-clock, goroutine scheduling, or map order.
   `WorkBreakdown.Total` must equal the sum of budgeted categories. This makes cost itself
   a property (exact `N` succeeds, `N-1` fails with `ErrIterationLimit`).
5. **Red-team, then record.** Adversarial fixed-seed searches (Pareto-elite candidates
   over generations) drive cost to its worst case; operational campaigns add cancellation,
   deadline, concurrency, caching, memory, race, and fuzz dimensions. Everything lands in
   `FINDINGS.md` / `DECISIONS.md` with a mutation kill matrix.

### What it actually found

- A **normalized-midnight arithmetic bug** in timezone-calendar validation (`Pacific/Apia`
  boundary) and a **`Lord Howe` repeated-half-hour divergence** where the production
  calendar search omits the first occurrence of an ambiguous local time — preserved as a
  regression and a production follow-up, *not* fixed (out of scope).
- A **nil-calendar-element-before-protobuf-cloning** defect (cloning normalizes nil into a
  valid-default empty message).
- Hard **cost evidence**: the worst reviewed valid case needs ~9.5M work units; a 10,000
  cumulative-work cap rejects 4 of 6 reviewed *valid* cases — i.e. the "obvious" guard is
  wrong. No production limit was selected; guards were deferred or rejected with evidence.
- An **11-of-11 mutation kill matrix** — each planted defect is killed by a named property
  (`PROP-ORDERING-START-EXCLUSIVE`, `PROP-EXCLUSION-SUBTRACTION`, …), demonstrating
  sensitivity to the *planned* fault set (explicitly not completeness).

## The core contrast: deep-narrow property analysis vs. broad-live acceptance testing

This is the one framing to internalize. **Alex's harness is a microscope** — one
computation, lifted out of the server, hammered by generated inputs with independent
oracles and exact cost accounting, offline and hermetic. **Umpire is a wide-angle
observer** — the whole live server, watched passively across every functional test, judged
by invariants over interacting entities.

They are not competitors; they sit at opposite ends of a scope/fidelity trade:

| | Alex's PBT harness | Umpire |
|---|---|---|
| **Unit of test** | one pure-ish computation (`ListMatchingTimes`) | the whole running Temporal server |
| **SUT relationship** | *copied out* into a `_test.go` package, instrumented | *observed in place*, never modified |
| **Where it runs** | offline, in-process, hermetic; no cluster | ride-along on a live functional-test cluster |
| **Input source** | **generated** (`rapid`), valid-by-construction from a semantic model | **whatever the existing tests drive** (Monitor); a Driver that generates actions is specced-but-unbuilt |
| **Oracle** | independent semantic model + brute force + parity + metamorphic | per-entity executable FSMs (`Classify`) + cross-entity relational rules |
| **Judgement style** | properties (universally-quantified predicates over inputs) | safety rules (every observation) + liveness rules (eventually) |
| **Shrinking / minimization** | first-class via `rapid`; minimized failfiles committed | none — a violation is whatever real traffic produced |
| **Cost as a target** | central: deterministic work budget, exact-boundary properties | not modeled |
| **Fault injection** | operational red team (cancellation/deadline/cache/race) around the copied fn | `FaultInjector` hook exists, dormant; is the Driver's job (unbuilt) |
| **Cross-entity reach** | none — single computation, no entities | the differentiator: speculative-task ↔ update ↔ workflow-close |
| **Portability tiers** | n/a (hermetic) | black/grey/white-box, canary-portable subset |
| **Primary output** | evidence: `FINDINGS`/`DECISIONS`/result bundles/corpus | violations that fail real tests, per-PR |
| **Maturity** | 3 of 4 plans complete, reviewed, evidence banked | Monitor built + enforced; Driver + coverage specced |
| **Production intent** | explicitly deferred (analysis-only) | enforced today across the suite |

### Where they genuinely agree

Both were designed by someone who internalized the same principles — several of them are
almost verbatim across the two:

- **Model-first, and the copy/observation is not the oracle.** Alex: "copying the
  computation does not make the copy an independent correctness oracle … semantic
  properties must use a separate model, brute-force oracle, or metamorphic relationship."
  Umpire: entities are executable models, and a total `Classify` means no vacuous pass. Both
  refuse to let the thing-under-test grade its own homework.
- **Determinism & replay, run separated from eval.** Alex commits minimized `rapid`
  failfiles and versioned result bundles; the budget is deterministic by construction.
  Umpire's SPEC lists "deterministic & replayable" and "separate run from eval" as
  constraints, and the Driver doc repeats it.
- **Valid-by-construction generation over filter-and-discard.** Alex's generator rules ban
  `Filter`/`SkipNow` for validity and build values directly; the "one labeled mutation"
  rule for invalid cases is exactly Umpire's Driver "Invalid… variant" idea for
  negative-space coverage.
- **Versioned definitions so results aren't silently compared across changes.** Alex
  versions the iteration definition (`v1`), validator (`v1`), and context-check
  (`context-check-v1`) and refuses to aggregate across them. Umpire tiers facts by
  provenance and refuses to let a grey-box rule "pass" where its facts can't arrive.
- **A structured result/error vocabulary instead of pass/fail.** Alex's `valid-success` /
  `valid-empty-query-result` / `invalid-*` / `indeterminate-validation-budget` /
  `*-budget-exhausted` vocabulary mirrors Umpire's insistence that "observed nothing" must
  be an explicit skip, never a silent pass.

### What Umpire can take from it

Ranked by how directly it hits an open Umpire gap (`UMPIRE_PLAN.md`).

1. **A mutation kill matrix answers "are our rules even exercised?" — mechanically.**
   Umpire's *whole-game* worry is dead/vacuous rules (`ContinueAsNew` almost certainly
   never fires; the scenario-coverage subsystem is the planned fix). Alex's kill matrix is
   the same idea from the other side: plant a defect switch per rule, prove the rule's
   outcome changes. Umpire could add a **mutation-test harness** — test-only mutation
   switches on the model, each asserted killed by a named rule — as a *cheaper, immediate*
   down payment on the dead-rule report, before the full generative Driver+Coverage lands.
   It directly answers the "unvalidated enforcement" acute risk.
2. **Cost/work as a first-class modeled property.** Umpire models lifecycle correctness but
   says nothing about *how much work* an operation took. Alex's deterministic work budget
   (a tick per unit of search, `Total == sum of categories`, exact-boundary properties) is
   a clean pattern for turning performance envelopes into invariants — a natural future
   Umpire rule class ("this update settled within N observed steps"), and it dovetails with
   the event-time fidelity gap.
3. **`rapid` as the generation engine when the Driver arrives.** The Driver doc reaches for
   coverage-guided fuzzing but defers the strategist. Alex's harness shows `rapid` doing
   exactly the deterministic, shrinking, replayable generation the Driver wants — including
   `rapid.MakeFuzz` to reuse a property under Go's coverage-guided fuzzer, and state-machine
   support for metamorphic *sequences*. That state-machine mode is a candidate substrate for
   Driver **flows** (guarded action sequences), and it's already a vetted dependency.
4. **Evidence discipline as a deliverable.** Alex banks reviewed campaigns, decision
   records with false-positive tables, and a schema-validated corpus. Umpire's enforcement
   currently produces violations but no comparable durable "here's what we checked, here's
   what we deferred and why" artifact. Adopting the `FINDINGS`/`DECISIONS` habit would make
   Umpire's tier-coverage reports and triage decisions auditable.

### Where Umpire is ahead (and why the approaches are complementary, not redundant)

- **Cross-entity, whole-system reach.** Alex's harness has no notion of interacting
  entities — it is one computation. Umpire's differentiator is precisely the invariants a
  single-computation (or single-archetype, cf. SAA) model *cannot state*: speculative task
  ↔ update, update ↔ its workflow's close. A property test of `ListMatchingTimes` will never
  catch a scheduler that computes the right times but then *acts* on them wrongly against
  the rest of the system.
- **Portability across deployments.** Alex's harness is hermetic by design; Umpire's tier
  model aims the same rules at canary/Cloud where you have no internal insight. Different
  problem, and Umpire owns it.
- **Zero-authoring, ride-along enforcement.** Alex's campaigns are hand-designed analyses;
  Umpire judges *every* functional test with no per-test assertion writing. That breadth is
  the thing Alex's depth can't provide.

### The synthesis

The two are the two axes of the same discipline. Alex proves you can take **one deep
computation** and, with generated inputs + independent oracles + exact cost accounting,
extract genuine bugs and hard evidence hermetically. Umpire proves you can take the **whole
live system** and judge cross-entity behavior for free on every test. The gap each leaves
is the other's strength:

- Point Alex's method at Umpire's model → a **mutation kill matrix** that kills the
  dead-rule risk now, and **cost properties** as a new rule class.
- Point Umpire's method at Alex's findings → the `Lord Howe` / `Apia` divergences are exactly
  the kind of thing a **black-box scheduler rule** (observe scheduled vs. actual fire times
  over a running schedule) would catch *in situ*, closing the loop from "analysis found it"
  to "enforcement guards against regression."

The ideal end state is one shared model with two consumers on *two* axes: Umpire's
Driver/Monitor across the live system, and a `rapid`-driven property layer drilling into the
deep computations (scheduler matching, calendar search, and their kin) that the wide-angle
observer can only see the surface of.
