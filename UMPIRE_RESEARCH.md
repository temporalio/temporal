# Umpire — Research (external prior art)

> **Status: historical/research.** This is strategy input, not a current implementation inventory;
> see `UMPIRE_PLAN.md` for status and ordering.

External projects, papers, tools, and ideas that pursue Umpire's goals, and what to borrow from
each. This is the **outward-facing** companion to [`UMPIRE_PRIOR_ART.md`](./UMPIRE_PRIOR_ART.md),
which covers **internal** Temporal prototypes (SAA, STAMP, Omes, the schedule PBT). For the system
those goals belong to read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for the trace-derived-fault design
this most directly informs read [`UMPIRE_TRACING.md`](./UMPIRE_TRACING.md).

Umpire, restated so the mapping is precise: a **passive runtime monitor** that observes a live
Temporal server (gRPC + OTEL), reconstructs **per-entity state machines** whose transition function
is a total `Classify → Advance/NoOp/Illegal` oracle, and **judges** them with **safety** rules
(hold at every observation) and **liveness** rules (hold eventually; unresolved-at-teardown =
violation) — the highest-value ones **cross-entity relational** — run **ride-along** over the
existing functional suite; plus an active **Planner** (routes over the model graph:
shortest/all/random+seed/constrained) and **Driver** (multiple realizers + fault injection +
input mutation), aiming to be **deterministic & replayable** with **coverage as the reward**.

Umpire is, almost exactly, **MOP-style parametric runtime verification over OTEL/gRPC
observations, targeting cross-entity invariants, fed by a GraphWalker-style route planner and a
Filibuster-style trace-derived fault plan, aspiring to DST seed/replay discipline.** Each of those
four clauses is a mature external field; this doc maps them.

## Synthesis

| Family | Exemplars | Umpire adopts | Umpire leaves |
|---|---|---|---|
| **Runtime verification / trace monitoring** | MOP / JavaMOP, LTL₃ & end-of-trace semantics, QRE | monitor-from-spec, **parametric trace slicing** (= per-entity FSMs), 3-valued verdicts, teardown-liveness as *truncation semantics*, past-time LTL for safety | AOP/bytecode weaving; giving a definitive liveness verdict before teardown |
| **Observed-history checkers** | Jepsen: **Elle**, Knossos, **Porcupine** (Go), Maelstrom | observe-then-judge stance; **dependency-graph / cross-entity** reasoning; soundness-not-completeness; counterexample-as-witness; Porcupine's Go engine + history viz | DB-isolation ontology; linearizability as the *only* property |
| **Trace-derived fault injection** | **Filibuster** (SFIT), **LDFI/molly**, **3MileBeach**, Krasnovsky trace-discovered models | observe call-footprint → fail each op; dynamic reduction; **tracer-is-injector**; **context-conditioned** injection; **backward-from-provenance** minimal fault sets | per-test hand-written assertions; source static-analysis for the response catalog; SAT/Dedalus machinery |
| **Model-based testing (the Planner)** | **GraphWalker**/AltWalker, Spec Explorer, ModelJUnit, Modbat | generator/stop-condition taxonomy; **coverage-as-stop-condition**; **online/adaptive** planning under nondeterminism; **scenario slicing**; nondeterministic/exception transitions | shallow per-vertex assertions (Umpire's `Classify` is stronger); dead/.NET tooling |
| **Stateful PBT (Go-native reuse)** | **rapid**, gopter, `go test -fuzz`; Quviq QC, Hedgehog, Hypothesis, fast-check | **automatic shrinking** of failing plans; **`MakeFuzz`→coverage-guided** fuzzing; **symbolic↔concrete** value threading (real Temporal handles); Bundle/`@invariant` patterns | random-walk generation (Umpire's Planner supersedes); non-Go tools |
| **Deterministic simulation testing** | **Antithesis**, **TigerBeetle VOPR**, FoundationDB/**BUGGIFY**, **Dropbox Nucleus**, madsim/Hermit/rr | seed→run→eval split; **seed+server-version** repro tuple; dense always-on assertions; **coverage-guided** search; "sometimes/reachability" assertions; BUGGIFY seams; several checkers not one | pure-sim rewrite; **bit-perfect internal replay** (Umpire gets *boundary* determinism) |
| **Model checking / formal methods** | **P** (spec monitors), TLA+/TLC/Apalache, Alloy, Stateright, SAMC, **MaceMC** | property **vocabulary** (□/◇/leads-to); **spec-monitor** design; Alloy relational logic for cross-entity rules; MaceMC liveness def + **critical-transition** diagnostic | closed-world state-space *generation* (Umpire observes, doesn't explore) |
| **OTEL/trace assertion tools** | **Tracetest**, Malabi | span-selector ergonomics; **ride-along** trigger-then-assert pattern; CI/CLI shape | stateless per-trace assertions (no cross-entity, no liveness — Umpire is a strict superset) |
| **Oracle strategies** | Metamorphic testing, differential testing | **input-mutation-as-metamorphic-relation** (mutation + predicted `Classify` delta = an oracle); history-level linearizability oracle as a complement | differential (needs a second impl); MT as a *primary* oracle |
| **FSM conformance theory** | (rural) Chinese-postman, W/HSI/UIO | **minimum-cost transition tour** (a better `AllRoutes`); **state-identifying sequences** to confirm the real system's state; **fault-domain** framing for defensible coverage | full W-suites (huge) — use HSI/UIO |

## 1. Runtime verification — Umpire's exact paradigm

**Monitoring-Oriented Programming (MOP / JavaMOP).** Synthesize a monitor from a declarative
temporal property, run it alongside the execution, fire on validation/violation. Its
**parametric trace slicing** — one monitor instance per parameter binding `(workflowID,
updateID, …)` — *is* Umpire's "one state machine per entity," and is the battle-tested name for
that mechanism. Umpire = MOP where events arrive from OTEL spans/gRPC instead of AOP pointcuts
and the "action" is emitting a `Violation`. **Adopt:** monitor-from-spec (so rules aren't
hand-coded FSMs), trace slicing, validated/violated hooks. **Leave:** bytecode weaving.
Roșu/Chen et al. — https://github.com/runtimeverification/javamop ·
overview https://link.springer.com/article/10.1007/s10009-011-0198-6

**The safety/liveness monitoring theory (LTL₃, past-time LTL, end-of-trace).** The RV result set
that grounds Umpire's design: **safety has a finite bad prefix** (monitor online, cheap with
past-time LTL and bounded state); **liveness cannot be definitively refuted on a finite trace**,
so monitors use 3-valued verdicts (true/false/**inconclusive**) and resolve pending "eventually"
obligations at **end-of-trace / truncation**. Umpire's **"unresolved-at-teardown = liveness
violation" is exactly truncation semantics** — a standard, named technique, not an ad-hoc choice.
**Adopt:** 3-valued verdicts during a run collapsing pending→violated at teardown; past-time LTL
for safety; MTL/bounded-liveness (`◇≤k`) if "eventually within N" is ever wanted. **Leave:**
attempting a definitive liveness verdict before teardown (impossible without bounds).
https://link.springer.com/article/10.1007/s10703-023-00429-8 · RV-Monitor (productized generator)
https://github.com/runtimeverification · **Quantitative Regular Expressions** (Alur/Mamouras) are
the principled path if rules ever compute *quantities* over the stream (counts, latencies, rates)
in bounded single-pass memory — https://www.cis.upenn.edu/~alur/Popl19.pdf

## 2. Observed-history checkers — the observe-and-judge stance

**Jepsen · Elle · Porcupine.** The purest embodiment of Umpire's passive stance: never control the
system, record what happened, reason backward. **Elle** (Kingsbury & Alvaro, VLDB'20) infers a
**dependency graph** from an observed history and reports anomalies as **cycles** — correctness as
a property of the *graph of relations between operations*, which directly validates Umpire's thesis
that the valuable invariants are **cross-entity relational**, not per-op predicates. **Porcupine**
(Athalye, MIT 6.824) is the fast, embeddable **Go** linearizability checker (executable model +
history → verdict, with P-compositionality and history **visualization**) — the closest build
reference in Umpire's own language. **Adopt:** dependency-graph-over-observations; soundness-not-
completeness (report only what the trace proves); counterexample-as-witness; Porcupine's Go engine,
model interface, per-entity partitioning, and violation timeline UX. **Leave:** the DB-isolation
ontology and linearizability-as-the-only-property — Umpire's domain is entity lifecycles and richer
temporal rules. Elle https://github.com/jepsen-io/elle · Porcupine
https://github.com/anishathalye/porcupine · Maelstrom (generic observe-only harness)
https://github.com/jepsen-io/maelstrom

## 3. Trace-derived fault injection — direct prior art for `UMPIRE_TRACING.md`

**Filibuster — Service-Level Fault Injection Testing (SFIT), the closest prior art overall.**
Meiklejohn et al., ACM SoCC'21. Instruments *client and server* comms libraries ("mirroring
OpenTelemetry"), observes the remote-call footprint of a **passing functional test**, then does a
concolic/DFS exploration **failing each observed call site** (call-site exceptions + service error
responses). **Dynamic reduction by service encapsulation** prunes combinations (Audible 69→31), but
the benefit is graph-shape-dependent (Netflix homepage: 3/1606). This is Umpire's
capture-footprint-then-fault-each loop, almost verbatim. **Adopt:** the observe-then-fail-each-op
loop; dual client+server interceptors to resolve the true callee; dynamic-reduction as a test-count
control; the "extend a happy path" framing; the reproduced-bug corpus as an eval model. **Leave:**
its **oracle is per-test hand-written conditional assertions** — Umpire replaces this with generic
safety/liveness rules; and its response catalog via **source static-analysis** — Umpire derives
observed response/error shapes from traces + the persistence footprint instead (language-agnostic,
fits one server). https://christophermeiklejohn.com/publications/filibuster-socc-2021.pdf ·
dynamic reduction https://christophermeiklejohn.com/filibuster/2021/10/14/filibuster-4.html

**Lineage-Driven Fault Injection (LDFI) / molly — the smart-search answer to combinatorial blowup.**
Alvaro et al., SoCC'15; productized at **Netflix** (2016). Reason **backward from the provenance of
a correct outcome**: build its lineage, encode as CNF where each disjunct is a fault that would
invalidate a derivation, SAT-solve for a **minimal fault set** that breaks *all* known derivations,
inject exactly that; if the outcome survives, feed the new lineage back and re-solve. Finds bugs in
~order-of-magnitude fewer runs than random FI; Netflix ran it on real request-trace lineage
(~200 experiments vs 2¹⁰⁰). A transition's gRPC+persistence footprint **is** a lineage of that
transition's success, so this is the principled way to shrink Umpire's `edge × op × fault-kind`
budget. **Adopt:** reasoning-backward-from-observed-success to inject only faults that could matter.
**Leave:** the Dedalus modeling requirement and full SAT machinery — take the principle, not the
formalism. https://people.ucsc.edu/~palvaro/molly.pdf ·
Netflix https://databeta.wordpress.com/2016/02/04/lineage-directed-fault-injection-at-netflix/

**3MileBeach — "the tracer is the injector," with conditional injection.** Zhang/Ferydouni/Alvaro,
SoCC'21. Interposes on message serialization to give tracing **and** fault injection in one layer,
and its **Temporal Fault Injection** faults a message *only when temporal prerequisites hold* —
i.e., inject at a specific *observed context*, not globally. That is exactly Umpire injecting at a
specific observed transition rather than every call to a method. **Adopt:** unify observe+inject in
one interception layer (Umpire already plans this with the gRPC `RPCFaultGenerator` + persistence
interceptor); TFI-style context-conditioned injection keyed to a transition's footprint. **Leave:**
serialization-layer interposition (their polyglot trick; Umpire targets one server).
https://people.ucsc.edu/~palvaro/3milebeach.pdf

**Trace-discovered resilience models (Krasnovsky, 2025–26).** The most on-the-nose recent statement
of "synthesize the model + fault scenarios from OTEL traces, in CI." Extracts a live
dependency/replica graph from traces and simulates faults; a companion paper tackles **async /
fire-and-forget** semantics by using **span causality (not wall-clock) to recover true order** —
critical for a heavily-async system like Temporal, and the same event-time concern
`UMPIRE_PLAN.md`/`UMPIRE_TRACING.md` already flag. **Adopt:** model-discovery-in-CI framing (matches
checking footprints into source control); span-causality to define where a fault is meaningful.
**Leave:** its coarse Monte-Carlo *availability estimation* — Umpire does per-op injection + rule
judging. https://arxiv.org/abs/2506.11176 · async: https://arxiv.org/pdf/2512.12314

**Production chaos tooling (context — none derive faults from traces).** Istio fault injection,
**toxiproxy**, Chaos Mesh, LitmusChaos, Gremlin, AWS FIS are all **hand-configured injection
mechanisms**, not fault-discovery systems (a GitHub survey confirms the field's manual, network/
instance bias — app-level faults ~3%). Relevant only as possible *actuators* for infra-level faults
(partition/latency) beyond gRPC/persistence; the *intelligence* Umpire wants is exactly what they
lack. https://istio.io/latest/docs/tasks/traffic-management/fault-injection/ ·
https://github.com/Shopify/toxiproxy · survey https://arxiv.org/html/2505.13654v1 ·
adjacent: idempotency-under-retry verification (Flux, OSDI'23) — a useful oracle when judging
retried operations — https://www.usenix.org/system/files/osdi23-ding.pdf

## 4. Model-based testing — direct prior art for the Planner

**GraphWalker (+ AltWalker) — essentially Umpire's Planner, already generalized.** Walks a directed
model graph with pluggable **generators** (`random`, `weighted_random`, `quick_random` =
pick-unvisited-edge-then-Dijkstra, `a_star` = shortest-path-to-target) and **stop conditions**
(`edge_coverage%`, `vertex_coverage%`, `reached_vertex`, `requirement_coverage`, `time`, `length`),
in **online** (plan-step-execute, adaptive) or **offline** (emit a path) mode, with `SHARED` states
stitching sub-models. `a_star`/`quick_random` = Umpire's shortest mode; `random`+seed = its random
mode; `reached_vertex` = "route to target, fail if unreachable." **Adopt (genuinely additive):**
(a) **coverage as a first-class stop condition** — formalizes "coverage as reward" into a
termination criterion; (b) **`weighted_random`** to bias toward risky transitions; (c) the
**online/adaptive planner** — vital because Temporal responses are nondeterministic and may force
mid-plan replanning; (d) `SHARED`/sub-model composition to scale many entity FSMs. **Leave:** its
shallow per-vertex assertions — Umpire's total `Classify` is a stronger oracle.
https://graphwalker.github.io/ · AltWalker https://github.com/altwalker/altwalker

**Microsoft Spec Explorer** — "guarded model program → explore into an FSM → traverse for transition
coverage," validated at Windows-protocol scale; **scenario slicing (Cord)** = Umpire's *constrained
exploration*; **on-the-fly mode** = adaptive planning under nondeterminism. **Adopt:** scenario
slicing as a first-class constraint language applied *before* route computation; explicit
explore-implicit-model-into-explicit-FSM if Umpire ever computes states lazily. **Leave:** the dead
.NET tool. https://www.microsoft.com/en-us/research/project/model-based-testing-with-specexplorer/

**Modbat** — first-class **nondeterministic / exception transitions**: an action may legally land in
one of several next states (a throw routes elsewhere), and the oracle accepts any legal landing —
richer than a single-valued `Advance` and a direct match for Temporal's timeouts/retries/races;
plus **component-model inheritance** for entity families. **Adopt** both.
https://github.com/cyrille-artho/modbat · **ModelJUnit** contributes the standard **coverage
taxonomy** Umpire should name its reward tiers after: **state / transition / transition-pair
(2-switch)** coverage. https://sourceforge.net/projects/modeljunit/

## 5. Stateful property-based testing — Go-native reuse

**`pgregory.net/rapid` — the highest-reuse engine (same language).** First-class state-machine
testing (`t.Repeat`, `rapid.StateMachineActions`), **fully automatic shrinking**, and
**`rapid.MakeFuzz`** turning any rapid test into a `go test -fuzz` target with a coverage-guided
corpus. `Skip()`-on-precondition ≈ `Illegal`/guard; the `""`/`Check` invariant ≈ `Classify`.
**Adopt (strongly additive):** automatic **shrinking of a failing plan** to a minimal reproducer
(Umpire lacks this), and `MakeFuzz` as a ready implementation of **coverage-as-reward** and of
**input-mutation** validation. **Target architecture:** Umpire's Planner generates the sequence; a
rapid-style shrinker minimizes counterexamples; `-fuzz` feeds seeds. **Keep-Umpire:** rapid has *no
route planner* — it random-walks; the Planner is the complementary piece.
https://github.com/flyingmutant/rapid · native fuzzing https://go.dev/doc/security/fuzz/ ·
`gopter` is a second Go reference but rapid supersedes it https://github.com/leanovate/gopter

**The broader stateful-PBT lineage (ideas, not the tools).** Quviq Erlang **QuickCheck**
(`eqc_statem`/`eqc_fsm`) is the battle-tested origin and the citable evidence base (LevelDB
data-loss, Riak/CRDTs, AUTOSAR 200+ issues, the Dropbox sync study); **Hedgehog** and Haskell
**quickcheck-state-machine** give the cleanest **symbolic↔concrete phase split** — plan abstractly,
then bind to real handles at execution — which is exactly Umpire's abstract-event→real-traffic seam
and what it needs to thread real Temporal IDs (run IDs, task tokens) through a plan; Python
**Hypothesis** (`Bundle`, `@invariant`, `target()`) and JS **fast-check** (`scheduledModelRun` for
race oracles) round it out. **Adopt:** symbolic/concrete value threading; the `precondition /
next_state / postcondition` triple (which Umpire already mirrors); parallel/scheduled runners as a
concurrency-oracle pattern. **Leave:** their pure random+shrink generation — the Planner is ahead.
https://www.quviq.com/documentation/eqc/overview-summary.html ·
https://hackage.haskell.org/package/quickcheck-state-machine ·
https://hypothesis.readthedocs.io/en/latest/stateful.html · https://fast-check.dev/docs/advanced/model-based-testing/

## 6. Deterministic simulation testing — determinism, replay, cadence

**The governing caveat.** Classic DST buys bit-identical replay by *owning the runtime* (single-
threaded pseudo-concurrency; mocked clock/network/disk/RNG/scheduler; seeded PRNG). **Umpire drives
a real, multi-threaded Go server it does not own**, so it sits at the Antithesis/Jepsen/black-box
end, not the FoundationDB/TigerBeetle end. **Consequence to decide explicitly:** Umpire's
reproducibility is **generative boundary-determinism** (seed + inputs + fault schedule + **server
version** reproduce *observable* behavior) — *not* full internal replay — unless it also
determinizes the server's environment (à la Hermit). This one decision drives the rest of the
design.

**Antithesis — closest philosophical match.** Runs *unmodified* software in a proprietary
deterministic **hypervisor**, then explores autonomously (coverage-guided) for property violations;
founded by ex-FoundationDB people. It exists precisely to get DST value **without rewriting the
system** — Umpire's exact situation. **Adopt:** external-determinism (around, not inside, the
server); **coverage-guided exploration** (Umpire's stated fuzzing goal — this is the high-value
direction over random seed-bashing); **"sometimes"/reachability assertions** that reward *ever*
reaching a rare state (a great primitive for "drive a real server into rare states"). **Consider as
buy-not-build:** running the Temporal server *under* Antithesis/Hermit rather than reimplementing a
determinism substrate. **Leave:** building a hypervisor. https://antithesis.com/docs/resources/deterministic_simulation_testing/

**TigerBeetle VOPR — the best open, studyable implementation.** Deterministic cluster sim + fuzzer;
two ideas survive the real-server gap: **`seed + git-commit` as the reproducibility tuple** (treat
the exact server build as part of the repro key) and **dense always-on invariant assertions** baked
into the running system (the payoff of making Umpire's rules dense and continuous, not test-only).
Its **"hub" farming seeds continuously and surfacing failing ones** is a ready blueprint for
Umpire's fuzzing infra. **Leave:** single-process whole-cluster sim; byte-for-byte cross-node
equality as the oracle. https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md

**FoundationDB / BUGGIFY** — the seminal DST reference; its most portable primitive is **BUGGIFY**:
developer-annotated fault-injection points inside/around the server, toggled by a seeded RNG — "
first-class fault injection at known seams," realizable *without* full determinism (and close in
spirit to the dormant `FaultInjector`/persistence seams Umpire already has). Time compression via a
controllable clock maps to Temporal's time-skipping test server. https://apple.github.io/foundationdb/testing.html ·
**Dropbox Nucleus** is the best *retrofit-scale industrial* case study: mocked environment + seeded
repro + **tens of millions of randomized runs nightly** + **multiple purpose-built checkers**
(CanopyCheck/Trinity) rather than one oracle — directly informing Umpire's cadence and its
"prefer several focused invariant-checkers" instinct. https://dropbox.tech/infrastructure/-testing-our-new-sync-engine

**Determinism substrates worth knowing** (in-process/foreign-binary techniques): **madsim** (Rust
runtime + libc interception; RisingWave made a real DB sim-testable — but they own the code),
**turmoil** (clean network-fault API), **shuttle** (randomized, *unsound-but-scalable* concurrency
search — validates Umpire's "randomized + seeded, accept unsoundness for scale" stance vs exhaustive
**Loom**), **Hermit** (deterministic Linux sandbox via syscall interception — the Antithesis-lite
path to determinize an *unmodified* server; caveat: maintenance-mode), **rr** (record/replay — a
*complementary* debugging primitive: capture a failing seed's actual server execution for
time-travel debugging, since Umpire's generation is only boundary-deterministic). **Resonate** (a
durable-execution engine — Temporal-adjacent — built single-goroutine-deterministic *by design*: the
contrast that frames Umpire as the **retrofit** answer for a server that wasn't). Primer:
https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html ·
index https://github.com/ivanyu/awesome-deterministic-simulation-testing

## 7. Model checking & formal methods — vocabulary and the entity-FSM model (contrast)

These **generate** executions rather than **observe** them, so they contribute *property vocabulary*
and the *entity-state-machine mental model*, not a checking method — don't turn Umpire into a model
checker.

- **P language** (MSR→AWS; used on S3/EBS/DynamoDB) models systems as **communicating state
  machines** and supports **spec monitors** — a passive observer machine fed an event stream that
  maintains its own state and flags safety/liveness. That is the closest existing design to Umpire's
  judge; the difference is only the event source (generated vs real). **Adopt:** the spec-monitor
  shape; invariants as separate artifacts. https://p-org.github.io/P/
- **TLA+ / TLC / Apalache** are where the **safety/liveness vocabulary** is most rigorous (□ always
  = safety, ◇ eventually / `↝` leads-to = liveness, fairness). AWS's decade of TLA+ on DynamoDB/S3
  is the strongest industrial evidence that cross-component temporal invariants catch real bugs.
  **Adopt:** the operator vocabulary for expressing rules; `P ↝ Q` (leads-to) as the canonical
  liveness shape (≈ "task scheduled ↝ task started"). https://apalache-mc.org/ ·
  https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/
- **Alloy** is relational logic over sets and relations — its entire worldview is *constraints over
  relationships between entities*, i.e. Umpire's cross-entity invariants; a mature ergonomic
  reference for a cross-entity rule DSL. https://alloytools.org/
- **MaceMC** (Mace toolkit) operationalizes Umpire's **liveness definition verbatim** ("need not
  always hold, must eventually") and contributes the **critical-transition diagnostic** — the last
  observation after which the pending obligation could no longer be met; pointing at it would sharply
  improve the debuggability of an unresolved-at-teardown violation. **Adopt** that diagnostic.
  https://www.usenix.org/conference/nsdi-07/life-death-and-critical-transition-finding-liveness-bugs-systems-code
- **SAMC/FlyMC** (semantic-aware DMCK) and **Concuerror** (DPOR) are active explorers — contrast —
  but their **independence/commutativity** reasoning is worth borrowing to scope cross-entity checks
  to causally-connected entities; **Stateright** (Rust) blurs model and implementation like Umpire's
  "judge the real system" pitch. https://www.usenix.org/conference/osdi14/technical-sessions/presentation/leesatapornwongsa

## 8. Observability / trace-assertion tools — the mechanism precedent

**Tracetest** (Kubeshop) is the closest existing *mechanism*: trigger an operation, collect its
OTEL trace, assert on any span — and its **trigger-via-existing-runner-then-assert** mode is proof
that Umpire's **ride-along** design is a validated pattern. But its judging model is **stateless,
per-trace, imperative assertions** with no cross-entity state and no liveness — Umpire is a **strict
superset**. **Adopt:** span-selector ergonomics; the CI/CLI ride-along shape. **Leave:** the
stateless per-trace model. https://github.com/kubeshop/tracetest · Malabi is the in-process,
code-first variant (same limits). The ecosystem convergence on "assert on OTEL spans" — none doing
temporal safety/liveness or cross-entity FSMs — confirms Umpire's differentiation is real.

## 9. Oracle strategies & FSM conformance theory

**Metamorphic & differential testing** address the oracle problem without expected values. Umpire
*has* an oracle (`Classify`), so these are **complements**: formalize **input mutation as a
metamorphic relation** (a mutation + the predicted change in the model's classification = an oracle,
turning mutation from a crash-finder into a judged test — the missing oracle for the SPEC's
input-mutation goal); and layer a **Jepsen/Elle-style history/linearizability oracle** on the event
stream *above* per-transition `Classify` for whole-run relational properties. **Leave** differential
testing as a *primary* oracle (needs a second implementation). https://dl.acm.org/doi/10.1145/3143561

**FSM conformance theory — the formal backbone for the Planner and coverage.** Classic methods with
*guaranteed fault coverage*: a **transition tour** covering every edge, minimized as the **(rural)
Chinese-Postman Problem** — a concrete algorithm for a **minimum-cost full-edge-coverage plan**,
strictly better than an exponential `AllRoutes`; and **W / HSI / UIO** **state-identifying
sequences** that let the oracle *confirm which state the real system is in* — catching
state-collapse / hidden-extra-state bugs that per-step `Classify` alone cannot, and upgrading a run
from "we drove events and nothing broke" to "we demonstrated **conformance up to a bounded fault
domain**." **Adopt:** Chinese-postman as a Planner mode; UIO/W-set state identification as an oracle
strengthener; the fault-domain (≤k extra states) framing to make coverage claims defensible.
**Leave:** full W-suites (huge) — prefer HSI/UIO.
https://www.sciencedirect.com/science/article/abs/pii/S0950584910001278

## Highest-value borrows (ranked)

1. **Filibuster's capture-then-fault-each loop + LDFI's backward-from-provenance pruning** — together
   they are the blueprint for `UMPIRE_TRACING.md`: observe a transition's call footprint, then fail
   each operation, using lineage-of-success to keep the `edge × op × fault-kind` budget sane.
   *Umpire's upgrade over both:* generic safety/liveness rules instead of per-test assertions.
2. **MOP parametric monitoring + LTL₃/end-of-trace semantics** — names and formally grounds what
   Umpire already does (per-entity monitors) and *why teardown-liveness is sound*; adopt monitor-
   from-spec + trace slicing + 3-valued verdicts.
3. **GraphWalker's generator/stop-condition taxonomy (+ online/adaptive mode)** — the Planner's
   missing pieces: coverage-as-stop-condition, weighted exploration, and adaptive replanning under
   Temporal's nondeterminism.
4. **rapid (shrinking + `MakeFuzz`)** — the Go-native way to add automatic counterexample
   minimization and coverage-guided fuzzing/input-mutation without leaving the language.
5. **Antithesis/TigerBeetle DST discipline** — `seed + server-version` repro tuple, dense always-on
   assertions, a seed-farming hub, and the explicit decision that Umpire's determinism is
   *boundary/generative*, not bit-perfect.
6. **Elle/Porcupine + Alloy** — the observe-and-judge, relational, Go-embeddable references for the
   cross-entity oracle and a rule DSL; **MaceMC's critical-transition** for debuggable liveness.
7. **FSM conformance theory** — Chinese-postman minimal tours and UIO/W state-identification to make
   the Planner's coverage and the oracle's state-confirmation *provably* meaningful.

## Where Umpire is genuinely different (and still right)

No external tool combines all four of Umpire's clauses at once. Runtime-verification frameworks
(MOP) monitor *in-process* one program, not a distributed server over telemetry. Observed-history
checkers (Elle) judge *one consistency model*, not open-ended per-entity lifecycles with a route
planner. Trace-derived fault tools (Filibuster) fault synchronous RPC graphs with *hand-written
per-test oracles* and weak async coverage. Model-based testers (GraphWalker) walk a graph with
*shallow assertions* and no cross-entity relational judging. DST systems (Antithesis/TigerBeetle)
assume they *own or virtualize* the runtime. Umpire's distinctive combination —
**parametric-RV judging + cross-entity relational invariants + a route planner over one shared
model + trace-derived fault injection + ride-along over the existing suite + portability tiers
(canary-capable)** — is unoccupied. This doc is the map of which *piece* each field has already
solved so Umpire can borrow the mechanism and keep the combination.

## Sources

Runtime verification: JavaMOP https://github.com/runtimeverification/javamop ·
monitoring LTL https://link.springer.com/article/10.1007/s10703-023-00429-8 ·
QRE https://www.cis.upenn.edu/~alur/Popl19.pdf
Checkers: Elle https://github.com/jepsen-io/elle · Porcupine https://github.com/anishathalye/porcupine ·
Maelstrom https://github.com/jepsen-io/maelstrom · Knossos https://github.com/jepsen/knossos
Trace-derived faults: Filibuster https://christophermeiklejohn.com/publications/filibuster-socc-2021.pdf ·
molly/LDFI https://people.ucsc.edu/~palvaro/molly.pdf ·
Netflix LDFI https://databeta.wordpress.com/2016/02/04/lineage-directed-fault-injection-at-netflix/ ·
3MileBeach https://people.ucsc.edu/~palvaro/3milebeach.pdf ·
trace-discovered models https://arxiv.org/abs/2506.11176 · async https://arxiv.org/pdf/2512.12314 ·
idempotency (Flux) https://www.usenix.org/system/files/osdi23-ding.pdf ·
chaos survey https://arxiv.org/html/2505.13654v1
MBT: GraphWalker https://graphwalker.github.io/ · AltWalker https://github.com/altwalker/altwalker ·
Spec Explorer https://www.microsoft.com/en-us/research/project/model-based-testing-with-specexplorer/ ·
Modbat https://github.com/cyrille-artho/modbat · ModelJUnit https://sourceforge.net/projects/modeljunit/
PBT: rapid https://github.com/flyingmutant/rapid · gopter https://github.com/leanovate/gopter ·
go fuzz https://go.dev/doc/security/fuzz/ · Quviq QC https://www.quviq.com/documentation/eqc/overview-summary.html ·
quickcheck-state-machine https://hackage.haskell.org/package/quickcheck-state-machine ·
Hypothesis https://hypothesis.readthedocs.io/en/latest/stateful.html · fast-check https://fast-check.dev/docs/advanced/model-based-testing/
DST: Antithesis https://antithesis.com/docs/resources/deterministic_simulation_testing/ ·
VOPR https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md ·
FoundationDB https://apple.github.io/foundationdb/testing.html ·
Nucleus https://dropbox.tech/infrastructure/-testing-our-new-sync-engine ·
madsim https://github.com/madsim-rs/madsim · Hermit https://github.com/facebookexperimental/hermit ·
rr https://rr-project.org/ · Resonate https://journal.resonatehq.io/p/deterministic-simulation-testing ·
primer https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html
Formal methods: P https://p-org.github.io/P/ · Apalache https://apalache-mc.org/ ·
AWS formal methods https://cacm.acm.org/research/how-amazon-web-services-uses-formal-methods/ ·
Alloy https://alloytools.org/ · MaceMC https://www.usenix.org/conference/nsdi-07/life-death-and-critical-transition-finding-liveness-bugs-systems-code ·
SAMC https://www.usenix.org/conference/osdi14/technical-sessions/presentation/leesatapornwongsa
Trace assertion: Tracetest https://github.com/kubeshop/tracetest
Oracles & conformance: metamorphic survey https://dl.acm.org/doi/10.1145/3143561 ·
Jepsen https://github.com/jepsen-io · FSM conformance survey https://www.sciencedirect.com/science/article/abs/pii/S0950584910001278
