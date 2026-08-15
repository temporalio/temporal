# Umpire and *eXtreme Modelling in Practice*

Date: 2026-08-15

Status: Research and recommendations; no conformance bridge or test generator described here is
implemented unless explicitly identified as current repository behavior.

## Executive conclusion

The paper's most useful lesson for Umpire is not “add trace checking.” It is to choose the
conformance technique per model slice:

- use **model-based test-case generation (MBTCG)** for small, deterministic modules with explicit
  inputs and outputs;
- use **model-based trace-checking (MBTC)** only where nondeterminism makes exact test generation
  impractical and semantic transitions can be observed without reconstructing lock-protected global
  state; and
- keep property-based acceptance tests, fuzzing, and specialized tests for behavior below or beyond
  those abstractions.

Umpire is already a promising hybrid in this sense. Its protocol, planner, sparse compiler, and
formal interpreter produce bounded semantic plans; its Monitor turns runtime observations into model
state, relations, rules, and normalized traces. But it does **not yet complete either paper-defined
conformance loop**: formal reachable-state traces do not become completed live regression plans, and
live implementation traces are not replayed step-by-step through `verify.Interpreter`. The existing
causal-footprint checks validate required observation shapes, not full model legality
([Umpire assurance boundary](UMPIRE.md#assurance-boundaries),
[`TraceRefinement`](common/testing/umpire/trace.go#L174-L206),
[`NormalizeCounterexample`](common/testing/umpire/verify/counterexample.go#L19-L58)).

The highest-value next increment is therefore:

1. preserve evidence provenance and define committed semantic cutpoints;
2. bridge a checked formal trace into a completed regression path;
3. run that same model-derived suite against the HSM and CHASM profiles; and
4. pilot the bridge on one small Nexus transition slice before attempting whole-server trace
   reconstruction or adding another formal backend or authoring DSL.

This recommendation is an inference from the paper and the current code, not a claim made by the
paper itself.

## What the paper actually did

The primary source is Davis, Hirschhorn, and Schvimer, [“eXtreme Modelling in
Practice”](https://www.vldb.org/pvldb/vol13/p1346-davis.pdf), PVLDB 13(9), 2020
([arXiv record](https://arxiv.org/abs/2006.00915),
[DOI](https://doi.org/10.14778/3397230.3397233)). The authors' first-party artifacts include the
[paper source](https://github.com/mongodb-labs/tla-trace-checker/tree/9ef27e6ab2a90d1cba20a13fe8ab35ee8ec8f0cd)
and the historical
[replication trace checker](https://github.com/mongodb-labs/repl-trace-checker/tree/695ed735b1e75fe6def50db60cbb34a55a22035c).

The paper tried four practices from the original eXtreme Modelling proposal:

1. model different aspects of a system with multiple specifications;
2. write a specification just before the corresponding implementation;
3. evolve model and implementation together; and
4. generate tests from the model and/or check whether implementation traces are legal model
   behaviors.

Both case studies actually began with existing implementations, so the paper is evidence about
retrofitting the conformance mechanisms, not a full empirical test of greenfield model/code
co-development ([paper §§1–3](https://arxiv.org/pdf/2006.00915)). The original proposal also
emphasizes close modeller/implementer collaboration and shared terminology
([Gravell et al., §§2.6, 3.1, and 4.3](https://arxiv.org/pdf/1111.2826)).

### Model-based trace-checking: expensive and abandoned

For MongoDB Server replication, existing JavaScript and randomized tests drove a replica set.
Test-only C++ instrumentation logged specification-level transitions; Python merged per-node logs,
sorted them by timestamps, reconstructed global states, generated `Trace.tla`, and invoked TLC to
ask whether the sequence was permitted by `RaftMongo.tla`. The companion artifact documents the
pipeline and its restrictions, including unsupported authentication and topology cases
([artifact README](https://github.com/mongodb-labs/repl-trace-checker/blob/695ed735b1e75fe6def50db60cbb34a55a22035c/README.md));
its state reconstruction contains implementation-specific assumptions such as making other nodes
followers when one becomes primary
([`update_state`](https://github.com/mongodb-labs/repl-trace-checker/blob/695ed735b1e75fe6def50db60cbb34a55a22035c/repl-trace-checker.py)).

Measured outcome ([paper §§4.1–4.2.5](https://arxiv.org/pdf/2006.00915)):

- the initial pool was 423 handwritten replication tests and 23 randomized suites;
- 120 of the 423 were incompatible with tracing; the remaining runs emitted 42,262 events;
- one representative rollback-fuzzer run emitted 2,683 events;
- only five handwritten tests and one randomized test were trace-checked, and only one handwritten
  trace passed;
- two engineers spent ten weeks: four weeks/570 C++ lines on instrumentation, three weeks/252
  changed TLA+ lines, and three weeks/484 Python lines on post-processing;
- the model grew from 42,034 states checked in two seconds to 371,368 states checked in 14 minutes;
  and
- the work never reached CI or accumulated coverage, and the team stopped because each additional
  specification appeared likely to cost nearly as much as the first. The artifact records the same
  stopping decision ([current-status note](https://github.com/mongodb-labs/repl-trace-checker/blob/695ed735b1e75fe6def50db60cbb34a55a22035c/README.md#current-status)).

The failure was architectural, not merely a tooling defect. Logging had to happen after a mutation
but before another node could observe it, making instrumentation part of the concurrency protocol.
Hierarchical locks, MVCC, latches, and futures made snapshots costly and potentially deadlocking.
The design model intentionally omitted real behavior such as transient dual leaders and term
gossip. Each mismatch then required fixing code, avoiding the behavior, complicating the model, or
rewriting the trace. The last two choices risked state explosion or an adapter bug that concealed an
implementation bug. A known initial-sync mismatch stopped one 2,683-event trace after four steps,
so nearly all evidence went unexamined ([paper §§4.2.1–4.2.3](https://arxiv.org/pdf/2006.00915)).

The authors' practical advice was to pair models with isolated implementation modules, simulate
nondeterministic inputs such as time, log only state readily available at a transition, and infer
hidden state only under explicit reliable assumptions. They warned that post-processing itself can
mask bugs ([paper §6](https://arxiv.org/pdf/2006.00915)).

### Model-based test-case generation: successful on a small kernel

For Realm Sync, the target was a deterministic operational-transformation kernel: 21 pairings among
six array operations, implemented in roughly 1,000 lines of C++. Engineers transcribed the merge
logic into TLA+, constrained the state space to three clients performing one operation each on a
three-element initial array, exported TLC's reachable graph, and used a 755-line Go generator to
emit C++ tests containing inputs, transformed operations, and expected final state
([paper §5](https://arxiv.org/pdf/2006.00915)).

Measured outcome:

- one engineer spent four weeks: two weeks/795 TLA+ lines on the model and two weeks/755 Go lines on
  the generator;
- TLC caught transcription errors while the model was written;
- TLC also found a nonterminating `ArraySwap`/`ArrayMove` path faithfully copied from mature C++,
  leading MongoDB to deprecate `ArraySwap` and exclude it from the remaining experiment;
- the generator emitted 4,913 tests;
- 36 handwritten tests covered 18/86 selected branches (21%), AFL covered 79/86 (92%) after about
  eight million executions, and generated tests covered 86/86 (100%); and
- all generated C++ tests passed ([paper §§5.1.3–5.2](https://arxiv.org/pdf/2006.00915)).

The result is strong but bounded. The coverage figure concerns one selected merge kernel after an
operation was excluded. Passing well-formed generated inputs does not rule out undefined behavior.
Direct transcription also correlates the oracle with the implementation: TLC found the shared
nontermination bug through exploration, but another shared semantic error could survive. The paper
does not publish the full Realm model, generator, or generated corpus; its paper-source repository
contains only the displayed
[invariant](https://github.com/mongodb-labs/tla-trace-checker/blob/9ef27e6ab2a90d1cba20a13fe8ab35ee8ec8f0cd/array_ot_invariant.tla)
and
[merge-rule excerpt](https://github.com/mongodb-labs/tla-trace-checker/blob/9ef27e6ab2a90d1cba20a13fe8ab35ee8ec8f0cd/array_ot_merge_example.tla).

## How this maps to Umpire

### The fit is a hybrid, but the loops remain open

| Umpire mechanism | Closest paper technique | What exists now | Missing conformance step |
| --- | --- | --- | --- |
| Protocol lifecycle planning and sparse regression compilation | MBTCG | A validated protocol derives actions and lifecycle routes; sparse intent is completed into bounded executable paths ([protocol](tests/umpire2/internal/protocol/protocol.go), [compiler](common/testing/umpire/regress/compiler.go)) | Paths are not generated from the formal checker's reachable graph with model-derived expected outputs |
| Monitor, facts, relations, lifecycle transitions, and rules | MBTC | Runtime observations update a semantic model and properties are judged at checkpoints ([runtime](common/testing/umpire/runtime.go), [overview](UMPIRE.md#monitor-observe-and-evaluate)) | A live trace is not checked as a sequence of legal `verify.Model` transitions |
| Normalized traces and causal footprints | MBTC infrastructure | Required/forbidden observation patterns and causality are checked inside action windows ([trace checker](common/testing/umpire/trace.go#L205-L278), [footprints](tests/umpire2/internal/protocol/causal_footprints.go)) | This checks trace shape, not abstract pre-state, enabled action, branch, and post-state |
| Formal model family and backends | Model exploration | One model family projects bounded modules/targets to TLA+, P, Ivy, and Fizz; CI checks generated artifacts and formal runs ([model family](common/testing/umpire/verify/family.go), [workflow](.github/workflows/umpire-model-verification.yml)) | A checked trace/counterexample cannot yet be executed against Temporal |
| Canonical formal interpreter and counterexample normalization | A reusable bridge component | Backend traces are replayed through `verify.Interpreter` and rejected when ambiguous or invalid ([normalizer](common/testing/umpire/verify/counterexample.go)) | It currently normalizes formal-backend evidence, not independently observed Go execution |

Calling Umpire a hybrid is therefore accurate at the architecture level, but claiming that it
already implements the paper's MBTC or MBTCG would overstate the code.

### Multiple aspect models fit “one semantic source”

EXM's multiple specifications do not require competing vocabularies. Umpire's
[`ModelFamily`](common/testing/umpire/verify/family.go#L97-L105) already provides modules, interfaces,
obligations, compositions, refinement maps, and bounded verification targets, while the compiled
[`Protocol`](tests/umpire2/internal/protocol/protocol.go#L70-L82) owns the canonical runtime
vocabulary. The right interpretation of “one semantic source” is one validated vocabulary and
refinement graph with multiple purpose-built projections, not one monolithic model of all Temporal.

This directly addresses the paper's abstraction tension. A design target may omit transport and
storage detail; a conformance target may select observable cutpoints and explicit stuttering
actions. Both should declare their relationship rather than forcing the design model to mirror Go
statements.

### HSM/CHASM parity is the strongest immediate analogy

The Realm success used one model-derived suite to keep independent implementations aligned. Umpire
already exposes the same sparse regression through CHASM or HSM local presets via
[`WithCHASM`](tests/umpire2/umpiretest/regression.go#L102-L115), and the compiled suite is retained in
the runner result ([`RunRegression`](tests/umpire2/umpiretest/regression.go#L22-L72)).

**Inference:** for plans restricted to capabilities shared by both profiles, compile once, run the
same completed paths and model-derived expected deltas under HSM and CHASM, and compare semantic
claims rather than protobufs or schedules. This is more faithful to the successful Realm experiment
than beginning with arbitrary whole-server traces. It can reveal behavior drift during migration
while preserving each implementation's internal mechanics.

### Evidence provenance and committed cutpoints are P0 gaps

The paper's failed trace checker demonstrates how a seemingly mechanical reconstruction layer can
become an untrusted second implementation. Two current Umpire details would recreate that risk:

1. [`Fact`](common/testing/umpire/entity.go#L62-L66) exposes only a name and target; it carries no
   source, source sequence, clock domain, or derivation metadata. Although `TraceEvent` supports
   those fields, [`executionTrace.recordFacts`](tests/umpire2/execution_trace.go#L128-L204) labels
   every fact and every relation/lifecycle transition derived from it as `in-process`. This erases
   whether the evidence actually came from gRPC, history, telemetry, or a direct in-process signal,
   and makes evidence qualification stronger than the retained provenance justifies.
2. [`Transition.Apply`](chasm/statemachine.go#L55-L113) deliberately emits `chasm.transition`
   telemetry even when source validation or the apply callback fails, attaching the error as an
   attribute. [`ChasmTransition.ImportSpanEvent`](tests/umpire2/internal/fact/chasm_transition.go#L40-L52)
   does not decode that error. It records source, destination, event, identity, and attempt, so an
   attempted transition can be interpreted as an applied transition. Even a successful `Apply`
   precedes transaction close/persistence, so it is not yet evidence of a committed transition.

Before Umpire treats runtime observations as formal refinement evidence, provenance must survive
raw observation -> fact -> relation/transition -> trace, and CHASM must distinguish at least
`attempted`, `applied`, `committed`, and `aborted` cutpoints. A failed or merely applied transition
must never advance the conformance state as if it committed. Black-box profiles that cannot observe
the cutpoint should return `unsupported` or `inconclusive`, consistent with the existing
[`EnvironmentProfile`](common/testing/umpire/environment_profile.go), rather than infer success.

## What Umpire should adopt

The following are recommendations inferred from the evidence above.

### 1. Build the formal-trace-to-regression bridge first

Add a small bridge from canonical formal traces to `regress.CompletedPath`:

```text
verified target + model hash + canonical TraceStep[]
  -> lifecycle/regression refinement lookup
  -> completed actions + symbolic/fresh bindings + expected abstract deltas
  -> existing regression executor
  -> independently observed semantic deltas and qualified claims
```

The bridge should reject an abstract action that has no unique executable refinement, not silently
omit it. Its artifact should retain target, bounds, model hash, action/binding sequence, expected
deltas, omissions, profile, and observations. This reuses the current interpreter, refinement
metadata, completed-path executor, artifact format, and model hashes instead of creating another
planner.

This is MBTCG when the bridge enumerates a bounded checked state graph into a corpus. It is also the
foundation for replaying a formal counterexample against Go. Prefer stable JSON cases consumed by a
table runner over generating thousands of Go source files unless compilation performance proves
acceptable.

### 2. Make live trace refinement executable, narrow, and evidence-qualified

For a selected target and semantic cutpoint, implement:

```text
provenanced observations -> abstract delta/candidate action
  -> verify.Interpreter.Enabled/Step
  -> accepted | violated | inconclusive(ambiguous/incomplete) | unsupported
```

Allow only declared stuttering and explicit existential hidden state. Do not embed ad hoc repair
logic in a log parser. Check short action windows or module-level traces and retain resumable
checkpoints, so one mismatch does not automatically discard thousands of later independent windows.
The projection/refinement adapter must have unit tests and seeded mutations because it is part of
the oracle.

### 3. Declare each target's conformance purpose

Extend target/manifest metadata with the intended use: design verification, test generation, trace
conformance, or some combination. A conformance-capable target should name:

- observable variables and cutpoints;
- concrete-to-abstract action and identity mappings;
- permitted stuttering and hidden-state policy;
- evidence sources and ordering requirements;
- bounds, symmetry reductions, and excluded input classes; and
- known implementation divergences, classified as unsupported rather than normalized away.

This preserves small abstract design models while making conformance projections honest.

### 4. Use one corpus against both implementations

For the common HSM/CHASM capability subset, compile the sparse/formal suite once and execute the
identical completed paths under both profiles. Compare each implementation to the model-derived
expected deltas and compare their qualified semantic claims to one another. This triangulation is
stronger than merely comparing HSM with CHASM: two implementations can agree on the same bug.

Regenerate and run the corpus in the same PR as model or implementation changes. Embed the model
hash in every case and fail CI on stale artifacts, building on the current generated-model check
([generator command](cmd/umpire-genmodels/main.go),
[CI check](.github/workflows/umpire-model-verification.yml#L100-L106)).

### 5. Measure oracle strength and marginal cost

For each pilot, report:

- abstract actions, branches, relations, and properties covered within the declared bound;
- implementation branch coverage as a diagnostic, not proof;
- generated case count, generation time, execution time, and deduplication ratio;
- accepted, violated, ambiguous, incomplete, and unsupported traces;
- seeded model, implementation, and adapter mutations detected;
- state-space growth when conformance detail is added; and
- engineering effort and adapter size for the first and next model slices.

The last metric directly tests the assumption that shared Umpire infrastructure lowers marginal
cost—the assumption that failed in MongoDB's MBTC study.

## What does not fit

Umpire should not copy these parts of the MongoDB experiment:

- **No global timestamp order or logging sleeps.** The paper's nodes ran on one machine and slept
  until a millisecond timestamp changed. Umpire correctly prefers causal references or comparable
  source sequences and treats clocks as explicit domains
  ([trace ordering](common/testing/umpire/trace.go#L249-L278),
  [environment ordering guarantees](common/testing/umpire/environment_profile.go#L18-L49)).
- **No whole-process consistent snapshots as the first target.** Observability can alter locks and
  timing. Capture semantic events at commit/transaction seams, then derive only declared abstract
  state.
- **No universal direct transcription.** Close correspondence made Realm MBTCG feasible, but a
  statement-shaped model of distributed Temporal would lose design clarity and explode its state
  space. Reserve transcription-like reference models for small pure reducers.
- **No silent trace repair or skipped behavior.** Classify a mismatch as implementation bug, model
  bug, adapter bug, unsupported behavior, deliberate divergence, or incomplete evidence.
- **No claim that coverage proves conformance.** The paper's 100% branch result was bounded and
  survived the risk of a model and implementation sharing a bug. Umpire's own coverage contract
  correctly treats coverage as exercised obligations, not correctness
  ([`Coverage`](common/testing/umpire/coverage.go), [assurance boundary](UMPIRE.md#assurance-boundaries)).
- **No expectation that every specification becomes cheap after infrastructure work.** Observation
  and refinement mappings are model-specific. Require demonstrated marginal-cost reduction before
  broad rollout.
- **No whole-server exhaustive live execution.** Exhaust the abstract state space where feasible;
  use constrained pairwise selection, risk/novelty prioritization, and hard live budgets elsewhere
  ([pairwise generator](common/testing/umpire/matrix.go),
  [campaign selection](common/testing/umpire/campaign/selection.go)).

## Recommended experiments

### Experiment 0: close the evidence gap

Preserve source provenance on facts and all derived events. Decode the CHASM transition error and
add a transaction/persistence-correlated committed outcome. Prove with tests that invalid-source,
apply-error, close-error, and successful-commit cases produce different abstract evidence. This is
a prerequisite, not optional telemetry polish.

### Experiment 1: one small Nexus MBTCG pilot

Use the retry/backoff slice already proposed in
[`UMPIRE_CODEGEN.md`](UMPIRE_CODEGEN.md#nexus-operation-is-the-right-pilot): retryable attempt
failure, entry to backing off, matching/stale backoff firing, and schedule-to-close timeout. Model
the slice as a bounded verification target, enumerate canonical interpreter traces, lower them to a
completed corpus, and run them first against an isolated CHASM reducer or `chasmtest.Engine` with
simulated time. Include valid and invalid source states, duplicate/stale task delivery, close/reload
boundaries, and deliberately faulty reducers.

Success means deterministic regeneration; complete abstract transition coverage within declared
bounds; all expected deltas independently observed; curated implementation and bridge mutations
caught; and tractable generation/execution cost. Branch coverage is reported but is not the exit
criterion.

### Experiment 2: HSM/CHASM parity

Select only actions both profiles support. Execute the exact Experiment 1 corpus—or the nearest
shared Nexus slice—against HSM and CHASM. Require both to refine the model and compare their
qualified semantic outcomes. Record implementation-specific unsupported observations instead of
weakening the shared property.

### Experiment 3: module-level MBTC

Feed committed, provenanced CHASM transition windows into the canonical interpreter. Simulate time
and task delivery; allow only declared stuttering; report ambiguous hidden state as inconclusive.
Measure how much model detail and adapter code are required. Expand to a local multi-component test
only if the module pilot shows useful mutation detection and lower marginal cost.

### Stop rule

Do not proceed to whole-server trace conformance if the pilot requires snapshots of lock-protected
global state, substantial behavior-repair post-processing, or near-per-target instrumentation. In
that case, retain model-generated module tests plus Umpire's existing evidence-qualified acceptance
properties. That is not a lesser fallback; it is the portfolio the paper's two case studies support.

## Decision

Adopt EXM as a **development discipline and conformance portfolio**, not as a mandate for one
universal trace checker. Umpire's existing canonical vocabulary, model family, sparse plans,
interpreter, evidence profiles, and bounded execution make it better positioned than the MongoDB
prototype. Its remaining high-risk gap is precisely the one the paper exposes: the trusted bridge
between abstract behavior and real observations.

Prioritize provenance, committed cutpoints, formal-trace lowering, and the small Nexus HSM/CHASM
pilot. Defer whole-server MBTC and further backend/DSL expansion until that bridge demonstrates
fault-detection value, reproducibility, and decreasing marginal cost.
