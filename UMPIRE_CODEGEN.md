# Umpire, formal languages, and code generation

Status: design exploration, 2026-08-15.

This note treats “Plang” as the current [P language](https://p-org.github.io/P/) used by
Umpire's generated verification toolchain.

## Recommendation

Umpire should pursue spec-first development in two distinct forms:

1. **Behavioral spec to executable tests and oracles.** Keep Umpire's protocol/model family as the
   common semantic contract, project it into several verification engines, turn formal traces into
   completed Umpire plans, drive the real Go server, and compare independently observed facts with
   the expected abstract transition. This is Umpire's core mission and is practical for both CHASM
   itself and features built on CHASM.
2. **Verified source to Go for small, pure kernels.** Pilot Dafny for deterministic transition
   reducers and validators whose interface is ordinary values in and a decision/value out. Keep
   persistence, protobuf adaptation, `chasm.MutableContext`, task dispatch, metrics, logging, and
   distributed scheduling in hand-written Go.

Do **not** attempt to generate all of Temporal, all of CHASM, or even a complete CHASM component
from Agda, Dafny, Lean, P, Ivy, or TLA+. Of these, only Dafny currently has a documented direct Go
backend, and Dafny is a sequential language. The other languages can still materially improve the
Go implementation by proving abstractions, finding schedules, generating tests, and acting as
independent reference oracles.

The most useful near-term experiments are:

- make CHASM transaction outcomes observable as normalized, replayable Umpire facts;
- add a Umpire-to-Dafny backend and use it first to cross-check Umpire's own pure verification
  interpreter;
- extract one Nexus Operation transition slice into a pure Dafny reducer, compile it to Go, and
  compare it against the current Go transition under `chasm.MockMutableContext` before considering
  it for production;
- prototype an Umpire-to-[Veil](https://veil.dev/) projection as the practical Lean route for
  unbounded safety proofs over transition systems.

## The important distinction: what artifact is being verified?

“Specification language” covers several substantially different things. The distinction matters
more than the surface syntax.

| Language | Primary artifact | What automation establishes | Executable/code path | Best Umpire role |
| --- | --- | --- | --- | --- |
| TLA+ | A mathematical set of state behaviors | TLC explores bounded finite states; Apalache provides symbolic bounded safety and invariant checking | PlusCal translates to TLA+, not production code | Abstract concurrency, crash/recovery, refinement, counterexample schedules |
| P | Asynchronously communicating state machines plus monitors | Systematic exploration of message schedules, nondeterminism, safety, and liveness | Current compiler emits a C# representation for P Checker, not Go | Distributed schedules, monitor-driven traces, log conformance |
| Ivy | Composable protocol interfaces, monitors, and implementations | Automated invariant/BMC reasoning in restricted first-order fragments; assume/guarantee testing | Testers and extracts compile to C++ | Component test bench and oracle generation |
| Dafny | A sequential implementation with contracts and ghost state | SMT-backed functional verification of the implementation against pre/postconditions, frames, invariants, and termination | Direct Go source backend and an FFI to Go | Verified pure reducers, validators, reference models, test generation |
| Lean | Definitions and propositions in dependent type theory | Kernel-checked proofs, with tactics/SMT-like automation layered above | Lean compiles modules through C; C ABI exists but is documented as unstable | Deep/unbounded proofs, verified reference semantics, Veil transition systems |
| Agda | Total dependently typed programs/proofs | Construction of a term/program whose type is the specification, plus coverage and termination checks | Haskell and JavaScript backends, not Go | Small proof-carrying reference semantics and research experiments |

The first three are primarily **behavior/model oriented**. Dafny is primarily **program oriented**.
Lean and Agda can be both, but usually require substantially more proof engineering. That is why
Dafny is the plausible code-generation pilot while P/TLA+/Ivy remain stronger first choices for
schedule exploration and implementation conformance.

## Where Umpire is already well positioned

Umpire already has most of the language-neutral waist this design needs:

- [`verify.Model`](./common/testing/umpire/verify/model.go) represents bounded identities, entity
  states, relations, guarded actions, branches, properties, abstractions, inventory, and live
  refinement metadata.
- [`verify.ModelFamily`](./common/testing/umpire/verify/family.go) decomposes that model into owned
  modules, interfaces, assumptions/obligations, refinement maps, compositions, and bounded targets.
- [`verify.Interpreter`](./common/testing/umpire/verify/interpreter.go) is a pure executable semantics
  for enabled actions, steps, property evaluation, and finite exploration.
- [`NormalizeCounterexample`](./common/testing/umpire/verify/counterexample.go) replays a backend
  trace through the canonical interpreter and rejects mismatched, ambiguous, or non-violating
  traces.
- [`toolchain`](./common/testing/umpire/verify/toolchain/toolchain.go) already generates TLA+, P,
  Ivy, and Fizz and normalizes their results into a shared evidence vocabulary.
- The canonical Temporal protocol is lowered into the verification model by
  [`Protocol.VerificationModel`](./tests/umpire2/internal/protocol/verification.go), then split into
  verification targets and explicit refinement closure by
  [`Protocol.VerificationFamily`](./tests/umpire2/internal/protocol/verification_family.go).
- The generated manifest records hashes, source provenance, bounds, abstractions, properties, and
  backend requirements. This is a suitable place to record proof assumptions and generated-code
  provenance too.

This is stronger than starting with a hand-written file in every formal language. The desired
shape is:

```text
                     generated model/proof backends
                TLA+ / P / Ivy / Dafny / Lean-Veil
                              |
                              v
canonical protocol -> model family -> checked traces / proof results
        |                                      |
        |                                      v
        +-> plan/action catalog -> Go realizers -> running Temporal
                                                   |
                                                   v
                                independently observed Umpire facts
                                                   |
                                                   v
                                normalized trace + qualified claim
```

Generated backends must remain projections, not six competing sources of truth. Conversely, a
green result from six projections cannot prove the source model is complete or correct: all six
can share the same missing behavior. Umpire therefore still needs independent observations,
mutation tests, deliberately faulty implementations, and explicit refinement checks against the
Go server.

### Source-of-truth rule

Until another authoring frontend is adopted, the compiled Go protocol declaration remains the
source of Umpire's behavioral contract. If the descriptor frontend proposed in
[`UMPIRE_STARLARK.md`](./UMPIRE_STARLARK.md) is adopted, that descriptor becomes the authoring
source and `verify.ModelFamily` remains a validated projection. TLA+, P, Ivy, Fizz, Dafny proof
models, and Veil models are always derived artifacts.

A Dafny reducer occupies a different level: its `.dfy` file may be the source of one concrete,
pure implementation, while the Umpire model remains the source of the externally visible behavior
that implementation must refine. The refinement mapping and differential tests connect the two.
Dafny-generated Go is derived, never edited, and the hand-written CHASM adapter is an explicit
trusted boundary. Without this division, adding Dafny would create a second behavioral model and
make drift more likely rather than less.

### The current IR is intentionally too small for all of CHASM

The v1 verification algebra handles finite entity existence/state, binary relations, Boolean
formulae, and finite action effects. That is exactly why it lowers cleanly to several backends. It
does not yet express the CHASM component tree, typed field values, versioned transition tokens,
logical and physical task multisets, deadlines, transaction-local dirty state, persistence
snapshots, crashes, or reloads.

Do not make every existing backend absorb those concepts at once. Add a layered model:

- retain `verify.Model` as the portable lifecycle/relation kernel;
- introduce a CHASM semantic module with values such as `Tree`, `Node`, `Ref`, `Transaction`,
  `Mutation`, and `TaskSet`;
- declare per-backend capabilities, and report an unsupported semantic feature rather than silently
  weakening it;
- project small finite instances to TLA+/P and selected first-order fragments to Ivy/Veil;
- allow Dafny or Lean to consume richer deterministic functions without requiring every backend to
  understand them.

That is a deep module boundary: a small `Initial`, `Enabled`, `Step`, `Observe`, and `Check`
interface can hide multiple formal encodings while preserving a single trace vocabulary.

## What each new language adds

### Dafny: the practical verified-to-Go experiment

Dafny combines imperative programs and executable functions with specifications checked by an
SMT-backed verifier. Its contracts cover preconditions, postconditions, frame conditions, loop
invariants, and termination ([Dafny overview](https://dafny.org/latest/)). It directly emits Go
source compatible with Go 1.18 and supports `{:extern}` declarations for existing Go packages
([compiler targets](https://dafny.org/latest/Installation),
[Go backend and externs](https://dafny.org/latest/Compilation/Go)).

This is genuinely different from Umpire's P/TLA+/Ivy use: the verified Dafny body can be the body
that executes in the application. It is therefore the only language in this comparison that makes
“write the original implementation there and generate Go” a reasonable experiment.

The boundary is still important:

- Dafny is sequential and has no native construct for spawning or controlling concurrent
  execution. `{:concurrent}` only restricts a method so target-language callers can safely invoke
  it concurrently ([Dafny reference](https://dafny.org/dafny/DafnyRef/DafnyRef)). It is not a
  replacement for P or TLA+ schedule semantics.
- Generated types are not idiomatic Go equivalents. Mathematical integers, sequences, sets, maps,
  traits, and generic values use the Dafny runtime and generated representations
  ([Go compilation details](https://dafny.org/latest/Compilation/Go)). This matters in hot paths and
  at protobuf/Temporal API boundaries.
- An `{:extern}` postcondition is an assumption about hand-written Go, not a proof of it. `dafny
  audit` reports such assumptions, and Dafny can add runtime contract checks
  ([audit documentation](https://dafny.org/v4.5.0/DafnyRef/DafnyRef)). Umpire should retain those
  assumptions in its manifest and exercise the extern boundary with generated and fuzz tests.
- Dafny's solver-assisted `generate-tests` can derive inputs for implementation block/path
  coverage, but the feature remains explicitly experimental
  ([test-generation documentation](https://dafny.org/v4.5.0/DafnyRef/DafnyRef),
  [maintainer example](https://dafny.org/blog/2023/12/06/automated-test-generation-chess-puzzles-with-dafny/)).

Good Dafny candidates in this repository are pure, bounded operations such as:

- evaluation of a guarded Umpire action and its state delta;
- relation-cardinality validation;
- ref/version acceptance rules represented as values;
- tree mutation validation and canonicalization independent of protobuf serialization;
- task invalidation/deduplication decisions represented as sets and returned as a decision;
- feature transition reducers that return the new component state and logical task intents.

Bad candidates are the CHASM engine, mutable tree object graph, persistence transactions, shard
ownership, gRPC handlers, actual task execution, telemetry, and anything whose useful semantics
depend on Go interfaces/reflection or concurrent Temporal runtime behavior.

### Lean: proofs that outgrow the portable IR

Lean is both an interactive theorem prover and a strict functional programming language. Tactics
produce proof terms checked by a small kernel ([Lean reference](https://lean-lang.org/doc/reference/latest)).
Lean compiles each module to C before native compilation
([compilation pipeline](https://lean-lang.org/doc/reference/latest/Elaboration-and-Compilation/)). It
has a C ABI, but the Lean documentation explicitly calls the current FFI unstable
([Lean FFI](https://lean-lang.org/doc/reference/latest/Run-Time-Code/Foreign-Function-Interface/)).

Go could call Lean through cgo, but that would add a native runtime, ABI/versioning, memory ownership,
cross-compilation, and scheduler boundary in a core server. Go's own documentation describes cgo
as a Go-to-C bridge with distinct pointer and allocation rules
([cgo](https://pkg.go.dev/cmd/cgo)). That is a poor default for Temporal production code.

Lean is nevertheless valuable in two forms:

1. **Meta-theory for Umpire.** Formalize the semantics of `verify.Model` once, then prove generic
   results such as “`Step` cannot create an undeclared entity,” “relation effects preserve declared
   endpoint types,” or “a normalized counterexample really reaches a violating state.” This tests
   Umpire's verification machinery, not just one Temporal feature.
2. **Transition-system proof through Veil.** Veil is a Lean-embedded language for specifying,
   model-checking, testing, and proving safety properties of distributed transition systems. It
   offers concrete/symbolic model checking and invariant proofs, with Lean available when
   automation stops ([Veil](https://veil.dev/),
   [CAV paper](https://verse-lab.github.io/papers/veil-cav25.pdf)). It is the most plausible Lean
   integration for CHASM, and much closer to Umpire's current model than raw theorem-prover code.

Veil is a young dependency. Treat it as an optional pinned backend until its trace format, model
checking, and headless CI workflow demonstrate stability. A Veil proof should produce a normal
`verify.Result` with tool version, assumptions, target hash, and qualified property claims rather
than a separate success channel.

### Agda: a reference-semantics laboratory

Agda is a dependently typed, total programming language and proof assistant. A well-typed ordinary
program terminates and pattern definitions are checked for coverage; `--safe` disables escape
hatches such as postulates, incomplete matches, and disabled termination checking
([Agda introduction](https://agda.readthedocs.io/en/latest/getting-started/what-is-agda.html),
[Safe Agda](https://agda.readthedocs.io/en/stable/language/safe-agda.html)). This makes illegal
states, proof-carrying inputs, and total reducer functions natural to express.

Agda's official compiler targets are Haskell and JavaScript, not Go
([Agda compilers](https://agda.readthedocs.io/en/stable/tools/compilers.html)). Its Haskell FFI can
replace a proved definition with runtime foreign code, but the documentation warns that the two
definitions are not checked for behavioral agreement
([Agda user manual](https://agda.readthedocs.io/_/downloads/en/v2.8.0/pdf/)). That is the same
refinement gap Umpire is trying to close, moved to a more awkward runtime.

Agda is therefore useful for a bounded research spike—a typed Umpire IR, a total path/ref
interpreter, or a proof about state-delta normalization—with results exported as JSON test vectors.
It is not a practical first integration or production code generator for this Go repository.

## How these differ from P, Ivy, and TLA+

### P: executable asynchronous models, not Go implementation generation

P's native abstraction is a collection of asynchronously communicating state machines. The
checker systematically explores message interleavings and nondeterministic choices against safety
and liveness monitors ([P semantics](https://p-org.github.io/P/advanced/psemantics/),
[P monitors](https://p-org.github.io/P/manual/monitors/)). That is a natural match for CHASM task
delivery, retries, cancellation races, persistence acknowledgements, and competing external
events.

The current documented compiler produces a C#/.NET representation that P Checker explores
([Using P](https://p-org.github.io/P/getstarted/usingP/)). This “code generation” is not a Go
backend and should not be described as a production generator for Temporal. P's newer PObserve
path validates implementation logs against P specifications
([P framework](https://p-org.github.io/P/)); that is philosophically almost identical to Umpire's
observation path and worth learning from.

Umpire should deepen P for schedule generation and trace conformance, not use it to generate
server code. A P counterexample should become an Umpire semantic plan whose events are realized
against `chasmtest.Engine` or a real test cluster, after which Umpire independently observes the
Go behavior and compares the normalized trace.

### Ivy: the closest existing example of spec-generated tests

Ivy is deliberately restricted so invariant checking and bounded model checking stay in useful
decidable fragments of first-order logic
([Ivy language](https://microsoft.github.io/ivy/language.html),
[decidability](https://microsoft.github.io/ivy/decidability.html)). Its monitors synchronize around
interface calls, and it can generate randomized environments plus executable specification
oracles. Its compositional testing documentation explicitly demonstrates a generated C++ test
environment driving an implementation while monitors check the trace
([Ivy compositional testing](https://microsoft.github.io/ivy/examples/testing/specification.html)).

Ivy can also extract a verified implementation to C++, provided its assumptions are represented in
the extract ([Ivy extraction](https://microsoft.github.io/ivy/examples/helloworld.html)). For a Go
server this still implies C++/cgo or a process bridge, so its extraction route is not attractive for
production CHASM. Its test-bench design is directly relevant, however:

- generate the environment separately from the monitor;
- retain assumptions and guarantees per component interface;
- use an assumption failure to qualify the claim rather than blaming the implementation;
- generate values satisfying the precondition rather than discarding mostly-invalid random data.

Umpire's existing Ivy generator currently uses Ivy as another verifier for the portable finite IR.
A second phase could generate an Ivy test isolate whose exported actions call a small Go adapter or
emit a JSON action stream, and whose monitor outcomes return through Umpire's result vocabulary.

### TLA+: the strongest abstraction boundary and the weakest codegen story

TLA+ specifies systems above the code level as mathematical state behaviors
([high-level view](https://lamport.azurewebsites.net/tla/high-level-view.html)). TLC explicitly
explores finite-state specifications and simulation traces; Apalache supplies a symbolic route
([TLA+ tools](https://lamport.azurewebsites.net/tla/tools.html)). PlusCal is translated into a TLA+
specification and checked there—it is not compiled into production code
([PlusCal introduction](https://lamport.azurewebsites.net/tla/tutorial/intro.html)).

That separation is a feature for modeling CHASM framework behavior. A TLA+ model can abstract away
protobufs, Go objects, persistence implementations, and queues while retaining atomicity, crash
points, version order, fairness, and task-delivery nondeterminism. It cannot by itself establish
that the Go code refines that abstraction. Umpire closes the gap by converting checked or violating
traces into executable plans and checking the Go observations against the same refinement map.

## Modeling CHASM itself

The framework model should describe the contract visible to every CHASM library, not reproduce
[`tree.go`](./chasm/tree.go) statement by statement. The current Go seams suggest the following
abstract state:

- a rooted, path-addressed component/data/collection tree;
- component registration/type and parent/child metadata;
- component lifecycle, including open, paused, closed, terminated, and detached behavior;
- stable persistence snapshot plus one transaction-local mutation;
- component refs carrying execution identity, component path, archetype, and version information;
- logical task intents and emitted physical tasks, including scheduled time, destination,
  validation state, and singleton/deduplication identity;
- execution state, current run, versioned transition, request ID, reuse/conflict policy, and
  speculative transition state;
- environment actions for crash, reload, replication, task delivery/duplication, timer firing, and
  concurrent/stale requests.

The model should begin with properties already implicit in the implementation:

1. **Tree integrity.** Paths are unique; every non-root node has exactly one live parent; component
   and data field types agree with registration; pointers resolve only to permitted nodes; deleting
   a subtree cannot leave reachable children or dangling task ownership.
2. **Lifecycle containment.** A closed parent rejects mutation of ordinary descendants; detached
   descendants obey their explicit exception; a terminal root maps the execution to a closed
   state; pausing and termination have the intended task-validity consequences.
3. **Transaction atomicity.** A failed transition/close publishes neither partial node mutations
   nor tasks. A successful close returns one coherent mutation whose serialized nodes, structural
   changes, metadata, visibility, and task validity describe the same logical post-state. Crash
   before persistence is equivalent to the old snapshot; crash after persistence reloads the new
   snapshot.
4. **Ref consistency.** The three levels in
   [`RefConsistencyLevel`](./chasm/engine.go)—execution-last-update, component-creation, and
   current-run—accept exactly the intended stale references. Weakening consistency never silently
   grants a different archetype or an unauthorized component, and current-run users must
   re-establish semantic identity.
5. **Task safety.** A task is emitted only for a component present at close; invalid tasks cannot
   execute; duplicate or stale delivery is idempotent or rejected; singleton replacement/ignore
   policies preserve at most one logical task; pure tasks cannot expose an intermediate tree.
6. **Replication/reload.** Applying the same persisted mutation twice is harmless where the API
   promises idempotence; active/standby application preserves version order; a reload does not
   resurrect deleted nodes or invalid tasks.
7. **Observation fidelity.** Every model-significant committed transition, task decision, and
   persistence outcome produces enough non-secret evidence to reconstruct one unambiguous abstract
   action.

### Which backend should check which part?

- **TLA+ or P:** transaction/crash interleavings, task delivery, timer and retry schedules,
  concurrent/stale requests, active/standby behavior, and bounded liveness.
- **Ivy or Veil:** inductive tree/ref/task invariants and assume/guarantee contracts at Engine,
  persistence, and task-processor interfaces.
- **Dafny:** pure `ApplyMutation`, ref acceptance, task-set update, tree validation, and
  serialization-independent close decisions.
- **Lean:** generic proofs about the richer semantic model or refinement between an abstract
  transaction and its detailed phases when bounded checking stops being enough.
- **Agda:** optional total reference functions and typed test-vector generation.

No single backend needs the entire model. `ModelFamily` should own the decomposition and record the
interfaces/assumptions between the tree, transaction, reference, task, and persistence modules.

### Observation changes needed before conformance is credible

[`chasm.NewTransition`](./chasm/statemachine.go) emits one generic transition telemetry event, and
[`fact.ChasmTransition`](./tests/umpire2/internal/fact/chasm_transition.go) routes it into Umpire.
That is a good start, but today it only routes Nexus Operation transitions and observes source,
destination, event, identity, and attempt. `Transition.Apply` emits the event even when source
validation or the apply callback returns an error, attaching an error attribute, while the Umpire
fact currently does not retain the outcome.

Before using these events as a refinement oracle, add a normalized outcome such as `attempted`,
`applied`, `committed`, or `aborted` and correlate the event with the transaction close/persistence
result. A destination named in an attempted transition is not evidence that it committed. Add
test-only/in-process facts at these seams:

- transition accepted/rejected and callback outcome;
- `CloseTransaction` success/failure plus canonical mutation/task summary;
- persisted/reloaded snapshot version;
- task emitted, invalidated, delivered, deduplicated, and completed;
- ref resolution result and selected consistency level.

The evidence should contain semantic IDs, hashes, counts, enums, and versions—not payloads,
headers, failure details, or secrets. Black-box environments will justify weaker claims when these
facts are unavailable, consistent with Umpire's existing `EnvironmentProfile` design.

## Modeling a feature built on CHASM

A CHASM feature model has two layers:

1. **Feature reducer:** given abstract component state, a semantic event, logical time, and selected
   external outcomes, return a new state plus logical effects such as tasks, child creation,
   relations, visibility changes, or an error.
2. **CHASM realization:** adapt protobufs and Temporal inputs to the reducer, apply its decision
   through `MutableContext`, persist via CHASM, execute tasks, and expose observations.

This separates what can reasonably be generated and proved from what must remain integrated Go.
The reducer should look conceptually like:

```text
Reduce(State, Event, Inputs) -> Decision

Decision = {
    State,
    ChildMutations,
    LogicalTasks,
    VisibilityChanges,
    Outcome
}
```

The interface must not expose `proto.Message`, `context.Context`, reflection, loggers, metrics,
network clients, or `chasm.MutableContext`. Those belong in a thin Go adapter. The reducer's output
is deterministic; nondeterministic environment results are explicit inputs. That makes the module
deep, testable in isolation, model-checkable as an action, and eligible for Dafny generation.

### Nexus Operation is the right pilot

[`nexusoperation.Operation`](./chasm/lib/nexusoperation/operation.go) is a real CHASM component with
persisted protobuf state, child Cancellation/Outcome/Visibility fields, a parent `OperationStore`,
attempt state, and task-producing transitions. Its
[`operation_statemachine.go`](./chasm/lib/nexusoperation/operation_statemachine.go) expresses the
semantic core clearly through `chasm.NewTransition`, but each callback currently both mutates the
component and invokes `MutableContext.AddTask`, time, backoff, metrics, or child access.

Start with one coherent slice rather than the entire component:

- scheduled attempt fails retryably;
- operation enters backing off and records the failed attempt;
- a backoff task carries the expected attempt and scheduled time;
- firing the matching backoff task reschedules exactly once and increments the attempt;
- stale/duplicate backoff tasks do not cause an extra attempt;
- schedule-to-close timeout can settle the operation during backoff.

Author a Dafny reducer over a small `OperationState`, `AttemptFailed`, `BackoffFired`, and `Decision`
datatype. Prove state/attempt monotonicity, deadline preservation, terminal absorption, and task
uniqueness. Compile it to a test-only Go package first. A hand-written adapter can apply its
decision to the existing protobuf and context.

Use [`chasm.MockMutableContext`](./chasm/context_mock.go) to compare the logical tasks and final
state against the existing Go transition. Then run the same semantic trace through
[`chasmtest.Engine`](./chasm/chasmtest/test_engine.go), which closes transactions and records
physical tasks, and finally through the real Umpire regression harness. Test at least:

- every valid source state and each invalid source state;
- success and every error branch before/after a mutation or task decision;
- zero, minimum, and large retry delays and attempt values;
- timeout racing with backoff firing;
- duplicate and stale task delivery;
- close/reload between every pair of semantic actions;
- embedded versus standalone hosting where their capabilities differ;
- mutation tests that deliberately omit the task, fail to increment the attempt, or permit a
  terminal-state retry, demonstrating that the oracle actually fails.

Only after differential tests, Umpire conformance, generated-code review, benchmarks, and a clean
`dafny audit` should the generated reducer become production code. If the generated Go API/runtime
is too intrusive, keep the Dafny implementation as an executable reference oracle and retain the
idiomatic Go reducer.

### Other feature-level opportunities

- Callback and Activity transition reducers have similarly bounded state/task semantics.
- Scheduler calculation and buffer/backfill decisions may contain pure algorithms worth proving,
  but the surrounding time and task orchestration should remain Go.
- Cross-component Nexus/Activity and Callback/Workflow relations are better handled in the Umpire
  model family and verified as composition/refinement properties than generated as feature code.
- New CHASM features can begin with the semantic reducer and Umpire action/property declaration,
  then add the Go realizer. That is a practical meaning of “spec comes first” without forcing the
  whole component into a foreign runtime.

## Proposed Umpire integration

### 1. Treat formal languages as backends with declared strength

Extend the backend contract with capabilities rather than a common lowest denominator:

- finite exhaustive exploration;
- bounded schedule exploration;
- inductive invariant proof;
- theorem proof;
- executable oracle generation;
- test-vector generation;
- production code generation;
- native trace export and source mapping.

Map outcomes into the existing statuses (`bounded-no-counterexample`, `finite-exhaustive`,
`invariant-proved`, `counterexample`, `unsupported`, `inconclusive`). Never report “proved” for a
bounded model check or “verified Go” when the only verified artifact is a reference model.

### 2. Make counterexamples executable against Go

Every backend adapter should translate its native trace to `verify.TraceEvidence`, replay it through
`NormalizeCounterexample`, and then compile the normalized semantic steps into an Umpire completed
plan. Execution should:

1. bind abstract identities to observed CHASM execution/component identities;
2. realize each semantic action through the environment's capabilities;
3. collect facts independently of the action return values;
4. compare the committed observed state delta with the formal delta;
5. retain the formal trace, completed plan, bindings, environment profile, omissions, and verdict;
6. minimize and replay a mismatch before proposing a sparse regression.

The same path can use satisfying/non-violating traces as coverage scenarios. Counterexamples are
particularly valuable, but they are not the only generated tests.

### 3. Add Dafny in two stages

**Stage A: verify Umpire's oracle.** Generate a Dafny form of `verify.Model` and a pure interpreter
for `Initial`, `Enabled`, `Step`, and `CheckState`. Compile it to a test-only Go package and compare
all smoke-bound states/transitions/properties with the existing Go interpreter. This is low-risk,
exercises the full toolchain, and tests whether Dafny's generated Go is maintainable here.

**Stage B: verify a feature reducer.** Use the Nexus retry/backoff slice above. The artifact manifest
must record:

- Dafny source and tool version/hash;
- options, generated Go hash, and runtime version;
- proved contracts and audited assumptions;
- extern declarations and which tests exercise each one;
- mapping from Dafny functions to Go package/symbols and Umpire actions;
- benchmarks and differential-test corpus.

Check generated Go into the repository, just like other generated source, so ordinary Go builds do
not require Dafny. The Go project recommends checking generated source into a package when clients
should not need the generator ([Go generate guidance](https://go.dev/blog/generate)).

### 4. Add Lean through Veil, not through cgo

Generate a Veil module from selected `ModelFamily` targets, initially for safety only. Emit action
and property names identical to Umpire's vocabulary so trace normalization is mechanical. Use raw
Lean later for generic semantic theorems that Veil cannot express.

Run Lean/Veil offline in CI. Do not link Lean into Temporal. If an executable Lean reference model
is useful, invoke it as a hermetic build/test tool that exchanges versioned JSON and produces test
vectors; do not put its C ABI on a production request path.

### 5. Keep Agda optional

An Agda spike should have a sharply bounded success criterion: for example, define typed state
deltas and prove normalization preserves endpoint typing, then emit the same golden vectors as the
Go interpreter. Unless it reveals a unique advantage over Lean or Dafny, do not add Agda to the
required toolchain.

## Incremental roadmap and gates

### Phase 0: define claims and trust boundaries

- Document which source is canonical for each artifact: Temporal protocol declaration, generated
  formal projection, hand-authored strengthening invariant, or Dafny reducer.
- Extend manifests with assumptions, backend strength, generator/compiler/runtime hashes, and
  source maps.
- Pin all external tools and verify downloaded artifacts. Run proof/model tools outside production
  credentials and with bounded CPU, memory, time, and output.
- Add model and implementation mutation cases before accepting any new backend as evidence.

Exit gate: a reader can tell exactly what each green result claims and what remains trusted.

### Phase 1: CHASM trace conformance

- Define the abstract transition/transaction/task fact vocabulary.
- Add committed-versus-attempted outcome and transaction correlation.
- Model ref consistency and a minimal close/reload/task slice in `ModelFamily`.
- Generate P/TLA+ schedules, replay them in `chasmtest.Engine`, and normalize the observations.

Exit gate: at least one deliberately broken CHASM behavior is found by a generated schedule and
reproduced as a stable Umpire sparse regression.

### Phase 2: Dafny test-only oracle

- Implement the portable IR interpreter subset in Dafny.
- Compile it to Go and compare it exhaustively with the canonical interpreter at smoke bounds.
- Measure generated package size, build time, execution time, allocations, API quality, and audit
  assumptions.

Exit gate: zero semantic mismatches on generated models and mutations, with acceptable CI and
maintenance cost.

### Phase 3: Nexus pure reducer

- Extract and prove the retry/backoff transition slice.
- Differential-test it against the current `chasm.NewTransition` implementation with
  `MockMutableContext`.
- Execute formal schedules through `chasmtest.Engine` and a real cluster.
- Benchmark generated and idiomatic versions.

Exit gate: the team can make an evidence-based decision among generated production Go, generated
test oracle only, or abandoning the experiment.

### Phase 4: Lean/Veil proof backend

- Project one foundation target (ownership fencing or delivery safety) to Veil.
- Prove an invariant beyond the current finite bounds.
- Normalize a Veil counterexample and retain a kernel-checkable proof artifact/result manifest.

Exit gate: Veil establishes a materially stronger claim than the existing Ivy/TLA+/P backends at a
justified proof-maintenance cost.

### Phase 5: expand selectively

Expand only where a module has a stable, pure semantic interface and an important property that
current testing misses. Do not set a goal of converting files or lines of Go.

## Failure modes and trade-offs

### Correctness and trust

- **Wrong specification:** a proof can perfectly establish the wrong property. Require human review
  of property statements, negative examples, and mutation tests.
- **Abstraction gap:** a verified model can omit the race or data that causes the real bug. Make
  refinement obligations and omissions explicit and replay formal traces against Go.
- **Shared generator bug:** all generated backends can share one incorrect lowering. Cross-check
  generated transitions with the canonical interpreter, retain independent backend traces, and
  hand-review a small semantic golden corpus.
- **Foreign-code assumptions:** Dafny externs and Agda/Lean FFI boundaries are trusted unless tested
  or separately verified. Audit and record every boundary.
- **Compiler/runtime trust:** Dafny verification does not remove trust in its Go compiler/runtime;
  Lean/Agda compilation likewise adds a compiler/runtime TCB. Differential tests and generated-code
  review remain necessary.
- **Observation ambiguity:** attempted, applied, committed, persisted, and observed are distinct.
  Never advance the implementation model from a destination label without an outcome/commit fact.

### Performance and scalability

- Model state grows combinatorially with identities, tree nodes, tasks, and schedules. Use model
  family decomposition, symmetry, small-model bounds, partial-order reduction where supported, and
  separate smoke/nightly profiles.
- A 10x production load should not cause a 10x formal check on request paths: proof/model checking is
  offline, and live monitoring is bounded/sampled under the environment profile.
- A Dafny-generated pure reducer is O(1) with respect to cluster size if its input is one component
  decision. Passing whole trees, unbounded collections, or protobuf graphs through generated
  representations will create unacceptable allocation/copying costs.
- cgo/native proof runtimes complicate scheduling, cross-compilation, race detection, and memory
  ownership. Avoid them in production.
- Formal liveness depends on fairness assumptions. A bounded quiescent-progress result is not a
  proof that overloaded Temporal eventually makes progress.

### Complexity and maintainability

- Every required language adds a compiler, editor/tooling, upgrade path, expertise burden, and
  failure mode. Start with Dafny plus an optional Lean/Veil backend; keep Agda experimental.
- Generated code can be hard to review and debug. Keep generated packages isolated behind a small
  hand-written Go facade and retain source maps/replay inputs.
- Proof automation can be brittle across solver/tool upgrades. Pin versions, use deterministic
  options, record timeouts as inconclusive, and measure proof complexity before expansion.
- A rich universal IR will become another programming language. Prefer layered, capability-checked
  semantic modules over adding every backend's feature to `verify.Model`.

### Security and operations

- Formal traces and solver inputs must exclude payloads, headers, credentials, user metadata, and
  raw failures. Retain bounded semantic identifiers and hashes.
- Treat model checkers, solvers, generated C#/C/C++, and proof metaprograms as build-time code with
  supply-chain and resource-exhaustion risk. Pin, checksum, sandbox, and cap them.
- Generated production code must pass the same Go review, fuzzing, race, lint, vulnerability, and
  compatibility processes as hand-written code.
- A verified reducer says nothing by itself about authorization, protobuf compatibility, persistence
  schema, metrics, rate limits, or side-channel behavior. Keep specialized tests for contracts
  below Umpire's abstraction.

## Decision

The practical target is not “Temporal written in a proof assistant.” It is:

> Umpire owns a versioned behavioral contract; multiple engines challenge that contract; formal
> traces become tests of the real Go system; independently observed Go traces are checked back
> against the contract; and a few deterministic, high-value kernels may be generated from verified
> Dafny implementations.

For CHASM itself, use P/TLA+/Ivy/Veil to model and test the tree, transaction, reference, task, and
recovery contract. For features on CHASM, begin with the model and a pure reducer; use Dafny only
where the reducer is a stable deep module. Lean can strengthen proofs of the semantic framework,
and Agda can remain a research reference. This preserves idiomatic Go where Temporal needs it while
making “spec comes first” operational rather than aspirational.
