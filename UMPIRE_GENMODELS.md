# Umpire — Generated Verification Models

> **Status: implemented experiment.** Umpire's Go protocol remains the source of truth. Ivy, P,
> and TLA+ models are deterministic generated artifacts, not independently authored
> specifications. The checked-in profile has one identity per entity type.

## Thesis

Umpire already contains most of a backend-neutral protocol model: entity lifecycles, semantic
actions, typed relations, sparse-regression predicates and effects, progress traits, and observed
refinements. The right integration is a compiler pipeline:

```text
Umpire's canonical Go protocol
          |
          v
validated immutable verification snapshot
          |
          +-------------------+-------------------+
          |                   |                   |
          v                   v                   v
       TLA+                 P                   Ivy
     TLC/Apalache       Checker/PEx          ivy_check
          |                   |                   |
          +-------------------+-------------------+
                              |
                              v
              normalized Umpire counterexample
```

The snapshot is the deep module in this design. It gives all exporters and the Go interpreter one
small, testable interface while hiding how today's protocol is assembled from lifecycles, action
catalogs, regression capabilities, relations, and rules.

The implementation starts with **TLA+ and TLC** because its global-state, initial-predicate, and
next-state relation model is the closest match for Umpire's current atomic relational actions. P
uses one generated world machine so its actor runtime does not accidentally add message semantics.
Ivy consumes the same quantified properties plus source-declared proof-strengthening invariants.

## Implemented experiment

The repository now implements this pipeline for the compiled default Umpire v2 protocol:

- the neutral snapshot contains 7 entity types, 6 typed relations, 62 atomic actions, 10
  properties, and one explicit refinement for every generated action;
- every declared lifecycle edge is present; 32 actions without live realizers are retained as
  abstract environment actions instead of being dropped;
- `nexus.start_activity` is the first richer sparse-regression action lowered into the kernel. Its
  declared variables, binding modes, preconditions, and effects are checked against the regression
  catalog before generation;
- all other regression actions, facts, resources, policies, rules, and causal footprints are
  inventoried as included, abstracted, or excluded with a reason in `manifest.json`;
- the Nexus/activity reciprocal-link safety rule is one declarative expression used by the live Go
  rule, the reference interpreter, TLA+, P, and Ivy;
- a source-declared Nexus/activity terminal-state invariant strengthens the collective inductive
  invariant used by Ivy and Apalache;
- TLC, Apalache, P, PEx, and Ivy runners normalize their distinct outcomes into the common result
  vocabulary below and retain native evidence.

This is intentionally an atomic protocol model. It does not model Temporal RPC transport, actor
mailboxes, observation ordering, or live realizers. Those remain concrete refinement concerns.

Current limits are explicit rather than implicit:

- checked generation uses one identity per entity type; nightly increases depth and schedule
  budgets but does not silently enlarge identity pools;
- P and PEx return `bounded-no-counterexample` because schedule and step limits remain part of the
  result. PEx depth is capped at 100 in both profiles because PEx 3.1 limits repeated choices at a
  source statement; nightly still increases its schedule budget to 10,000;
- Apalache uses depth 5 for smoke and 20 for nightly bounded checking;
- the seven lifecycle `MustProgress` properties are checked as quiescent safety by TLC, Apalache,
  P, and PEx, but Ivy reports them as unsupported because this Ivy backend proves safety only;
- there are no source-declared fairness assumptions, so no result claims unbounded liveness;
- actorized P, PVerifier, broad regression lowering, and live sparse-regression replay are deferred.

## Non-negotiable source-of-truth rule

“Derived from the model as-is” means:

- protocol semantics are authored once, in Go;
- generated `.tla`, `.p`, and `.ivy` files contain no hand-maintained protocol logic;
- regenerating with the same source, options, and tool versions is deterministic;
- an exporter fails on unsupported semantics instead of silently weakening them;
- generated artifacts carry the source names needed to translate a counterexample back to an
  Umpire action and entity binding;
- backend-specific proof hints may be declared in Go, but never edited into generated files.

A modest declarative extension to the Go IR is compatible with this rule. It is necessary because
Go function values such as `RelationDeriver`, `Entity.OnFact`, `Realizer`, and rule `Check*` methods
cannot be translated safely by reflection or source-code analysis.

## Source inputs

The canonical Umpire v2 model is currently assembled from `tests/umpire2/protocol.Protocol` and
the Monitor's registered rules. Together they expose several related semantic views:

| Existing source | Semantics used by generation | Boundary |
|---|---|---|
| `Lifecycle` | initial state, states, legal edges, terminal states, capabilities, hosting, `MustProgress`, disposition | the adapter instantiates the compiled entities; `MustProgress` is translated only as quiescent progress |
| edge `Action` catalog | names, entity references, preconditions, lifecycle effects, hosting, explicit action gaps | realizers remain opaque; gaps become marked environment actions |
| `RelationSchema` | typed endpoints and one/many cardinality in each direction | fact-driven relation mutation remains outside the kernel unless an action declares it |
| sparse `regress.Domain` | complete machine-readable inventory plus the selected `nexus.start_activity` multi-entity action | other regression actions await explicit refinement mappings |
| safety/liveness rules | shared declarative Nexus/activity safety plus inventory of the remaining rules | most existing rules remain imperative Go and are not claimed as formal properties |
| causal footprints | inventory, provenance, and live-refinement role | they describe concrete refinement, not the abstract transition relation |

The lifecycle/action catalog and sparse-regression domain overlap but are not identical. For
example, the regression model has richer multi-entity effects and some state vocabularies that
project several lifecycle outcomes into one regression outcome. A generator must not union these
views by name and hope they agree.

The verification snapshot therefore needs two layers:

1. an **abstract protocol kernel** of typed predicates, relations, actions, choices, and properties;
2. explicit **refinement declarations** connecting lifecycle events, regression actions, observed
   facts, and causal footprints to that kernel.

Initially, the sparse-regression catalog is the best seed for cross-entity abstract actions, while
lifecycles provide authoritative entity-state structure and derived structural properties. The
snapshot compiler must reject an overlap that has no declared mapping or whose mapped effects
disagree. This turns today's overlap into a consistency check instead of a second source of truth.

## Alternatives considered

### 1. One verification snapshot, then three exporters — recommended

`Protocol.Compile` produces or exposes a normalized, immutable snapshot. A pure Go interpreter and
all exporters consume only that snapshot.

Advantages:

- one semantic lowering and validation path;
- exporter-equivalence tests can compare all backends with the Go interpreter;
- source provenance and unsupported constructs are recorded once;
- backend work stays isolated from Temporal-specific facts and realizers.

Cost: the snapshot and its expression/property algebra are new code, and existing opaque rules need
incremental migration.

### 2. Generate each backend directly from `Protocol`

This is initially smaller, but each exporter would independently interpret lifecycles, action gaps,
regression predicates, stuttering, and progress. The three interpretations would drift, and there
would be no backend-neutral object against which to test them.

### 3. Replace the Go declaration with an external DSL

An external language could generate both Go and formal models, but it would move the source of
truth, force a broad migration, and duplicate many Go-native type and realization seams. It is not
required to explore formal backends and conflicts with the “as-is” constraint.

## Verification snapshot

The snapshot should be immutable, serializable, stable-ordered, and free of function values:

```text
VerificationModel
├── types
│   ├── finite entity identity sorts
│   └── finite or explicitly bounded value sorts
├── variables
│   ├── entity existence and lifecycle state
│   ├── scalar protocol state
│   └── typed relation tuples
├── initial predicate
├── actions
│   ├── typed parameters and binding modes
│   ├── guards
│   ├── simultaneous state/relation effects
│   ├── explicit nondeterministic choices
│   └── semantic action classification
├── properties
│   ├── derived type/cardinality invariants
│   ├── declared safety invariants
│   └── progress obligations with explicit fairness
├── refinements
│   ├── lifecycle/regression projection
│   └── abstract action/observation footprint
└── provenance and capability diagnostics
```

### Minimal declarative additions

The existing Go model needs, within reason:

1. **A property expression algebra.** Start with equality, membership, entity state, relation
   membership, Boolean composition, implication, and bounded quantification over declared types.
   Imperative rules remain runtime-only until expressed in this algebra.
2. **Relation effects.** An abstract action can add or remove typed tuples, not only fire lifecycle
   events.
3. **Explicit choice.** Allowed outcomes are a finite declared disjunction rather than hidden in a
   realizer or callback.
4. **Refinement mappings.** Declare how regression predicates/actions project to lifecycle state
   and how concrete observations implement or stutter an abstract action.
5. **Progress and fairness.** Keep `MustProgress` as bounded/quiescent progress by default. Promote
   it to unbounded liveness only when the source declares the fairness assumption that makes the
   claim meaningful.
6. **Proof hints.** Optional strengthening invariants or backend capability annotations live beside
   the source property. They must not change the abstract behaviors.

This is deliberately not a general-purpose theorem-prover AST. New constructs should be admitted
only when every selected backend can preserve them or the capability checker can reject the
unsupported backend explicitly.

### Opaque data that stays outside the kernel

The following remain execution or observation concerns:

- `Realizer` implementations and live environment handles;
- RPC names, HTTP paths, polling, worker setup, and test hooks;
- fact decoder functions and raw protobuf payloads;
- wall-clock timestamps that do not affect allowed behavior;
- learned call footprints and fault-injection mechanics;
- secret or production trace values.

Their stable names and refinement role may appear in the manifest, but they do not become abstract
state merely because they are observable.

## Common lowering semantics

Every exporter must implement the same rules.

### State

For each entity type with a configured finite identity pool, generate:

```text
exists[entity]
state[entity]
```

An unused identity is not a protocol entity and has no meaningful lifecycle state. Fresh creation
chooses an unused identity, marks it present, sets its initial state, and applies all other action
effects atomically. Pool exhaustion is a declared bound result, not an accidental deadlock.

Each relation is a set of typed `(source, target)` tuples. Its schema generates endpoint-existence,
typing, and one/many cardinality invariants. `LinkedFrom` resolves through an explicit successor
relation and requires the declared uniqueness that makes the binding unambiguous.

### Actions

An abstract action contains parameters, guards, and simultaneous effects:

```text
enabled(action, bindings, state)
next = step(action, bindings, state)
```

- `Requires` and regression preconditions become guards.
- lifecycle effects become state updates.
- regression predicate effects become state, scalar, or relation updates according to their
  declared predicate kind and exclusivity key.
- every state component not changed by an action is framed as unchanged.
- only source-declared choices produce nondeterministic successors.
- failure, retry, timeout, drop, and duplicate behaviors are ordinary actions only when explicitly
  present in the source kernel.

Action `Kind`, hosting, and environment capabilities constrain which actions a verification profile
includes. They do not change an action's abstract effect.

### Legal lifecycle gaps

An `ActionGap` means a legal semantic transition has no atomic live realizer. Exporters include it
as an abstract environment action derived from the lifecycle edge, mark it `unrealized` in the
manifest, and preserve its capability/hosting restrictions. They must not omit the legal transition
because the live Driver cannot currently force it.

If the edge's full cross-entity effect cannot be derived, generation fails until the source adds a
declarative effect or a deliberate abstraction. A single-entity lifecycle update must not be
presented as the complete effect when the source says more state changes atomically.

### Observation tolerance and stuttering

`Lifecycle.Classify` accepts duplicate, late, and out-of-order observations as `NoOp`. That is a
runtime conformance policy, not permission for the protocol to execute arbitrary duplicate actions.
The formal behavior may include a stuttering step where required by a backend, but no new semantic
transition is inferred from `NoOp`.

### Progress

Current `MustProgress` means that an entity may not remain pending at the end of the observed run.
The faithful initial translation is a bounded or quiescent-state obligation. It is close to P's
rule that a liveness monitor should not finish in a hot state. It is not, by itself, enough to emit
an unbounded TLA+ leads-to property or fairness condition. TLA+ distinguishes the next-state
relation—what may happen—from fairness—what eventually must happen. [Lamport's overview explains
that distinction](https://lamport.azurewebsites.net/tla/high-level-view.html).

## Backend fit

| Umpire concept | TLA+ | P | Ivy |
|---|---|---|---|
| bounded entity IDs | finite constant set | finite generated ID values | finite iterable type for checking, uninterpreted type for proof |
| entity state | function `Entity -> State` | map owned by world machine | mutable function |
| typed relation | set of tuples | set/map of tuples | relation |
| initialization | `Init` predicate | world-machine start state | `after init` |
| atomic action | action predicate | one event handler | `action` block |
| precondition | conjunct in action | assertion/guard before update | `assume` as a transition guard; `require` only at a declared proof boundary |
| unchanged state | `UNCHANGED` | handler leaves field untouched | assignment semantics/frame generated by Ivy |
| choice | action disjunction/existential binding | `choose` | nondeterministic value plus guard |
| safety | `INVARIANT` | assertion/spec monitor | invariant/guarantee |
| progress | temporal `PROPERTY` plus declared fairness | quiescent assertion; hot/cold only for declared liveness | only where supported by declared proof strategy |
| refinement | refinement mapping/trace projection | separate explicit actorized model | specification/implementation and isolates |

### TLA+ with TLC — first backend

TLA+ models behavior as a sequence of global states using an initial condition and next-state
relation, which closely matches the Umpire kernel. [The official high-level
description](https://lamport.azurewebsites.net/tla/high-level-view.html) presents exactly this
state-machine shape.

Generate:

- `CONSTANTS` for finite identity and value domains;
- `VARIABLES` for existence, entity state, scalar values, and relations;
- `Init` for all legal initial worlds;
- one parameterized predicate per Umpire action;
- `Next` as the disjunction of generated actions;
- `TypeOK` plus derived relation/cardinality invariants;
- declared safety properties as invariants;
- declared temporal properties separately, with their fairness assumptions visible;
- one `.cfg` per verification profile so bounds are reviewable.

TLC can check deadlock, state invariants, and temporal properties over a configured finite model;
the official Toolbox documentation distinguishes these result kinds and the `Init`/`Next` behavior
specification. [TLC model overview](https://tla.msr-inria.inria.fr/tlatoolbox/doc/model/overview-page.html)

The runner executes the equivalent of:

```sh
java -cp tools/tla2tools.jar tla2sany.SANY Umpire.tla
java -cp tools/tla2tools.jar tlc2.TLC -workers 1 -config Umpire-smoke.cfg Umpire.tla
```

The TLA+ tools repository documents `tla2sany.SANY`, `tlc2.TLC`, and the `tla2tools.jar` entrypoint.
[TLA+ tools CLI](https://github.com/tlaplus/tlaplus)

#### Apalache over the generated TLA+

Apalache can add symbolic bounded checking and inductive-invariant checks without creating a second
TLA+ source. The generator should stay in the supported TLC/Apalache subset for profiles that target
both tools. Apalache supports finite sets and `UNCHANGED`, but not `ENABLED`, weak fairness, or
strong fairness directly. [Supported feature matrix](https://apalache-mc.org/docs/apalache/features.html)

```sh
apalache-mc check --config=Umpire-smoke.cfg --length=5 \
  --inv=Safety,QuiescentSafety --no-deadlock Umpire.tla
```

A clean bounded run means only “no counterexample through 20 steps.” Apalache's documentation calls
bounded checking incomplete and separately describes the initialization, preservation, and
property queries needed for an inductive invariant. [Apalache checking
modes](https://apalache-mc.org/docs/apalache/running.html)

Nightly `all` also runs three separate symbolic proof obligations against the same file:

```text
Init => InductiveInvariant
InductiveInvariant /\ Next => InductiveInvariant'
InductiveInvariant => DeclaredSafety
```

All three must complete with `NoError` before the normalized status is `invariant-proved`.

### P language (P/Plang) — second backend

P is built around communicating state machines. Each machine has local state and an unbounded FIFO
event buffer, and sends are asynchronous. [P state-machine
semantics](https://p-org.github.io/P/manual/statemachines/) This is valuable for a later refinement
model, but it is not Umpire's current atomic global transition semantics.

The faithful first lowering is therefore:

- one generated `Protocol` or `World` machine owns all abstract state;
- one generated event exists per Umpire action;
- each handler checks the guard and applies the action atomically;
- a finite generated driver chooses enabled action/binding combinations;
- safety properties become assertions;
- bounded/quiescent progress is asserted when no protocol action is enabled.

The generated world also increments a checker-only step counter. PEx otherwise interprets a legal
retry cycle as its own implicit liveness failure before reaching Umpire's explicit bound. The
counter changes no protocol guard or effect; it only keeps bounded checker states distinct. A
native PEx cycle or per-statement choice-cap report is normalized as `inconclusive`, never as a
failed Umpire property.

P spec machines observe events, and hot/cold states express liveness obligations whose hot state
must be left. [P monitor documentation](https://p-org.github.io/P/manual/monitors/)

```sh
p compile --pproj Umpire.pproj --mode bugfinding
p check <generated-assembly> --mode bugfinding --testcase tcUmpire --schedules 100
```

The standard checker systematically explores the requested number of schedules and writes coverage
and failing traces; that is bug finding under a budget, not exhaustive proof. [P compiler and
checker](https://p-org.github.io/P/getstarted/usingP/)

For small finite instances, also generate a PEx-compatible project:

```sh
p compile --pproj Umpire.pproj --mode pex
p check <generated-jar> --mode pex --testcase tcUmpire --schedules 100
```

PEx describes itself as exhaustive checking for small finite processes and inputs, but step,
timeout, and resource limits still have to appear in Umpire's normalized result. [PEx
documentation](https://p-org.github.io/P/advanced/pex/)

An entity-per-machine P model is a later, explicitly selected **execution refinement**. It adds
mailboxes, FIFO order, buffering, delivery scheduling, and new interleavings. Loss, duplication,
delay, and reordering enter that model only through declared execution policies. The actorized model
must never be presented as a syntax-only translation of the atomic kernel.

PVerifier is also a later proof option. It requires protocol-specific specifications, invariants,
contracts, and often lemmas; these cannot generally be synthesized from transition effects alone.
[PVerifier usage](https://p-org.github.io/P/advanced/PVerifierLanguageExtensions/using-pverifier/)

### Ivy — third backend

Ivy is the closest fit for Umpire's typed relational view:

- entity identity becomes an Ivy type;
- lifecycle state becomes a mutable function;
- Umpire relations become Ivy relations;
- guards and effects become action bodies;
- initialization uses `after init`;
- safety and cardinality properties become quantified invariants;
- nondeterminism uses an unconstrained value narrowed by a guard;
- isolates are introduced only when the source declares a sound component boundary.

Ivy's language provides actions, guarded nondeterministic choice, initializers, invariants, and
specification/implementation isolates. [Ivy language reference](https://kenmcmil.github.io/ivy/language.html)

```sh
ivy_check Umpire.ivy
```

`ivy_check` checks invariants, assertions, and non-interference; the full program must be checked
without selecting one `isolate`, or only that isolate's guarantees are covered. [Ivy command
reference](https://kenmcmil.github.io/ivy/commands.html)

Ivy proves inductive invariants rather than enumerating a configured finite state graph. Many
useful properties need strengthening invariants. The generator may derive structural invariants,
but protocol-specific strengthening facts must be declared in the Go model. The capability checker
should reject output that falls outside the intended decidable fragment instead of producing an
unreliable proof job; Ivy documents EPR and stratified-function restrictions as its decidability
boundary. [Ivy decidability](https://kenmcmil.github.io/ivy/decidability.html)

Ivy is therefore a selected-protocol proof backend, not the first always-on verifier.

## Generated artifacts

Checked-in generated files make semantic diffs reviewable and let CI prove regeneration is clean:

```text
tests/umpire2/genmodels/
├── manifest.json
├── model.ir.json
├── tla/
│   ├── Umpire.tla
│   ├── Umpire-smoke.cfg
│   └── Umpire-nightly.cfg
├── p/
│   ├── Umpire.p
│   └── Umpire.pproj
└── ivy/
    └── Umpire.ivy
```

Native traces, logs, state databases, schedules, and normalized run results are not checked in;
they are local or CI artifacts.

`manifest.json` records:

- verification snapshot hash and generator version;
- source protocol version and source names;
- backend and pinned tool versions;
- finite bounds and identity-pool sizes;
- included actions, abstract environment actions, and properties;
- omitted execution/refinement metadata;
- explicit abstractions and unsupported semantics;
- fairness assumptions;
- symmetry or state constraints;
- requested guarantee level.

`model.ir.json` is a diagnostic serialization of the validated snapshot. Exporters must consume the
in-memory typed snapshot, not reparse this file. Keeping the diagnostic file checked in makes the
semantic change visible even when generated backend syntax is noisy.

## Generator and runner shape

Implemented package boundaries:

```text
common/testing/umpire/verify/
├── model.go          backend-neutral snapshot and expression algebra
├── validate.go       type, frame, completeness, capability, and refinement checks
├── interpreter.go    pure enabled/step/explore/replay implementation
├── result.go         normalized guarantees and counterexamples
├── runner/           shell-free foreign-tool execution and result normalization
├── tla/              TLA+ and configuration emitter
├── p/                P world-machine emitter
└── ivy/              Ivy emitter

tests/umpire2/protocol/
└── verification.go   Temporal protocol -> neutral snapshot adapter

cmd/umpire-genmodels/
└── main.go           deterministic generation and pinned runner entrypoint
```

The important interfaces are small:

```text
protocol.VerificationModel(options) -> verify.Model
verify.Validate(model) -> error
verify.Interpreter.Enabled(state) -> action bindings
verify.Interpreter.Step(state, action, bindings) -> successor states
verify.Interpreter.Explore(depth) -> reachable graph and violations
backend.Generate(model) -> files
runner.Check(context, request) -> verify.Result
```

The validator runs before every exporter. It rejects at least:

- unknown types, states, predicates, relations, or action references;
- unbounded values in a bounded profile;
- fresh creation without a finite identity pool;
- conflicting or incomplete lifecycle/regression refinements;
- missing frame information;
- ambiguous `LinkedFrom` bindings;
- a legal action omitted without an explicit abstraction;
- a temporal property without required fairness;
- a construct unsupported by the selected backend;
- backend identifiers that collide after escaping.

No new third-party Go dependency is required. The foreign tools are pinned external build tools.

## Running and interpreting verification

Developer commands:

```sh
make umpire-genmodels
make umpire-check-genmodels
make umpire-verify-smoke
make umpire-verify-nightly
```

`umpire-genmodels` deterministically rewrites the checked-in files. `umpire-check-genmodels`
regenerates into a temporary directory and fails on changed, missing, or unexpected artifacts.
Smoke runs SANY, TLC, bounded Apalache, P, PEx, and Ivy. Nightly increases P/PEx budgets and
Apalache depth, and adds the three-obligation Apalache inductive proof. Tool paths come from
`UMPIRE_TLA_JAR`, `UMPIRE_JAVA_TOOL`, `UMPIRE_APALACHE_TOOL`, `UMPIRE_P_TOOL`, and
`UMPIRE_IVY_TOOL`.

The checked tool versions and SHA-256 digests are in `manifest.json`: TLA+ tools 1.7.4, Apalache
0.61.0, P 3.1.0, and Ivy 1.8.26. Ivy records distinct macOS universal and Linux x86-64 wheel
hashes.

A normalized counterexample contains:

```text
backend and tool version
failed property
guarantee level and bounds
Umpire action sequence
entity/value bindings
before/after state and relation deltas when replayed by the reference interpreter
fairness and abstraction assumptions
native artifact paths
exact replay command, or commands for a multi-obligation proof
```

Native TLC traces, Apalache traces, P schedules, and Ivy proof traces remain attached. Normalization
adds a common view; it does not discard backend evidence.

### Result vocabulary

Never return a single `verified` Boolean. Use precise statuses:

| Status | Meaning |
|---|---|
| `generated` | emitted files parsed/type-checked; no behavior claim |
| `bounded-no-counterexample` | no violation within explicit depth/schedule/resource bounds |
| `finite-exhaustive` | all states of the configured finite instance were explored without violation |
| `invariant-proved` | inductive invariant proved under recorded assumptions |
| `counterexample` | a property violation was found |
| `unsupported` | source semantics cannot be preserved by this backend/profile |
| `inconclusive` | timeout, state limit, memory limit, crash, or interrupted tool run |

Hitting a timeout, depth, state, or schedule limit is never success.

## Assurance chain

Formal generation does not prove that Temporal implements the generated protocol. The complete
chain remains:

1. Umpire declares abstract behaviors and properties.
2. The verification snapshot validates and normalizes those declarations.
3. Backends check the abstract behaviors.
4. Umpire's live Driver realizes abstract actions.
5. The Monitor checks observed lifecycle effects and rules.
6. Action and causal-footprint reconciliation check that concrete observations refine the abstract
   action.

The formal backends strengthen steps 1–3. They do not replace live `Reconcile`, footprint
reconciliation, or differential validation against the server.

## Validating the generators

The Go interpreter is the semantic oracle for exporter tests, not another authoring surface.

For tiny bounded worlds:

1. enumerate valid initial states;
2. enumerate enabled action/binding pairs;
3. compute successor states in the pure Go interpreter;
4. obtain or replay equivalent one-step backend transitions;
5. compare normalized state and relation deltas;
6. repeat over reachable states to the configured depth.

Add mutation tests for the dangerous exporter mistakes:

- remove an action guard;
- omit an `UNCHANGED`/frame clause;
- reverse relation cardinality;
- omit an action or one branch of a choice;
- make fresh identity reusable;
- turn observation `NoOp` into semantic behavior;
- weaken a property or silently drop fairness;
- mis-map a regression outcome to a lifecycle state.

Each mutation must make an equivalence or golden test fail. This is the generator counterpart to
Umpire's existing reconciliation discipline.

## CI cadence

`.github/workflows/umpire-model-verification.yml` implements the following path-filtered pull
request job and a daily scheduled job. The workflow installs checksum-pinned foreign tools,
regenerates, runs the Go and seeded-counterexample tests, executes the selected profile, and uploads
normalized plus native evidence even on failure.

### Pull requests

- regenerate and require a clean diff;
- validate the snapshot and manifest;
- parse/type-check every generated backend;
- run the checked finite TLC instance and bounded P/PEx instances;
- run a small deterministic P schedule budget;
- run one-step and tiny-world equivalence tests against the Go interpreter;
- report changed bounds, assumptions, omitted semantics, and guarantee levels in logs.

### Nightly

- run the nightly schedule/depth profile;
- run Apalache at depth 20 and the selected inductive-invariant job;
- run multiple P seeds and search strategies;
- run the larger bounded PEx schedule budget;
- retain normalized and native counterexamples as CI artifacts;
- trend state counts, branching, duration, and memory so state-space growth is visible.

### Proof jobs

- run Ivy only for protocols with declared proof invariants;
- later evaluate PVerifier where source-declared contracts justify it;
- fail if a previously proved property degrades to bounded or inconclusive without an explicit
  approval in the source manifest.

The workflow pins the TLA+ jar, P NuGet package, Apalache release archive, Ivy platform wheel, and
GitHub Actions. Tool-version drift changes the manifest and generated-artifact gate deliberately.

## Testing strategy

The implemented tests cover:

- deterministic snapshot normalization, JSON, hashing, manifests, and generated files;
- validation of references, fresh bindings, cardinalities, fairness, complete action refinements,
  and conflicting regression mappings;
- interpreter atomicity, quantified properties, relation structure, freshness, complete bounded
  exploration, and abstract-trace replay;
- exact coverage: every source action and property occurs in each backend or has an explicit
  unsupported diagnostic;
- compilation/checking by the actual pinned SANY, TLC, Apalache, P, PEx, and Ivy tools when their
  environment variables are present;
- agreement between the interpreter and TLC on a tiny bounded reachable-state count;
- a seeded missing-reciprocal-link bug found and normalized to `seed.bug` by the interpreter, TLC,
  P, PEx, and Ivy;
- rejection when the selected `nexus.start_activity` regression declaration drifts from its
  source-level preconditions or effects;
- strict parsing of success, counterexample, timeout, limit, tool-error, and parse-failure results.

Remaining deeper assurance work is transition-by-transition graph comparison, a broader mutation
suite for frame/guard/choice mistakes, larger identity pools, and compiling a normalized abstract
trace into a live sparse regression. Those are useful follow-ons; they are not claimed by the
current experiment.

## Trade-offs

### Performance

One normalization pass and checked-in artifacts add generation cost, but avoid three independent
semantic traversals. Tiny-world equivalence tests are cheap; foreign verification dominates runtime.
PR bounds must remain intentionally small, with larger exploration moved to nightly jobs.

### Scalability

State space grows with the product of entity bounds, lifecycle states, relation combinations,
choices, and interleavings. A 10× entity increase can produce exponentially more states, not 10×
work. Control it with explicit profiles, symmetry only when proven sound, independence/commutativity
metadata, state constraints, and separate protocol slices. Never hide truncation.

TLC symmetry is useful only for genuinely interchangeable identities and properties. It must be a
source-declared, validated optimization rather than inferred from similar names.

### Complexity

The snapshot, validator, and interpreter are additional machinery. They earn their cost by forming
one deep module that isolates every backend and makes semantic drift testable. A direct exporter is
smaller only until the second backend repeats its lowering logic.

### Security

Models contain symbolic identities and bounded values, never live request payloads, tokens, or raw
traces. Diagnostics use stable source names and normalized digests. Tool runners use fixed argument
vectors, isolated output directories, pinned binaries/images, resource limits, and no shell
interpolation of model values.

## Failure modes

### The source model is incomplete

Formal tools can prove an incomplete model. The manifest exposes omitted/opaque semantics, while
live reconciliation and coverage show whether production behavior has a matching abstract action.

### The lifecycle and regression views disagree

Snapshot compilation fails with both source locations and the unmapped state/action/effect. It must
not choose one silently.

### A backend cannot preserve a construct

Generation returns `unsupported`, identifying the exact construct and supported alternatives. An
explicit profile may choose a documented over- or under-approximation; the manifest records it and
the result cannot claim the stronger guarantee.

### Liveness is vacuous or unfair

Only source-declared fairness permits an unbounded liveness property. Without it, the generator
emits bounded/quiescent progress and labels the weaker claim precisely.

### Bounds create false confidence

Every result includes identity sizes, value bounds, maximum depth, schedule count, state count, and
termination reason. Pool exhaustion is separately reported.

### State explosion

The runner returns `inconclusive`, preserves logs and partial native artifacts, and suggests the
largest completed profile. It does not silently reduce bounds.

### A tool crashes or changes output

The runner preserves stdout/stderr and native artifacts. Parser failure is `inconclusive` or a
runner test failure, never a clean verification result. Pinned versions and fixture-based parser
tests make output drift explicit.

### A generated counterexample cannot run live

The result identifies the missing environment capability or unrealized abstract action. It remains
a model counterexample and may motivate a new realizer; it is not discarded.

## Phased implementation

### Phase 0 — semantic inventory and alignment (implemented)

- inventory lifecycle states/actions, regression predicates/actions, relations, rules, and
  footprints from the compiled default protocol;
- declare the first lifecycle/regression refinement maps;
- choose one small Nexus slice with no unresolved semantic overlap;
- define precise result/guarantee vocabulary and finite verification profiles.

Exit: the chosen slice has one unambiguous abstract transition system and all exclusions are
machine-readable diagnostics.

### Phase 1 — verification snapshot and interpreter (implemented)

- add the neutral model, expression algebra, validation, stable serialization, and manifest;
- add relation effects, explicit finite choices, progress/fairness declarations, and refinement
  mappings to the Go source model;
- compile the Nexus slice and interpret it exhaustively in tiny worlds;
- test lifecycle/regression consistency and action completeness.

Exit: all reachable tiny-world transitions can be enumerated without a foreign tool.

### Phase 2 — TLA+ and TLC (implemented)

- generate `.tla` and `.cfg` files;
- add SANY parsing, TLC execution, result parsing, and counterexample normalization;
- check in smoke and nightly configurations;
- add Go/TLA+ bounded transition-equivalence tests.

Exit: TLC exhaustively checks a finite Nexus instance and a seeded bug normalizes to Umpire action
names.

### Phase 3 — Apalache (implemented)

- constrain generated TLA+ to the supported shared subset where requested;
- add typechecking and bounded depth profiles;
- add optional source-declared inductive invariants and the three proof-obligation checks;
- preserve Apalache traces in the common result shape.

Exit: the same generated TLA+ supports both explicit-state and symbolic checks without semantic
forks.

### Phase 4 — P world-machine model (implemented, bounded)

- generate the single-world-machine model, driver, monitors, and project file;
- integrate schedule-bounded P and PEx checking;
- normalize schedules and compare bounded transitions with the Go interpreter;
- keep actorized/entity-machine generation out of this phase.

Exit: P and PEx check the same atomic kernel as TLC.

The generated kernel and seeded mutation are shared. A complete transition-by-transition P graph
comparison remains follow-up work.

### Phase 5 — declarative property migration (implemented for Nexus/activity links)

- migrate high-value structural and cross-entity safety rules from imperative Go to the property
  algebra while retaining generated/runtime evaluations from the same declaration;
- add explicit progress/fairness only where the protocol guarantees it;
- compile properties to the Go Monitor as well as formal backends where feasible.

Exit: at least one real cross-entity Umpire rule is authored once and checked live, by TLC/P, and by
the Go interpreter.

### Phase 6 — Ivy (implemented for safety)

- generate typed relations, actions, invariants, and initializers;
- add source-declared strengthening invariants and capability checks for the decidable fragment;
- run full `ivy_check` without isolate selection;
- normalize failed proof traces.

Exit: Ivy proves a selected stable protocol invariant or reports an explicit unsupported/proof-gap
result without hand-edited Ivy.

### Phase 7 — optional execution refinements (deferred)

- generate entity-per-machine P models only from declared delivery/failure policies;
- use TLA+ or Ivy refinement mappings where they add value;
- connect model counterexamples to sparse-regression compilation and live replay;
- evaluate PVerifier for protocols whose proof contracts and lemmas justify its toolchain cost.

## Implemented first experiment

The checked profile is broader than the originally proposed Nexus-only slice: it includes one
identity for each of Activity, Callback, NexusOperation, TaskQueue, Workflow, WorkflowRun, and
WorkflowTask, every lifecycle edge, all six relation schemas, and the cross-entity
`nexus.start_activity` regression action. The single-identity bound keeps TLC exhaustive while still
exercising hosting, action gaps, branching outcomes, failure paths, relations, and quiescent
progress. A separate tiny reciprocal-link fixture intentionally omits one relation effect.

The success criterion is not merely that TLC reports no error. It is:

1. every included Umpire action has a generated counterpart and source provenance;
2. the Go and TLA+ reachable-state count agrees for the tiny equivalence fixture, while the full
   checked profile is independently exhaustive in both;
3. a seeded semantic mutation is found and normalized to the expected Umpire action;
4. unsupported or omitted semantics appear in the manifest;
5. regeneration is deterministic and produces a clean diff.

## Bottom line

Ivy, P, and TLA+ fit Umpire as verification backends, but not as interchangeable syntaxes:

- **TLA+/TLC** is the best first translation of the atomic global protocol kernel.
- **P** is useful first as a world-machine checker and later as an explicit communicating-machine
  refinement.
- **Ivy** is the strongest fit for selected quantified relational invariants once the source model
  includes the strengthening facts needed for proof.

The design works only if all three consume one validated verification snapshot, unsupported
semantics fail visibly, and every result states its bounds and assumptions. That preserves Umpire's
central claim: **the model is the source; planning, driving, monitoring, and formal verification are
derived views of it.**
