# Starlark as an Umpire authoring language

Date: 2026-08-15

Status: Analysis and recommendation; no Starlark frontend is implemented here.

## Executive finding

The largest opportunity is not Starlark itself. It is to make Umpire's existing
sparse-plan compiler into an explicit typed constraint planner: authors provide
semantic milestones, symbolic variables, relationships, ordering, fault scopes,
and coverage requirements; Umpire fills the gaps and produces one canonical
case, every bounded case, or enough cases to cover selected combinations.

That planner belongs in Go behind a canonical constraint IR. It can serve both a
typed Go DSL and an optional Starlark frontend. Prolog and especially Picat offer
valuable design lessons for unification, constraint propagation, tabling,
partial-plan completion, and one-versus-many answer selection, but neither
language should become Umpire's runtime or semantic authority.

Starlark still has a potentially material authoring advantage if Umpire treats
selected lambdas as quoted expressions. Native-looking expressions such as
`run_a != run_b` and `timeout_a > timeout_b + seconds(1)` can then lower to the
same invariant, guard, or plan-constraint IR instead of requiring a growing set
of helpers such as `different` and `exceeds`. That is more than replacing Go
braces, although it requires an AST-aware compiler rather than only ordinary
Starlark evaluation.

At the compiler level the Starlark layer remains "syntactic sugar," and that is
a virtue: the compiled runtime `Protocol`, constraint planner, canonical
`verify.ModelFamily`, validation, hashing, interpreter, and all existing
backends remain the semantic authorities for their respective stages.

The proposal becomes more than cosmetic if one Starlark package can:

1. declare machines, states, relations, actions, modules, interfaces, targets,
   properties, and refinement mappings, while lowering machines to Umpire's
   internal entity representation;
2. declare executable examples/scenarios and implementation-conformance tests;
3. project the same runtime protocol, sparse-regression domain, normalized
   verification IR, and existing FizzBee, Ivy, P, and TLA+ artifacts; and
4. generate the typed Go adapter and test surfaces needed to bind that abstract
   model to Temporal code.

It would be a net loss if Starlark merely replaced Go struct literals with
unvalidated dictionaries and strings. The recommendation is therefore to build
the typed constraint IR and planner in Go first, then compare a strong generic
Go DSL with an **optional, deliberately small Starlark frontend** over the same
boundary. Do not replace the IR, embed Starlark in the test runtime, or adopt
FizzBee, Prolog, or Picat wholesale.

The likely gain is concentrated in data-shaped declarations and behavioral
scenarios. Starlark will not remove the essential complexity of evidence
decoding, identity, runtime rules, Temporal API calls, worker behavior, fault
injection, environment allocation, or cleanup. Those remain Go.

## Current Umpire baseline

These are repository facts, not proposals:

- The compiled [`Protocol`](tests/umpire2/internal/protocol/protocol.go) is the
  runtime catalog for facts, entity factories, relations, action bindings and
  gaps, sparse regression, and causal footprints. Its default declaration is
  still assembled from Go factories and action implementations.
- The sparse-regression [`Domain`](common/testing/umpire/regress/catalog.go) and
  typed vocabulary let tests state partial-order intent while the compiler
  fills and validates executable paths. The repository's existing
  [developer-experience review](UMPIRE_UX.md) identifies this as the strongest
  current authoring interface.
- [`verify.Model`](common/testing/umpire/verify/model.go) is already a compact,
  backend-independent IR for bounded entities, relations, actions, effects,
  safety/quiescence/progress properties, abstractions, and refinements.
- [`verify.ModelFamily`](common/testing/umpire/verify/family.go) adds modules,
  provider/consumer interfaces, obligations, compositions, refinement maps, and
  verification targets. This is richer structural information than a simple
  state-machine DSL normally carries.
- [`verify.Validate`](common/testing/umpire/verify/validate.go) performs semantic
  checks that Go's type system does not: referenced entities/states/relations,
  binding modes, relation endpoint types, fresh creation, property shapes,
  refinement coverage, ownership, and contract closure.
- [`verify.Project`](common/testing/umpire/verify/project.go) closes dependencies
  and projects a model family into a target-specific model.
- [`umpire-genmodels`](cmd/umpire-genmodels/main.go) already normalizes and hashes
  the IR and emits FizzBee, Ivy, P, TLA+, manifests, and closure reports. For
  example, Umpire already generates
  [`Umpire.fizz`](tests/umpire2/testdata/genmodels/protocol-atomic/fizz/Umpire.fizz).
- The current FizzBee backend emits atomic actions, explicitly disables FizzBee
  liveness checking, and reports canonical progress properties as unsupported.
  ([generator](common/testing/umpire/verify/toolchain/internal/fizz/generate.go),
  [coverage test](common/testing/umpire/verify/toolchain/internal/fizz/generate_test.go))

**Inference:** Umpire does not need a new semantic core. It needs a better
authoring surface and a small common descriptor boundary. Starlark cannot lower
only to `verify.ModelFamily`: that IR intentionally omits live fact decoding,
entity factories, relation derivation, realizers, causal footprints, and the
full sparse-regression vocabulary. Instead, a frontend should produce a compact
validated descriptor package that projects into the existing runtime,
regression, and verification models without changing their semantics.

## Umpire is already a small logic planner

The Prolog/Picat comparison is not merely an analogy. Sparse regression already
contains many logic-programming mechanisms:

- normalization lowers ordered instructions, `AnyOrder`, `During`, bindings,
  requirements, labels, and precedence into a typed constraint DAG;
- action capabilities declare typed local variables, input/fresh/observed
  modes, preconditions, effects, resources, and realizations;
- `unify` matches an action effect with a requested grounded goal;
- `satisfyGoal` searches backward from an outcome or relation through action
  preconditions and then applies a satisfying action;
- `completeBindings` derives values from facts and otherwise enumerates matching
  plan symbols; and
- compilation returns the first canonical path or every bounded satisfying
  path.

That already lets an author omit mechanics between semantic key frames. The
current restrictions identify the opportunity: unification is one-way against
ground atoms, unresolved variables are completed through same-type Cartesian
enumeration, repeated recursive goals are rejected by a recursion stack rather
than tabled, total topological orders are materialized before search, and the
canonical path is selected after generating candidates.

The most useful lessons are:

- From **Prolog**: shared logical variables, substitutions, relational use of
  predicates, propagation before enumeration, tabled goal-directed evaluation,
  explicit input/output and determinism modes, and caution around negation and
  cut. ([unification](https://www.swi-prolog.org/pldoc/man?section=compare),
  [constraint logic programming](https://www.swi-prolog.org/pldoc/man?section=clp),
  [tabling](https://www.swi-prolog.org/pldoc/man?section=tabling))
- From **Picat**: a ground-state/action/cost planning model, tabled completion
  from an initial state to a goal, best-plan and all-best answer modes, and
  declarative guidance based on the current partial plan.
  ([official user guide, Chapters 7–8](https://picat-lang.org/download/picat_guide_html/picat_guide.html))

Picat is the closer operational blueprint; Prolog supplies the broader
relational vocabulary. The detailed source-backed comparison is in
[`UMPIRE_PROLOG_PICAT_RESEARCH.md`](UMPIRE_PROLOG_PICAT_RESEARCH.md).

Do not copy dynamic typing, cut, clause-order-dependent semantics, unrestricted
recursive predicates, or negation-as-failure. In particular, missing runtime
evidence is not a negative fact. Observation-backed propositions must preserve
Umpire's `unknown` or `inconclusive` outcome unless the relevant relation is
explicitly declared complete.

## Where a descriptor language fits

| Good Starlark candidates | Keep in Go |
| --- | --- |
| Machine names, lifecycle states, traits, and transitions | Fact structs and protobuf/OTEL/history decoding |
| Relation schemas and cardinalities | Entity field capture and nontrivial `OnFact` logic |
| Abstract action guards, effects, branches, capabilities, and action gaps | RPC, worker, callback, timer, and fault realizers |
| Declarative properties and refinement maps | Runtime rules that need arbitrary algorithms or external types |
| Verification modules, compositions, targets, bounds, and abstractions | Environment setup, authority, cleanup, and evidence retention |
| Sparse scenarios, partial order, expected outcomes, and named policies | Exact protobuf, performance, schema, metric, and low-level race tests |

Simple fact-to-transition mappings or relation derivations could eventually be
described, but custom capture logic must have a stable Go binding key and an
explicit handwritten implementation. The compiler must reject a missing
required binding before a test environment is allocated.

## Three viable approaches

| Approach | Benefit | Cost | Assessment |
| --- | --- | --- | --- |
| Build the constraint planner and a typed Go DSL | Retains compile-time typing, IDE navigation, refactors, and the smallest toolchain; delivers all plan-completion capability | Go cannot naturally quote relational expressions, so constraints need generic constructors or source analysis | **Required foundation and possibly sufficient** if authors are Temporal Go engineers |
| Put Go and Starlark frontends over one typed IR | Lets each audience choose its authoring surface; tests both against identical semantics | Requires frontend parity tests, source maps, and two toolchains | **Recommended experiment** after the shared IR exists |
| Make Starlark the sole authoring source and generate Go boundaries | Gives the cleanest portable descriptor package and native quoted-expression syntax | Weakens editor/refactor support and makes generation part of every model change | Adopt only if accessibility and readability gains are measured, not assumed |

A fourth option—making arbitrary Starlark functions the executable model—is not
recommended. Quoted expression lambdas are compiled once into declarative IR;
they are not retained as callbacks. Arbitrary callbacks cannot be reliably
lowered into Umpire's IR or emitted with matching semantics to FizzBee, Ivy, P,
and TLA+. They would also create a second runtime and debugging model beside Go.

## What Starlark actually provides

### Source facts

- Starlark is a Python-like, dynamically typed configuration language intended
  to be embedded in a host. The host can provide domain-specific functions and
  types. Its stated design principles include deterministic evaluation,
  hermetic execution, simplicity, Python familiarity, and tooling support.
  ([official Starlark repository](https://github.com/bazelbuild/starlark))
- Starlark still performs static name resolution and well-formedness checks
  before execution, even though value types are dynamic. The host may add
  predeclared functions and custom value types, and completed modules are frozen.
  ([language specification](https://github.com/bazelbuild/starlark/blob/master/spec.md#name-binding-and-variables),
  [module execution](https://github.com/bazelbuild/starlark/blob/master/spec.md#module-execution))
- The Go implementation supports parsing/executing a file with host-provided
  names and returns evaluation errors with backtraces. A host can also cap
  execution steps and cancel a running Starlark thread.
  ([Go embedding API](https://pkg.go.dev/go.starlark.net/starlark#ExecFileOptions),
  [execution limits](https://pkg.go.dev/go.starlark.net/starlark#Thread.SetMaxExecutionSteps))
- Function arguments are evaluated before a call, while a lambda produces a
  function value that captures its body syntax and free variables. This makes a
  lambda a natural explicit quotation boundary for symbolic expressions.
  ([function calls](https://github.com/bazelbuild/starlark/blob/master/spec.md#function-and-method-calls),
  [lambda expressions](https://github.com/bazelbuild/starlark/blob/master/spec.md#lambda-expressions))
- The Go implementation currently reserves the right to make breaking language
  and API changes before a finalized version-1 specification.
  ([starlark-go stability notice](https://github.com/google/starlark-go#stability))

### Umpire inference

The Go library would both parse and evaluate each module. Evaluation enables
loops and helper functions to remove repetition, while retaining the parsed AST
allows selected lambdas passed to `invariant`, `when`, or `where` to be compiled
into Umpire's expression IR. Umpire should accept only declarative DSL values and
compiled expressions; it should never retain a Starlark callback as runtime or
formal-model behavior.

Plain `where(run_a != run_b)` cannot work through ordinary embedding: eager
evaluation turns the comparison into a Boolean before `where` receives it.
Context-sensitive AST rewriting could intercept that syntax, but then ordinary
comparisons would behave differently depending on their call site. Prefer the
small, explicit quotation marker `where(lambda: run_a != run_b)`.

The loss is **compile-time Go typing**, not the absence of types altogether.
Because model files would run only during generation, most dynamic failures can
be moved to generation/CI time. More importantly, the frontend need not expose
raw dictionaries. Go-hosted constructors can accept checked arguments and
return symbolic Starlark values such as `Machine`, `State`, `Relation`, `Action`,
and `Property`. The existing whole-model validators then provide a second,
semantic checking phase.

A safe frontend should therefore have four layers of checks:

1. Starlark parse/name errors;
2. restricted quoted-expression compilation with source-located type checking;
3. strict DSL-constructor checks, including argument types and source positions;
4. the existing protocol, sparse-regression, and
   `verify.ValidateModelFamily` checks after projection.

This is weaker than IDE-time Go type checking, but substantially stronger than
"a bag of dictionaries." For declarative model code, the shorter authoring and
faster review loop may be worth that trade.

## What FizzBee demonstrates

### Source facts

FizzBee is an important precedent, but it is not simply a Starlark library:

- FizzBee presents actions, nondeterministic `any` choices, safety and transition
  assertions, liveness/fairness, and guard conditions in Python-like syntax.
  ([getting-started guide](https://fizzbee.io/design/tutorials/getting-started/))
- Its `exists assertion` checks that exploration can reach at least one state
  satisfying a predicate. This is a useful coverage obligation distinct from a
  universal safety property.
  ([liveness and fairness guide](https://fizzbee.io/design/tutorials/liveness/#ctl-property-exists))
- Its complete `.fizz` syntax is a custom grammar with constructs such as
  `action`, `role`, `atomic`, `serial`, `parallel`, `oneof`, `any`, `require`,
  and temporal assertions.
  ([official parser grammar](https://github.com/fizzbee-io/fizzbee/blob/main/parser/FizzParser.g4))
- The implementation parses that grammar separately, then evaluates extracted
  Python-like expressions and statements with `go.starlark.net`.
  ([official evaluator source](https://github.com/fizzbee-io/fizzbee/blob/main/modelchecker/starlark.go))
- Roles group participant state and actions, can be instantiated dynamically,
  and can be marked symmetric.
  ([roles](https://fizzbee.io/design/tutorials/roles/),
  [symmetry reduction](https://fizzbee.io/design/tutorials/symmetry_reduction/))
- Channels give calls explicit blocking, delivery, and ordering semantics.
  Non-atomic calls can model request or response loss. The channel facility is
  documented as work in progress.
  ([channels](https://fizzbee.io/design/tutorials/channels/))
- Non-atomic actions let FizzBee insert intermediate transitions and implicitly
  explore message loss, network partitions, thread/process crashes, and selected
  storage failures. Durable/ephemeral annotations control crash behavior.
  ([implicit fault injection](https://fizzbee.io/design/tutorials/fault-injection/))
- FizzBee's model-based testing workflow model-checks a `.fizz` model, generates
  Go test/interface/adapter scaffolding, and runs sequential and parallel traces
  against a user-implemented adapter. Unimplemented adapter actions are disabled
  so the suite can be adopted incrementally.
  ([MBT overview](https://fizzbee.io/testing/),
  [Go quick start](https://fizzbee.io/testing/tutorials/quick-start/))
- It derives state, sequence, and algorithm visualizations from the executable
  specification.
  ([visualizations](https://fizzbee.io/design/tutorials/visualizations/))
- The project documents current language limitations, including source-location
  errors and restrictions on where Fizz functions may be called.
  ([current limitations](https://fizzbee.io/design/tutorials/limitations/))

### Umpire inference: what is worth earning

The most valuable FizzBee lesson is not its Python surface by itself. It is the
workflow that turns one behavioral model into design verification, navigable
traces, and implementation tests.

High-value ideas to adopt:

1. **Describe behavior once, derive tests.** Treat action choices and properties
   as the source of sequential and concurrent implementation traces, rather
   than translating a list of handwritten unit tests into Starlark.
2. **Keep a refinement/adapter seam.** Generate Go interfaces and test drivers,
   but keep concrete Temporal setup, calls, and observation code in Go. FizzBee
   demonstrates this separation; Umpire can make the generated methods more
   strongly typed than FizzBee's generic `[]Arg`/`any` surface because Umpire's
   IR already knows entity parameter types and binding modes.
3. **Make distributed-systems semantics first-class authoring concepts.** Roles,
   reliable/unreliable channels, atomicity/yield boundaries, durable versus
   ephemeral state, nondeterministic branches, and named failure policies are
   much easier to review than manually expanded transitions.
4. **Expand conveniences into explicit canonical behavior.** A `channel()` or
   `fault_policy()` helper should lower to ordinary Umpire environment actions,
   branches, state, and properties. It should not acquire backend-specific or
   hidden semantics. That preserves agreement among FizzBee, Ivy, P, TLA+, and
   Umpire's interpreter.
5. **Make counterexamples the testing currency.** Preserve action names,
   parameters, branches, and source spans so a formal counterexample can be
   rendered as a readable scenario and replayed through the Go adapter.
6. **Add visualization from the IR.** State/sequence views would improve design
   review even for engineers who never run a model checker. This benefit is
   independent of Starlark, but a readable source-to-diagram link compounds the
   accessibility gain.
7. **Use symmetry deliberately.** A symbolic `symmetric_ids()` constructor could
   express interchangeable bounded identities. The optimization must be an IR
   concept with cross-backend validation, not a FizzBee-only annotation.
8. **Separate reachability from correctness.** Add an existential/reachability
   obligation to the canonical verification vocabulary so a target can prove it
   reached an intended state instead of inferring this from a passing invariant.

FizzBee's liveness, non-atomic execution, and implicit failures are examples of
semantics Umpire could earn, not benefits conferred by a Starlark frontend. The
current Umpire-to-FizzBee backend deliberately does not express them. Each would
need an explicit canonical-IR design, interpreter semantics, generator support,
and cross-backend tests before the Starlark DSL exposed it.

Ideas to defer:

- probabilistic/performance modeling, until the canonical IR has explicit
  semantics and at least two consumers;
- adopting FizzBee's custom parser or copying its implicit semantics;
- making a backend-generated `.fizz` file the Umpire source of truth.

One FizzBee behavior should not be copied: its MBT runner treats
`ErrNotImplemented` actions as disabled so partial adapters can pass while being
developed. That is convenient for scaffolding, but Umpire's assurance boundary
requires unsupported behavior to remain visible. Generated Umpire bindings
should distinguish a declared `ActionGap` from a required binding and fail
preflight when a required implementation is absent.

FizzBee's own grammar-plus-Starlark architecture and documented limitations show
that extending Python syntax has a real implementation and diagnostics cost.
Umpire can avoid most of that cost by staying within valid Starlark syntax and
offering library functions/custom host values.

## Models and tests in one language

"Tests in Starlark" should be split into three concepts:

1. **Properties** are model obligations: safety, quiescence,
   progress/fairness, cardinality, and refinement obligations. These belong in
   the canonical model and run in every capable backend.
2. **Scenarios** are named, finite action traces with expected abstract state or
   observations. They are useful as examples, smoke tests, regression traces,
   and replay targets. They can be declared in Starlark and lowered to the
   existing sparse-regression plan where it fits, with a small scenario IR only
   for information that plan cannot represent.
3. **Concrete bindings** create test servers, call Temporal APIs, inject faults,
   and read implementation state. These should remain hand-written Go behind a
   generated typed interface. Exposing all of Temporal to the Starlark host would
   erase hermeticity, create a second runtime API, and make failures harder to
   debug.

This division allows the model and tests to share names without pretending that
arbitrary Go integration setup can or should be generated.

`go test` integration does not require generating custom logic for every case.
One generated or handwritten `TestUmpireScenarios` can enumerate compiled
scenario descriptors as named subtests through the high-level Temporal runner.
Keeping scenario behavior in the normalized descriptor preserves source spans
and avoids making generated `_test.go` files another semantic representation.

## Proposed shape

The compilation boundary should be:

```text
*.umpire.star
    -> parser + restricted evaluator + quoted-expression compiler
    -> checked Umpire DSL values ------------------------+
                                                         |
typed Go DSL --------------------------------------------+
                                                         |
                                                         v
validated descriptor.Package + typed Constraint / Scenario IR
    -> runtime Protocol projection
    -> sparse-regression Domain / Plan projection
       -> native Go constraint propagation + tabled completion
          -> completed cases, proofs, residual goals, or typed failure
    -> verify.ModelFamily projection
       -> existing Validate / Project / hash pipeline
       -> existing FizzBee / Ivy / P / TLA+ generators
    -> generated typed Go binding interfaces and scenario catalog
```

The authoring syntax can remain ordinary Starlark without looking like a Bazel
rule or a Go struct literal. The best division is different for models and test
plans:

- model declarations are flat calls that accumulate checked declarations in a
  module-local package;
- test plans are nested data because sequence, partial order, repetition, and
  fault scope are part of their meaning.

For example, this is an illustration of the desired model style, not a
committed API:

```python
Nexus = machine("NexusOperation")

starts_at(Nexus.unspecified)
succeeds_at(Nexus.succeeded)
fails_at(Nexus.failed | Nexus.timed_out | Nexus.rejected)

active = Nexus.scheduled | Nexus.backing_off | Nexus.started
terminal = Nexus.succeeded | Nexus.failed | Nexus.timed_out | Nexus.rejected

transition(
    (Nexus.unspecified | Nexus.backing_off)
        >> on.schedule
        >> Nexus.scheduled,
)
transition(Nexus.scheduled >> on.start >> Nexus.started)
transition(Nexus.started >> on.complete >> Nexus.succeeded)

invariant(
    "terminal operations are not active",
    lambda op: not (
        op.state in active and
        op.state in terminal
    ),
)
eventually("active operations settle", active, terminal)
```

`machine`, `starts_at`, `transition`, `invariant`, and `eventually` are ordinary
predeclared Starlark functions. `Nexus`, its state values, state sets, and the
transition expression are Go-hosted Starlark values. The `|` and `>>` operators
use Starlark's normal expression syntax and are interpreted only for those
values. The invariant lambda is not run by a checker as Starlark code; its AST is
lowered to generic `Not`, `And`, `In`, and field-reference nodes. The author
writes `machine`, `starts_at`, and `eventually`; the compiler can still lower
them to Umpire's internal entity, initial-state, and progress representations.

The calls accumulate into the package being evaluated, but none relies on a
hidden "current machine." The owner is carried by `Nexus.unspecified` and every
other symbolic value. This permits declarations for several machines to be
interleaved, lets helper functions add related declarations, and lets the
frontend reject a transition whose states belong to different machines.

Test plans should keep their meaningful nesting:

```python
op = Nexus.ref("op")

test("cancellation survives an RPC failure", one_path(
    reaches(op, Nexus.started),

    during(
        fail_next(rpc.cancel_nexus_operation),
        cancel_with_retry(op),
    ),

    expect(cancel_request_failed(op)),
    reaches(op, Nexus.canceled),
))
```

This is declarative data despite its function-call notation: evaluation creates
an immutable plan tree and does not invoke Temporal or inject a fault. The tree
makes the scope of `fail_next` unambiguous. The same applies to partial order and
capability requirements:

```python
left = Nexus.ref("left")
right = Nexus.ref("right")
handler = Workflow.ref("handler")

test("two callers can share a handler", all_paths(
    requires(capability.workflow_callbacks),

    any_order(
        start(left, handler),
        start(right, handler),
    ),

    reaches(handler, Workflow.completed),
    reaches(left, Nexus.succeeded),
    reaches(right, Nexus.succeeded),
))
```

A fully imperative plan builder would make the common linear case slightly
flatter, but scoped faults and unordered groups would then need begin/end calls,
mutable child builders, or a hidden scope stack. Nested values preserve those
boundaries directly and can be validated or transformed before execution.

### Test references, RPCs, faults, and values

The plan vocabulary should preserve the separation that already exists in
sparse regression:

- `cancel_with_retry(op)` is a semantic action that can have more than one live
  realization;
- `rpc.cancel_nexus_operation` is a typed concrete RPC target;
- `fail_next(...)` is a fault policy, and `during(...)` scopes that policy over
  synthesized behavior;
- `requires(...)` states an environment capability without configuring the
  environment;
- `bind(...)` captures a typed value observed from model or implementation
  state.

Symbolic test references and literal values can stay ordinary assignments:

```python
op = Nexus.ref("op")
handler = Workflow.ref("handler")
run = RunID.ref("run")
timeout = seconds(2)

test("an asynchronous operation times out", one_path(
    schedule(op, start_to_close = timeout),
    respond_async(op, handler),
    bind(run, workflow.run_id(handler)),
    reaches(op, Nexus.timed_out),
))
```

### Partial plans are constraint queries

An incomplete plan should not contain imperative placeholders. Omission is the
hole: the author states the semantic milestones and relationships that matter,
and the planner synthesizes registered semantic actions between them. Explicit
symbolic choices add dimensions the solver may bind:

```python
first = Nexus.ref("first")
second = Nexus.ref("second")
run_a = RunID.some("run_a")
run_b = RunID.some("run_b")

timeout_a = choose("timeout_a", seconds(2), seconds(3), seconds(4))
timeout_b = choose("timeout_b", seconds(0), seconds(1), seconds(2))
fault = choose(
    "fault",
    no_fault,
    fail_next(rpc.start_nexus_operation),
)

test("distinct runs with separated timeouts", cover(
    all_combinations(timeout_a, timeout_b, fault),

    where(lambda: (
        run_a != run_b and
        timeout_a > timeout_b + seconds(1)
    )),

    during(fault,
        any_order(
            start(first, run_id = run_a, timeout = timeout_a),
            start(second, run_id = run_b, timeout = timeout_b),
        ),
    ),
))
```

The expression compiler lowers `!=`, `>`, `+`, and `and` into generic typed IR
nodes. It does not create helpers for every domain relationship. The same
expression IR serves model invariants, action guards, test constraints, the
native planner, and formal backend generation.

Completion policy must be explicit and independent from search order:

| Authoring form | Contract |
| --- | --- |
| `one_case(...)` / current `one_path(...)` | Return the first satisfying completion under a documented, versioned total ordering; use `best_case` when cost optimality is intended. |
| `all_cases(...)` / current `all_paths(...)` | Return every semantically distinct completion inside explicit finite bounds, or return a typed incomplete-enumeration result. |
| `cover(all_combinations(x, y), ...)` | Return at least one canonical completion for every requested assignment tuple; report an unsatisfiable tuple rather than silently omitting it. |
| `best_case(..., by = objective)` | Return one optimum under an explicit objective and deterministic tie breaker. |
| `all_best_cases(..., by = objective)` | Return every optimum under that objective and the same explicit bounds. |

`all_cases` must define whether independent commuting actions are equivalent;
it must not vaguely promise every concrete scheduler interleaving. `cover` is
often the better test-generation contract because it covers author-selected
dimensions without multiplying irrelevant internal alternatives.

Solver-controlled inputs such as `timeout_a` require a finite value set, range,
and step. A symbolic identity such as `run_a` may remain abstract until a fresh
action creates it. An observed value is bound during execution, so the same
constraint becomes a runtime assertion rather than a source of pre-execution
values. Binding modes in the action catalog determine the phase; authors should
not restate them in every plan.

Faults are never inferred merely because a faulty path would satisfy the goal.
The planner may choose among the explicit `fault` values above and synthesize
semantic actions inside the declared `during` scope. It must not invent a fault,
RPC target, authority, capability, observation source, or justified claim.

References are scoped by the plan that consumes them, even when their Starlark
variables are declared at module scope. The action catalog remains authoritative
for whether a reference is an input, fresh output, or observed value; authors
should not repeat those binding modes in every plan. Host value types let the
frontend reject using an activity reference where a Nexus operation is
required, a machine reference where an RPC target is required, or a projection
whose result type does not match `run`.

Module-level accumulation is acceptable for declarations and completed tests,
provided evaluation uses a fresh package collector, every declaration carries
its owner, and the package is frozen and validated after evaluation. Avoid an
implicit current machine or current test: those would make imports, helper
functions, and source-located diagnostics order-dependent in surprising ways.

## Constraint planner boundary

The deep module is the planner, not either authoring syntax. A minimal canonical
vocabulary is:

```text
Term       = Symbol(type, id) | Literal(type, value) | finite expression nodes
Constraint = Equal | Different | Compare | MemberOf | Predicate | Before
           | Fresh | Observed | Requires | Objective
Answer     = substitution + residual constraints + completed actions
           + cost + proof provenance
```

The first native Go solver should remain deliberately smaller than Prolog or a
general CP/SAT/SMT system:

1. propagate equality, disequality, finite membership, type, freshness,
   capability, relation, and simple duration/integer difference constraints;
2. search backward from unsatisfied semantic goals through registered action
   effects and preconditions;
3. search the enabled frontier of the partial-order DAG instead of materializing
   every total order first;
4. table canonical subproblems keyed by world, remaining frontier,
   substitution/domains, active policies, domain version, and profile;
5. use answer subsumption only for explicitly lossy modes such as `one_case` or
   `best_case`; and
6. retain all inequivalent answers for `all_cases` and refuse to call truncated
   enumeration complete.

Each completion should explain which action effect satisfied each authored
goal, which bindings and choices were selected, which actions/resources were
synthesized, and which source constraint forced the choice. Failure should
distinguish unsatisfiable, ambiguous, unsupported, conditional/unknown, and
incomplete because a bound was exhausted. Only fully grounded, validated
answers cross the existing `CompletedPath` execution boundary.

The solver may later delegate large numeric, cardinality, or scheduling
fragments through this IR, but a general solver is not required for the first
useful version. Picat or Prolog generation could be a differential oracle for
small fixtures; embedding either runtime would add packaging, interop,
cancellation, marshaling, and a second diagnostic stack without replacing
Umpire's existing Go execution boundary.

## Go versus Starlark

The logic-planning capability is independent of Starlark. A strong typed Go DSL
over the same IR can express the same query:

```go
runA := regress.Some[nexus.RunID]("run-a")
runB := regress.Some[nexus.RunID]("run-b")

timeoutA := regress.Choose("timeout-a", 2*time.Second, 3*time.Second, 4*time.Second)
timeoutB := regress.Choose("timeout-b", 0, time.Second, 2*time.Second)

plan := regress.AllCases(
	regress.Where(
		regress.Different(runA, runB),
		regress.Exceeds(timeoutA, timeoutB, time.Second),
	),
	nexus.Start(first, runA, timeoutA),
	nexus.Start(second, runB, timeoutB),
)
```

This is illustrative rather than a proposed package API. It compares the best
plausible Go surface, not today's string-heavy constructors.

| Concern | Go | Starlark |
| --- | --- | --- |
| Constraint solving, completion, and coverage | Same native planner | Same native planner |
| Variable/action typing | Compile time plus semantic validation | Evaluation time plus semantic validation |
| IDE navigation and refactoring | Strong | Requires custom tooling |
| Native relational expressions | Needs constructors or Go source analysis | Quoted lambdas preserve familiar operators |
| Concision and reviewability | Slightly noisier | Potentially cleaner |
| Non-Go authors and portable descriptor data | Weaker | Stronger |
| Hermetic bounded evaluation | Requires API discipline | Natural fit |
| Build, debugging, and maintenance | One toolchain | Additional frontend and parity tests |
| Projection to runtime/formal backends | Same IR | Same IR |

If the primary authors are Temporal Go engineers, the Go frontend may be
sufficient. Starlark earns adoption only if quoted expressions, reviewability,
portable descriptor packages, or non-Go accessibility produce a measured gain
large enough to pay for weaker tooling and another frontend. The two surfaces
must never acquire different solver semantics.

## Go generation

Generating Go is useful at the boundary to the implementation, but generated Go
should not sit between Starlark and the canonical model checker pipeline.

Generate:

- stable ID/state/action constants or wrapper types where they improve adapter
  signatures;
- required action-realizer, fact-projector, observation, and refinement binding
  interfaces with typed parameters;
- a scenario catalog consumed by one table-driven Go test and by
  counterexample replay;
- source maps back to `.umpire.star` declarations.

Do not generate:

- a giant Go struct-literal copy of the Starlark model merely to reconstruct the
  IR;
- concrete server setup or business logic;
- editable adapter stubs that regeneration could overwrite;
- tests whose only assertion is that generated output matches itself.

The handwritten side should implement generated interfaces or populate a
generated constructor with all required bindings. This restores compile-time Go
checking at the implementation boundary even though the authoring language is
dynamic. Optional behavior must be explicit in the descriptor; absence must not
silently disable a test.

Generated code must be checked in or reproducibly regenerated according to the
repository's normal policy, contain a source hash/version, and support the
existing `--check`-style drift test.

## Risks and controls

| Risk | Control |
| --- | --- |
| Dynamic type errors and misspelled strings | Custom symbolic DSL values, strict constructor argument checking, full canonical validation, and negative compiler tests with file/line diagnostics. |
| Two frontends drift semantically | Both lower to the same versioned constraint/descriptor IR; differential fixtures compare normalized IR and completed cases. Designate a source of truth per migrated model rather than merging declarations from both. |
| Quoted expressions become arbitrary callbacks | Compile a documented pure expression subset from the retained AST, reject unsupported calls/control flow, and never invoke the lambda during planning or execution. |
| Hidden macro behavior | Provide a small standard library, require conveniences to lower to inspectable IR, emit normalized IR beside generated artifacts, and keep provenance. |
| Runaway or malicious evaluation | No I/O/network/time predeclared functions, repository-rooted allowlisted `load`, execution-step and wall-clock limits, cancellation, and bounded output sizes. Starlark hermeticity is only as strong as Umpire's host functions. |
| Dependency/API churn | The repository does not currently depend on `go.starlark.net`; pin a reviewed version behind a narrow internal wrapper and treat upgrades as compiler changes. |
| Poor IDE experience | Formatter/linter, generated symbol catalog, source-aware diagnostics, and eventually an editor/LSP layer. Do not claim Go-equivalent tooling in the first version. |
| Search or table explosion hidden by concise syntax | Require finite domains and explicit action/path/table/time bounds, show expansion counts and estimated combinations, and return incomplete rather than unreachable when a limit is exhausted. At 10x model size, planning and verifier state growth dominate frontend evaluation cost. |
| `all_cases` overpromises completeness | Version the semantic path-equivalence relation, make commuting-action reduction visible, and fail if enumeration is truncated. |
| Negation treats absent evidence as false | Permit closed-world negation only for relations declared complete; otherwise preserve unknown/inconclusive and reject unsafe ungrounded negation. |
| Semantic drift among backends | Differential tests against Umpire's interpreter and every supported backend; conveniences must lower before backend selection. |
| FizzBee-specific facilities are immature | Borrow concepts, not implementation assumptions; channels, source diagnostics, and some failure facilities are explicitly documented as incomplete or work in progress. |

Because Starlark would run at generation/test-build time rather than in the
Temporal server, it adds no production request-path or crash-recovery behavior.
The operational failure mode is a failed or stale generation step, which should
be made deterministic and CI-fatal.

## Recommended spike

Use one small end-to-end slice—preferably the Workflow lifecycle, its action
bindings/gaps, and representative sparse scenarios—but test the planner and the
frontend as separate hypotheses:

1. Specify current `OnePath`/`AllPaths` semantics, including path equivalence,
   canonical ordering, freshness, observations, faults, and what bound
   exhaustion means.
2. Extract a typed constraint/planner boundary in Go without changing completed
   suites. Keep the current search as the first implementation and compare its
   answers semantically where provenance differs.
3. Add proof/residual artifacts, tabled canonical subproblems, finite-domain
   propagation, and explicit `one`, `all`, and targeted `cover` completion
   modes. Do not expose new syntax before the IR can serialize, validate, bound,
   and explain them.
4. Design the strongest plausible typed Go DSL over that boundary.
5. Build a restricted Starlark loader, immutable DSL values, and quoted-lambda
   expression compiler for the same boundary. Retain the parsed source AST,
   compile a documented pure subset, and never keep runtime callbacks.
6. Author the same lifecycle and cases in both frontends: a happy path, a
   partial plan with synthesized actions, symbolic identity disequality,
   duration arithmetic, partial order, an explicit RPC fault choice, an
   observed binding, and an unsatisfiable combination.
7. Compare the projected runtime catalog, completed cases, verification model,
   interpreter results, and all four formal generators/checkers. Use Picat only
   as an optional differential oracle for small bounded completion fixtures.
8. Measure authoring lines and repeated tokens, review readability, source
   diagnostics, IDE/navigation quality, generation time, search states/table
   memory, proof usefulness, and counterexample-to-source traceability.

Adopt the Starlark frontend only if the spike achieves all of the following:

- semantic parity with the Go frontend at every affected projection;
- no planner or backend behavior conditioned on which frontend authored the IR;
- materially clearer declarations and symbolic expressions than the best Go
  DSL, not merely fewer braces;
- a small expression subset that covers real invariants, guards, and plan
  constraints without arbitrary callbacks;
- source-located errors of comparable usefulness to Go compiler/DSL errors;
- reproducible generation and drift checking; and
- typed Go binding interfaces that require no generated-file edits.

## Decision

Proceed with the typed constraint planner independently of the frontend choice.
That is the substantive capability: partial plans become constraint queries and
Umpire produces bounded executable witnesses, exhaustive semantic completions,
or targeted coverage sets with proofs and typed failure.

Keep Go as the implementation and semantic authority. First make the Go DSL as
deep and typed as practical. Then run a narrow Starlark prototype over the same
IR. Starlark's strongest case is not shorter constructors; it is readable flat
model declarations, nested immutable plan data, and quoted native expressions
that compile into shared invariants, guards, and constraints.

If the primary authors are Go engineers and the head-to-head comparison shows
only cosmetic improvement, retain Go. If quoted expressions, portable model
packages, or non-Go accessibility produce a clear measured win, adopt Starlark
as an optional or model-by-model source frontend with generated typed Go
boundaries. Do not embed Prolog or Picat, make Starlark executable model logic,
or allow either frontend to infer faults, authority, capabilities, or evidence
that the author did not declare.
