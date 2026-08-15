# What Umpire can learn from Prolog and Picat

Date: 2026-08-15

Status: Research and design analysis. This document does not propose replacing Umpire's Go core or
embedding a logic-language runtime.

## Executive finding

Umpire can benefit substantially from Prolog and Picat, but mostly **below** the proposed Starlark
syntax.

Umpire's sparse regression compiler is already a small, specialized logic planner. It has typed
symbols, local logical variables, predicate templates, unification against grounded goals,
backward satisfaction of preconditions, fresh and observed bindings, partial-order constraints,
and enumeration of satisfying completed paths. The main opportunity is therefore not to make
authors write Prolog. It is to make those existing ideas explicit and complete:

1. represent an incomplete test plan as a typed constraint problem;
2. use real substitutions and constraint propagation instead of repeatedly enumerating every
   same-typed symbol;
3. table canonical planning subproblems so recursive or overlapping searches terminate and are not
   recomputed;
4. use mode-directed answer selection to distinguish one canonical plan, one optimal plan, all
   optimal plans, and all plans under explicit bounds; and
5. return residual goals and conflicting constraints as explanations when no unconditional plan is
   available.

Picat is the closer operational model. Its planner asks authors to define a goal test and a
relation from a state to a successor state, action, and cost; `plan` fills a plan and `best_plan`
finds an optimal one. It even exposes `sequence(PartialPlan, NextAction)` as a way to restrict which
actions may extend the current partial plan ([Picat User's Guide, Chapter 8](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).
Prolog contributes the more general lessons: relations can be queried in several directions,
unification accumulates shared bindings, constraint solvers propagate information before search,
tabling computes goal-directed fixed points, and negation or incomplete knowledge requires much
more care than a Boolean `not` suggests ([SWI-Prolog unification](https://www.swi-prolog.org/pldoc/man?section=compare),
[constraint logic programming](https://www.swi-prolog.org/pldoc/man?section=clp),
[tabling](https://www.swi-prolog.org/pldoc/man?section=tabling)).

The recommended division is:

```text
valid Starlark syntax
    -> immutable model and partial-plan values
    -> typed canonical constraint IR
    -> native Go unification + propagation + tabled planning
    -> completed paths, residual goals, or typed failure
    -> existing realization, execution, observation, and qualification layers
```

Starlark should construct the question. A typed Go solver should answer it. Neither Prolog nor
Picat should become Umpire's source language, runtime, or semantic authority.

## The current Umpire compiler is already logic-shaped

This is not an analogy imposed from outside; the current implementation already contains the
following pieces:

- An author writes declarative outcomes, actions, relations, bindings, requirements, unordered
  groups, policy scopes, labels, precedence edges, and finite repetition. `OnePath` and `AllPaths`
  select completion cardinality
  ([`instruction.go`](common/testing/umpire/regress/instruction.go#L62-L225)).
- Normalization type-checks the source and lowers it to a domain-independent constraint DAG of
  symbols, semantic nodes, ordering edges, policy scopes, labels, and environment requirements
  ([`normalize.go`](common/testing/umpire/regress/normalize.go#L110-L230)).
- The model catalog describes each action with typed local variables, input/fresh/observed binding
  modes, preconditions, effects, resources, capabilities, independence, and a realization key
  ([`catalog.go`](common/testing/umpire/regress/catalog.go#L9-L98)).
- `unify` matches an action effect template against a grounded goal and produces variable
  bindings; `instantiateAtoms` applies the resulting substitution
  ([`compiler_canonical.go`](common/testing/umpire/regress/compiler_canonical.go#L11-L49)).
- `satisfyGoal` works backward from an outcome or relation, finds actions whose effects unify with
  it, recursively satisfies their preconditions, and then applies the action
  ([`compiler_search.go`](common/testing/umpire/regress/compiler_search.go#L271-L319)).
- `completeBindings` derives bindings from existing facts, then enumerates every plan symbol with
  the required type for variables that remain unbound
  ([`compiler_search.go`](common/testing/umpire/regress/compiler_search.go#L321-L377)).
- Compilation enumerates topological orders of the milestone DAG, satisfies each node, deduplicates
  worlds and paths, sorts completed paths canonically, returns the first for `OnePath`, and rejects
  bounded truncation for `AllPaths`
  ([`compiler.go`](common/testing/umpire/regress/compiler.go#L126-L237)).

That already lets a test omit mechanics between semantic key frames. Umpire documents the
contract directly: the compiler “fills the gaps between author-supplied semantic key frames,
synthesizes resources and data flow, and emits a completed plan”
([`UMPIRE.md`](UMPIRE.md#L114-L128)).

The current engine is deliberately narrower than Prolog:

- terms are flat predicate arguments, not recursively structured first-order terms;
- unification is one-way template matching against a grounded atom, not general unification
  between two partially instantiated terms;
- a recursion stack rejects a repeated `(world, goal)` rather than suspending it and computing a
  tabled fixed point;
- variable completion is a typed Cartesian search over named symbols;
- canonical choice is applied after path generation rather than used to subsume worse answers
  during search; and
- failure explanation records a shortest missing chain, not a proof tree, residual constraint set,
  or minimal conflicting core.

These restrictions are not inherently wrong. They keep the compiler small and predictable. They
identify exactly where logic-programming techniques would earn their cost.

## What belongs in the user-facing Starlark syntax

### Keep models readable as state machines and relations

Prolog's `predicate(term, ...)` notation would expose the compiler's implementation model and make
ordinary lifecycle declarations harder to read. The proposed flat declarations should remain the
primary surface:

```python
Nexus = machine("NexusOperation")

starts_at(Nexus.unspecified)
succeeds_at(Nexus.succeeded)
fails_at(Nexus.failed | Nexus.timed_out | Nexus.rejected)

transition(Nexus.scheduled >> on.start >> Nexus.started)
transition(Nexus.started >> on.complete >> Nexus.succeeded)
```

The compiler may lower each transition to logical preconditions and effects. Authors do not need
to see the Horn-clause-shaped representation to benefit from it.

Relations should remain explicit, typed domain vocabulary. The Prolog lesson is that the compiler
should be able to use a relation in more than one direction when its declared modes permit that;
it is not that every Starlark function should become an arbitrary relational predicate. SWI-Prolog
documents input, output, partial, and ground argument modes separately from determinism such as
`det`, `semidet`, `nondet`, and `multi`; its documentation also notes that these types do not by
themselves form a static type system
([SWI-Prolog mode and determinism declarations](https://www.swi-prolog.org/pldoc/man?section=modes)).
Umpire should keep its actual `Type`, `ParameterMode`, and `BindingMode` validation rather than
copying only Prolog's notation.

### Expose typed holes, not Prolog variables

Starlark variables are ordinary lexical bindings. They do not become unbound on backtracking and
must not be presented as if they were Prolog logic variables. A host-created value can instead
represent a typed hole in the plan IR:

```python
op = Nexus.ref("op")
handler = Workflow.some("handler")
terminal = Nexus.some_state("terminal", within = Nexus.terminal)

test("an operation settles through some handler", one_path(
    reaches(op, Nexus.started),
    linked(op, handler),
    reaches(op, terminal),
))
```

This is illustrative syntax, not a committed API. `some` should create a source-located typed
symbol with a finite domain. Repeated use of the same value means equality by shared identity.
Different values remain different only when freshness or an explicit disequality requires it.
The solver, not Starlark evaluation, performs unification.

Sound unification matters even if the first version retains only flat terms. SWI-Prolog documents
that ordinary unification can create cyclic terms and that sound unification rejects binding a
variable to a term containing itself
([`unify_with_occurs_check/2`](https://www.swi-prolog.org/pldoc/man?predicate=unify_with_occurs_check%2F2)).
Umpire should avoid rational trees entirely: canonical plan terms should be finite, acyclic, typed
values.

### Add constraints only when they state test intent

Useful Starlark constraints would include:

- equality or disequality of typed references;
- membership in a finite state, value, RPC, fault, or realization domain;
- ordering and non-overlap of named milestones;
- exact or bounded action, retry, fault, and resource counts;
- capability requirements;
- freshness and observed-binding requirements; and
- a declared plan cost objective.

For example:

```python
test("both callers use one handler", best_path(
    any_order(
        start(left, handler),
        start(right, handler),
    ),
    where(
        left != right,
        faults <= 1,
    ),
    reaches(left, Nexus.succeeded),
    reaches(right, Nexus.succeeded),
))
```

Again, this illustrates the boundary rather than prescribing operators. Every expression must
produce immutable typed constraint data. It must never call a solver during Starlark evaluation.

Constraint logic programming is relevant because constraints can delay a decision until enough
information exists and can propagate all known restrictions before enumeration. SWI-Prolog's CLP
manual contrasts this with generate-and-test search, where separately generating values for two
integer restrictions can create an unnecessary infinite product
([SWI-Prolog constraint logic programming](https://www.swi-prolog.org/pldoc/man?section=clp)).
Picat exposes `cp`, `sat`, `smt`, and `mip` through a common modeling interface
([Picat User's Guide, Chapter 12](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).
The syntax lesson is solver-independent constraints, not a promise that Umpire needs four solver
backends.

### Make completion policy explicit

Prolog predicates may have zero, one, or many answers. Picat makes backtrackable rules explicit and
uses mode-directed tabling to retain one best answer or every best answer. Umpire should similarly
avoid an implicit “whatever depth-first search found first” contract.

The current names can remain, but their semantics should be sharpened and may eventually expand:

| Authoring form | Required meaning |
| --- | --- |
| `one_path(...)` | One canonical satisfying completion under a documented total ordering |
| `all_paths(...)` | Every semantically distinct completion inside explicit finite bounds, or a typed incomplete-enumeration error |
| `best_path(..., by = ...)` | One optimum under an explicit cost function and deterministic tie-breaker |
| `all_best_paths(..., by = ...)` | Every optimum inside explicit finite bounds |

Picat table modes distinguish input, output, minimized, maximized, and “all minimum” answers; its
planner uses the same machinery for planning
([Picat User's Guide, Sections 7.1 and 8.1](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).
That is a better model for these modes than Prolog's cut.

### Preserve nested plan data

Logic programming does not change the earlier conclusion that nested plan data is appropriate.
`during(policy, body...)`, `any_order(...)`, and finite repetition encode scope and partial order,
not imperative control flow. They should continue to lower into the existing DAG and scope IR.
Logical holes and constraints supplement that tree; they do not replace its meaningful structure.

### Keep faults and RPCs explicit

Plan completion must never infer that a fault should exist merely because adding one makes an
outcome reachable. A fault policy is an author-selected perturbation with an exact scope; an RPC
is a typed realization target; a semantic action describes model behavior. These three layers
should remain separate.

It is reasonable for the solver to fill semantic actions **inside** an explicitly declared fault
scope or to choose among an explicitly declared finite set of allowed faults. It is not reasonable
for it to invent fault injection, canary authority, missing capabilities, or observation evidence.
Those choices change the experiment or the justified claim, rather than merely completing it.

## What belongs in the compiler and solver internals

### A typed term and substitution layer

Replace the current special-purpose `map[string]Argument` binding flow with an explicit, persistent
substitution over typed terms:

```text
Term       = Symbol(type, id) | Literal(type, value) | finite Constructor(type, terms...)
Constraint = Equal | Different | MemberOf | Predicate | Before | Fresh | Observed | Requires
Answer     = substitution + residual constraints + completed actions + cost + provenance
```

General term constructors should be added only if a concrete domain needs them. Flat typed terms
cover current entities and values and avoid the complexity of arbitrary Prolog terms. Union-find
or an equivalent persistent unifier can make equality propagation cheap; an occurs check remains
mandatory if constructors are introduced.

`InputBinding`, `FreshBinding`, and `ObservedBinding` already provide stronger domain-specific
modes than generic `+` and `-`. Preserve them. Add a checked determinism contract to each compiled
capability or query where useful:

- exactly one applicable realization;
- zero or one;
- zero or more; or
- one or more.

This turns ambiguous grounding into a declared contract instead of a consequence of catalog order.

### Constraint propagation before enumeration

The first solver does not need CP-SAT or SMT. It can propagate Umpire's existing finite constraints:

- intersect symbol domains by type and membership;
- merge equal symbols and reject unequal literals;
- enforce fresh symbols against the created set;
- use relation facts to reduce candidate identities;
- reject unavailable capabilities and realizations early;
- maintain ordering reachability incrementally; and
- carry action preconditions and effects as positive predicate constraints.

This directly improves `completeBindings`, which currently expands every same-typed plan symbol for
each unbound action variable. A general solver backend becomes justified only when real models need
large numeric ranges, cardinality constraints, scheduling, or optimization that this finite
propagator cannot handle. The constraint IR should remain backend-independent so a future CP, SAT,
or SMT adapter cannot leak syntax or semantics into Starlark.

### Tabled planning over canonical subproblems

The highest-value implementation lesson is tabling. SWI-Prolog describes tabling as memoizing
answers, suspending recursive variant calls instead of looping, and computing a goal-directed fixed
point. It also calls out the costs: tables consume memory and become stale when their underlying
world changes ([SWI-Prolog tabled execution](https://www.swi-prolog.org/pldoc/man?section=tabling)).
Picat's planner is implemented with tabling and requires states to use compact ground
representations because all searched states are tabled
([Picat User's Guide, Chapter 8](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).

For Umpire, a planning subproblem key should include at least:

```text
canonical world facts and created symbols
+ remaining milestone DAG frontier
+ canonical substitution and finite domains
+ active policy scopes
+ selected profile/capability set
```

Symbols in the key should be alpha-renamed so two states that differ only in private symbolic names
share a table entry. Runtime identities must not participate until an observed binding intentionally
grounds them. Tables should be local to one compile request over one immutable domain version and
profile, eliminating the stale-world problem.

Variant subproblems should suspend until new answers are available rather than returning failure as
the current recursion stack does. The first implementation can use an explicit worklist and strongly
connected components instead of reproducing a Prolog virtual machine.

### Answer subsumption and optimal completion

The current compiler generates all surviving states, sorts paths, and then keeps the first in
`OnePath` mode. Picat's mode-directed tabling can retain only the minimum answer, or all answers
having the minimum objective, per input state
([Picat User's Guide, Section 7.1](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).

Umpire can apply the same idea with an explicit cost tuple, for example:

```text
(semantic actions, faults, synthesized resources, fresh entities, catalog-order tie breaker)
```

The actual tuple is a product decision and must be versioned because it determines which plan is
canonical. `one_path` can subsume more expensive answers while searching. `all_paths` must retain
all semantically distinct answers and must not use lossy subsumption. `all_best_paths` can retain
all answers tied under the declared objective.

### Search the partial-order frontier instead of materializing every total order

The current compiler enumerates every topological order before satisfying milestones. A tabled
scheduler can instead choose from the currently enabled DAG frontier, apply one milestone, then
memoize the resulting subproblem. Independent actions that commute can be canonicalized using the
catalog's existing `IndependentOf` relation; the compiler already reduces completed path keys by
swapping independent actions into canonical order
([`compiler_canonical.go`](common/testing/umpire/regress/compiler_canonical.go#L156-L180)).

This should be specified carefully: `all_paths` may mean all total traces, all completed semantic
worlds, or all traces modulo commuting independent actions. Umpire currently deduplicates some
commuting traces, so the canonical equivalence relation should be made explicit before changing the
search algorithm.

### Partial-plan guidance inspired by Picat

Picat's optional `sequence(PartialPlan, NextAction)` relation selects viable next action forms based
on the partial plan constructed so far
([Picat User's Guide, Chapter 8](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).
Umpire can learn from this without exposing arbitrary callbacks.

A declarative guidance IR could contain:

- allowed or forbidden adjacent action classes;
- precedence between action classes;
- bounded consecutive retries;
- phase restrictions such as “finish this participant before switching”;
- cost adjustments; and
- symmetry-breaking preferences.

Guidance must remain declarative, inspectable, and backend-independent. A Starlark lambda that runs
during search would make results depend on an opaque evaluator callback, frustrate serialization and
source mapping, and prevent other planning backends from sharing the same semantics.

### Proofs, residual goals, and conflicts

Plan completion should return more than “reachable” or “cannot satisfy sparse plan.” Every answer
should retain which action effect satisfied each goal, which substitutions were made, what actions
were synthesized, and which source constraints forced the choice. A failure should identify:

- the unsatisfied goal;
- candidate actions considered;
- missing capabilities or resources;
- rejected substitutions and their type/freshness/equality conflict;
- the relevant source spans; and
- whether the search was unsatisfiable, ambiguous, unsupported, or merely incomplete because of a
  bound.

Umpire already records candidates and a shortest missing chain. Logic systems suggest making this a
first-class proof or residual artifact. SWI-Prolog's well-founded semantics can return a residual
program explaining why an answer remains conditional or undefined
([SWI-Prolog well-founded semantics](https://www.swi-prolog.org/pldoc/man?section=WFS)).
Umpire does not need to copy that representation, but it should copy the principle that unresolved
answers retain a machine-readable explanation.

## Incomplete and partial plans: what can safely be inferred

“Incomplete plan” covers several different cases. They need different policies:

| Missing information | Current support | Recommended treatment |
| --- | --- | --- |
| Actions between known semantic milestones | Yes | Continue synthesizing from registered semantic capabilities |
| Order between independent milestones | Yes, through `AnyOrder` and DAG edges | Search the enabled frontier and retain the declared equivalence/bounds |
| Symbolic implementation identity | Yes, through plan symbols and observed bindings | Keep symbolic until a declared observed projection grounds it |
| Which typed entity or value satisfies a relation | Partial | Add typed finite holes plus unification and domain propagation |
| Which terminal state is acceptable | No general state-set hole | Allow a typed state variable constrained to an explicit set |
| Which completion is preferred | Canonical post-sort only | Add explicit, versioned objectives and best/all-best modes |
| Which fault occurs | Only when authored as a policy | Never invent; choose only from an explicit author-provided finite fault domain |
| Which RPC realizes a semantic action | Catalog/realization concern | Resolve only through validated realization metadata, never by semantic guessing |
| Missing environment capability or evidence | Rejected/qualified | Never synthesize authority or treat absence as success |
| Unbounded recursion or enumeration | Rejected or bounded | Preserve explicit bounds and distinguish incomplete search from unsatisfiability |

This is the central assurance rule: the solver may complete **means** inside the declared semantic
experiment. It must not complete **intent**, **authority**, or **evidence** on the author's behalf.

## Negation and partial observation

Prolog's `\+ Goal` means that `Goal` cannot be proven; it is not classical negation. SWI-Prolog
also documents cases where disequality implemented through negation fails on insufficiently
instantiated variables even though unequal solutions exist
([SWI-Prolog control predicates](https://www.swi-prolog.org/pldoc/man?section=control),
[comparison and unification](https://www.swi-prolog.org/pldoc/man?section=compare)).

That caveat is especially important for Umpire because its runtime observation is intentionally
partial. The Monitor ignores unregistered traffic, and the project explicitly states that missing
evidence is not evidence of correctness ([`UMPIRE.md`](UMPIRE.md#L98-L112)). Therefore:

- do not expose a generic `not(predicate(...))` whose meaning is closed-world absence;
- represent explicit negative model facts separately from facts not observed;
- use at least `true`, `false`, and `unknown/inconclusive` for observation-backed propositions;
- permit closed-world negation only inside a model relation explicitly declared complete; and
- delay or reject negation over ungrounded variables rather than letting search order determine it.

Well-founded semantics demonstrates how recursive negation can preserve an `undefined` result and
its dependencies rather than choosing an arbitrary Boolean answer
([SWI-Prolog well-founded semantics](https://www.swi-prolog.org/pldoc/man?section=WFS)).
Umpire already has `unsupported` and `inconclusive` qualified outcomes. It should integrate solver
unknowns with that evidence model instead of importing Prolog's closed-world default.

## What should not be copied

### Do not copy cut or clause-order semantics

Prolog's cut discards choice points created since entering the predicate. The SWI-Prolog manual
warns that cut can destroy declarative constraint semantics and prematurely prune delayed goals
([control predicates](https://www.swi-prolog.org/pldoc/man?section=control),
[constraint logic programming](https://www.swi-prolog.org/pldoc/man?section=clp)).

Umpire should have named query modes, cost objectives, deterministic tie-breakers, and explicit
bounds instead. Catalog declaration order may provide a final stable tie-breaker, but it must not
silently change satisfiability or completeness.

Picat's explicit backtrackable `?=>` and committed `=>` rules are clearer than an embedded cut, but
Umpire still should not expose rule commitment in its state-machine syntax. The useful lesson is to
declare whether a query expects one answer or many, and whether it retains one optimum or all
optima ([Picat User's Guide, Section 4.1](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).

### Do not copy dynamic typing

Both Prolog and Picat permit flexible runtime terms; Picat is explicitly dynamically typed. Umpire's
domain already knows entity and value types, argument modes, fresh/observed provenance, relation
endpoints, capabilities, and realizations. Weakening those contracts would turn useful relational
search into ambiguous string matching.

The Starlark frontend may be dynamically typed as a host language, but every value admitted to the
canonical IR should carry a checked Umpire type and source span.

### Do not expose arbitrary recursive predicates initially

User-defined Horn clauses would introduce termination, stratified-negation, indexing, module,
debugging, and cross-backend semantic questions. Umpire can gain most of the value by compiling
machines, relations, and action capabilities into a fixed positive rule fragment under explicit
bounds. Derived recursive relations should wait until tabled evaluation and explanation artifacts
are proven on the existing catalog.

### Do not equate plan success with implementation evidence

A logic solver proves only that a completion is valid in Umpire's abstract catalog. It does not
prove that Temporal executed it, that observations were complete, or that the model was correct.
The existing completed-path, realization, runtime observation, reconciliation, property, and
qualified-claim boundaries must remain intact
([`UMPIRE.md`](UMPIRE.md#L158-L186), [`UMPIRE.md`](UMPIRE.md#L231-L248)).

### Do not embed Prolog or Picat as the first implementation

SWI-Prolog supports embedding through its C foreign interface and permits a C host to enumerate
multiple Prolog answers ([SWI-Prolog foreign interface](https://www.swi-prolog.org/pldoc/man?section=foreign)).
For this Go repository that implies a native runtime, C interop or a process boundary, packaging,
thread and cancellation policy, value marshaling, and a second diagnostics stack.

Picat's official guide documents extending the implementation with C-defined deterministic
predicates by rebuilding it, rather than a stable Go embedding boundary
([Picat User's Guide, Appendix G](https://picat-lang.org/download/picat_guide_html/picat_guide.html)).

Those costs are hard to justify when Umpire already owns a Go planner and canonical IR. A Prolog or
Picat exporter could be valuable later as a differential oracle for small fixtures. It should not
be the production compiler or the source of completed runtime plans.

## Recommended architecture

### 1. Keep Starlark as a pure frontend

Evaluation produces frozen declarations, typed references, plan groups, and constraint values. It
does not search, invoke Temporal, inspect observations, or retain Starlark callbacks. Each value
keeps its source span.

### 2. Extend the canonical sparse IR

Add only the concepts the solver needs and artifacts can preserve:

- typed logical variables with stable IDs and finite domains;
- equality, disequality, membership, cardinality, and objective constraints;
- explicit completion mode and bounds;
- declarative partial-plan guidance;
- residual constraints and proof provenance; and
- a versioned semantic equivalence for completed paths.

The existing node, edge, scope, requirement, symbol, action, resource, and policy representations
remain the base.

### 3. Implement a native Go tabled constraint planner

Refactor behind a small interface that takes the immutable domain, normalized IR, profile, and
bounds and returns a stream or bounded collection of `Answer` values. Internally use:

- persistent typed substitutions;
- finite-domain propagation;
- canonical world/subproblem keys;
- a worklist for mutually recursive goals;
- answer subsumption for best-plan modes;
- frontier search for partial orders; and
- hard table, depth, action, path, and wall-clock limits.

All limit exhaustion must produce an incomplete-search result, never an unreachable result.

### 4. Preserve the existing completed-suite boundary

Only fully grounded, validated answers become `CompletedPath`. Each path retains synthesized
actions, resources, policy intervals, milestones, bindings, objective cost, and a proof map back to
source constraints. Execution and observation continue unchanged.

### 5. Make explanations first-class

Compilation should return one of:

- completed answers;
- unsatisfiable with a conflict/explanation;
- ambiguous where a one-answer contract was declared;
- unsupported because a model or environment capability is missing;
- conditional/unknown because a required fact or observation is not established; or
- incomplete because an explicit resource bound was exhausted.

This classification should refine the existing typed `CompileError` categories rather than become
formatted solver text.

## Suggested implementation sequence

1. **Specify current semantics.** Lock down what makes two completed paths equivalent and what
   `OnePath` and `AllPaths` promise. Add golden fixtures for ordering, freshness, observed bindings,
   faults, resources, and ambiguous grounding.
2. **Extract a solver boundary without changing behavior.** Keep the current search as the first
   implementation and compare completed suites byte-for-byte or semantically where provenance
   differs.
3. **Add tabled subproblem memoization.** Start with positive goals and the existing flat terms.
   Measure explored subproblems, table memory, and path parity.
4. **Add answer subsumption for `OnePath`.** Use the current canonical ordering as the initial
   explicit cost/tie-break specification. Keep `AllPaths` lossless.
5. **Replace same-type Cartesian grounding with finite-domain propagation.** Preserve every current
   type, freshness, observation, and relation validation error.
6. **Return proof and residual artifacts.** Improve the current candidate/missing-chain diagnostic
   before exposing more partiality to authors.
7. **Add typed holes and `best/all_best` to Starlark.** Only after the IR and solver can serialize,
   validate, explain, and bound them.
8. **Evaluate a general constraint backend only with representative pressure.** Use a scheduling,
   numeric-bound, or cardinality case that the native propagator handles poorly. Keep it behind the
   same IR.
9. **Optionally generate Picat for differential testing.** Compare small bounded completion sets and
   optimal costs; never make the external runtime authoritative.

## Decision

Learn deeply from Prolog and Picat, but copy selectively.

- From **Prolog**, take relational semantics, shared logical bindings, sound finite unification,
  constraint propagation, tabled fixed points, explicit modes/determinism, and caution around
  negation and cuts.
- From **Picat**, take a ground-state/action/cost planning model, completion of a partial plan,
  explicit one-versus-many search, mode-directed optimal answers, solver-independent constraints,
  and declarative guidance based on the partial plan.
- Keep **Starlark** as the elegant, valid, readable construction language.
- Keep **Go and Umpire's canonical IR** as the typed semantic and execution authority.

The result would be more than syntactic sugar. Today Umpire can fill action gaps between mostly
ground milestones. A logic-inspired solver could safely complete identities, values, acceptable
states, ordering, and optimal routes under typed constraints, while explaining ambiguity and
remaining unknowns. That is a material increase in capability—as long as Umpire refuses to infer
faults, authority, evidence, or test intent that the author did not declare.

## Primary sources

- [SWI-Prolog Reference Manual: comparison and unification](https://www.swi-prolog.org/pldoc/man?section=compare)
- [SWI-Prolog Reference Manual: sound unification](https://www.swi-prolog.org/pldoc/man?predicate=unify_with_occurs_check%2F2)
- [SWI-Prolog Reference Manual: type, mode, and determinism declarations](https://www.swi-prolog.org/pldoc/man?section=modes)
- [SWI-Prolog Reference Manual: control predicates, cut, and negation as failure](https://www.swi-prolog.org/pldoc/man?section=control)
- [SWI-Prolog Reference Manual: constraint logic programming](https://www.swi-prolog.org/pldoc/man?section=clp)
- [SWI-Prolog Reference Manual: tabled execution](https://www.swi-prolog.org/pldoc/man?section=tabling)
- [SWI-Prolog Reference Manual: tabling with constraints](https://www.swi-prolog.org/pldoc/man?section=tabling-constraints)
- [SWI-Prolog Reference Manual: well-founded semantics](https://www.swi-prolog.org/pldoc/man?section=WFS)
- [SWI-Prolog Reference Manual: foreign language interface](https://www.swi-prolog.org/pldoc/man?section=foreign)
- [Picat official User's Guide](https://picat-lang.org/download/picat_guide_html/picat_guide.html)
- [Constraint Solving and Planning with Picat, official book PDF](https://www.picat-lang.org/picatbook2015/constraint_solving_and_planning_with_picat.pdf)
- [Tabling for Planning, official Picat tutorial](https://picat-lang.org/download/ecai14.pdf)

