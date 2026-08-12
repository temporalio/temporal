# Canonical Go IR v2 Design

Date: 2026-08-09

Status: Superseded package layout. The phase 1 design below is retained as
history; the implementation now lives at `tests/umpire2/protocol`, alongside
the renamed baseline at `tests/umpirev1`, as described in the Umpire 2 package
migration design.

## Decision

Phase 1 adds a parallel `tests/umpire/protocolv2` package. It does not modify,
deprecate, or replace the existing Umpire packages.

`protocolv2` is a compatibility-backed canonical protocol declaration: it uses
the existing entity factories, facts, lifecycles, and executable actions while
making their registration and planning relationships explicit in one compiled
model.

## Problem

The current declarations are split across several registries:

- `model.DefaultEntities` associates entity factories with fact subscriptions.
- Entity constructors own lifecycle declarations.
- `planner.DefaultModels` derives a structural lifecycle catalog.
- Per-entity `actionFor` functions map lifecycle edges to executable actions.
- The decoder's complete fact set is maintained separately from entity
  subscriptions.

These pieces work, but the compiler cannot validate them as one protocol. The
monitor and planner can agree on the entity set while action coverage and fact
registration still drift independently.

## Goals

- Define one canonical Go declaration for the existing protocol surface.
- Derive monitor registration, lifecycle planning, and edge-to-action planning
  from the compiled declaration.
- Preserve current runtime behavior and reuse current implementations.
- Validate cross-catalog references before a test starts.
- Keep the compiled module immutable and safe to reuse across concurrent tests.
- Prove behavioral equivalence with the existing APIs.

## Non-goals

Phase 1 does not:

- Add relations, properties, refinement, or a new type system.
- Replace `umpire.Entity`, `umpire.Fact`, `umpire.Lifecycle`, or `umpire.Action`.
- Change lifecycle semantics, fact decoding, action realization, or reconciliation.
- Canonicalize compound scenarios, fault actions, mutation variants, or rejection
  campaigns that are not part of the current edge-action planner.
- Migrate existing callers or remove existing registries.
- Fix inconsistencies discovered in current behavior. Those are recorded as
  follow-up findings after v2 reproduces them.

## Package boundary

The package is a new leaf in the dependency graph:

```text
common/testing/umpire       tests/umpire/fact
          ^                         ^
          |                         |
tests/umpire/model      tests/umpire/action
          ^                    ^
           \                  /
            tests/umpire/protocolv2
```

Existing packages never import `protocolv2`. This prevents import cycles and
ensures Phase 1 cannot alter v1 behavior accidentally. New tests opt into v2 by
importing it directly.

The initial file layout is:

```text
tests/umpire/protocolv2/
  protocol.go          declaration and compiled protocol types
  compile.go           normalization and validation
  default.go           Temporal's canonical default declaration
  monitor.go           ModelState registration adapter
  planner.go           lifecycle and action planning adapters
  compile_test.go      compiler validation tests
  default_test.go      canonical catalog tests
  equivalence_test.go  comparisons with the existing APIs
```

The package is intentionally one deep module. Callers depend on a small protocol
interface rather than knowing which existing registry supplies each part.

## Declaration model

The authoring form reuses existing Umpire types:

```go
type Declaration struct {
	Facts    []umpire.Fact
	Entities []EntityDeclaration
}

type EntityDeclaration struct {
	Type         umpire.EntityType
	New          umpire.EntityFactory
	Facts        []umpire.Fact
	Actions      []ActionBinding
	ActionGaps   []ActionGap
}

type ActionKey struct {
	Entity  umpire.EntityType
	From    string
	Event   string
	Hosting umpire.Hosting
}

type ActionBinding struct {
	Key    ActionKey
	Action umpire.Action
}

type ActionGap struct {
	Key    ActionKey
	Reason string
}
```

`Facts` is the complete decoder fact set. `EntityDeclaration.Facts` is the
entity's subscription subset. Keeping both in the same declaration lets the
compiler prove that every subscription is decodable.

Action keys are exact after compilation. Declaration helpers may expand one
action across several source states, but wildcard state matching is not retained
in the compiled IR. Exact keys keep resolution deterministic and make duplicate
bindings visible.

`ActionGap` records a deliberately unbound lifecycle edge. It preserves current
unsupported behavior while distinguishing it from an accidental omission. A
reason is mandatory.

Monitor-only entities have neither action bindings nor gaps. Phase 1 does not
require every monitored lifecycle to be actively drivable.

## Compiled module and interface

`Compile` defensively copies and indexes the declaration:

```go
func Compile(Declaration) (*Protocol, error)
func Default() (*Protocol, error)

func (p *Protocol) Register(*umpire.ModelState)
func (p *Protocol) Lifecycle(umpire.EntityType) (*umpire.Lifecycle, bool)
func (p *Protocol) Action(ActionKey) (umpire.Action, bool)
func (p *Protocol) PlanTo(
	entityType umpire.EntityType,
	target string,
	mode umpire.RouteMode,
	constraints umpire.Constraints,
	opts ...umpire.Option,
) (*umpire.Plan, error)
func (p *Protocol) PlanEdge(
	entityType umpire.EntityType,
	from string,
	event string,
	hosting umpire.Hosting,
) ([]umpire.Action, error)
```

The final implementation may keep catalog inspection methods unexported unless
tests or real consumers need them. `Register`, `PlanTo`, and `PlanEdge` are the
primary interface.

`Lifecycle` constructs a fresh entity through its factory and returns that
entity's lifecycle. A mutable lifecycle is never shared by the protocol. Planning
therefore receives a fresh structural model on each call while the compiled maps
remain immutable.

Returned actions are deep-copied for their mutable declaration fields, including
preconditions, effects, entries, footprints, and rejection metadata. Their
existing realizers remain opaque and are never invoked during compilation or
planning.

## Compilation and validation

Compilation returns deterministic, contextual errors and does not partially
construct a usable protocol. It validates:

1. Every decoder fact has a unique concrete Go type.
2. Every entity type is unique.
3. Every entity factory returns a non-nil entity whose `Type` matches its
   declaration.
4. Every entity subscription appears in the decoder fact set.
5. Every lifecycled entity passes the existing lifecycle validation.
6. Every action and gap references a declared lifecycled entity.
7. Every action and gap references a real `(from, event)` lifecycle edge.
8. Every action key is unique, and no gap overlaps an action key.
9. A binding's hosting is compatible with both the edge's `HostedIn` trait and
   the action's existing `Hosting` declaration.
10. A bound action declares an effect for the keyed entity type and event.
11. Every action gap has a non-empty reason.

Compilation does not require total action coverage. Instead, the default catalog
has an explicit coverage test for the entities supported by today's edge-action
planner. This avoids inventing active behavior for monitor-only entities.

Action planning requires concrete hosting when an edge has distinct standalone
and embedded realizations. Structural route planning may continue to use
`AnyHosting`.

## Default protocol

`Default` compiles one private declaration containing:

- The complete current decoder fact set.
- The five current default entity factories and their subscriptions.
- The current Workflow edge-action bindings.
- The current NexusOperation edge-action bindings.
- Explicit gaps for lifecycle edges the existing planner cannot realize
  atomically.

The declaration uses existing exported factories, fact types, action values, and
action constructors. It does not duplicate entity or action implementations.

Before writing the bindings, characterization tests capture the current resolver
matrix. The v2 declaration follows observed behavior even where current comments
or intended semantics appear inconsistent. Any discrepancy becomes a separate
finding rather than an implicit Phase 1 fix.

## Monitor adapter

`Register` performs the same operations as the current default registration:

1. Register the declaration's decoder facts.
2. Register each entity factory with its subscribed facts.

The adapter accepts an already compiled `Protocol`, so registration itself has no
validation failure path. It does not call `model.RegisterDefaultEntities`.

## Planner adapter

`PlanTo` gets a fresh lifecycle for the requested entity and delegates structural
route finding to the existing `umpire.PlanTo`.

`PlanEdge`:

1. Validates that the target edge is compatible with the requested hosting.
2. Plans a route from the lifecycle's initial state to `from`.
3. Resolves each route edge and the target edge through the protocol's action
   index.
4. Returns copied existing `umpire.Action` values.

This preserves the current planning algorithm while replacing per-entity switch
registries with data.

Errors identify the entity, source state, event, and hosting. Missing bindings
remain planning errors; they are never silently skipped by `PlanEdge`.

## Migration sequence

1. Add compiler fixtures and validation tests without importing current Temporal
   declarations.
2. Characterize the existing Workflow and NexusOperation action-resolution
   matrices through their public planning APIs.
3. Declare NexusOperation first. It exercises multiple source states, hosting
   restrictions, parameterized action constructors, and unsupported edges.
4. Add NexusOperation planning equivalence tests.
5. Declare Workflow and add its planning equivalence tests.
6. Add WorkflowRun, TaskQueue, and WorkflowTask as monitor-only entities.
7. Add the complete decoder fact set and monitor registration.
8. Add catalog and fact-routing equivalence tests.
9. Run targeted package tests with `-tags test_dep`.
10. Run `make lint-code`.

Each entity is added vertically: declaration, validation, adapter behavior, and
equivalence tests land together before the next entity is added.

## Test plan

### Compiler tests

- Accept a minimal valid monitor-only entity.
- Accept a valid lifecycled entity with a bound action.
- Reject duplicate facts, entities, action keys, and overlapping gaps.
- Reject nil factories and entity type mismatches.
- Reject subscriptions absent from the decoder set.
- Reject invalid lifecycle declarations.
- Reject bindings and gaps for unknown entities or edges.
- Reject hosting mismatches.
- Reject actions without a matching declared effect.
- Reject gaps without reasons.
- Verify returned lifecycles and actions do not share mutable protocol state.

### Default catalog tests

- Compare v2 entity types and subscriptions with `model.DefaultEntities`.
- Compare lifecycle initial states, states, events, edges, traits, and terminals
  with the current planner catalog.
- Verify every current active edge/hosting lookup is either bound or explicitly
  recorded as a gap.
- Verify all entity subscriptions are in the decoder fact set.

### Behavioral equivalence tests

- Compare v1 and v2 Workflow plans for every current edge.
- Compare v1 and v2 NexusOperation results for each source, event, and relevant
  hosting, including matching errors for unsupported combinations.
- Compare action names, kinds, hosting, preconditions, effects, entries, and
  footprints; opaque realizer values are not deep-compared.
- Register separate v1 and v2 model states and feed representative facts through
  both, then compare created entity types and lifecycle outcomes.

## Failure modes and trade-offs

### Partial catalog migration

The largest risk is a v2 declaration that represents only part of current
behavior. Equivalence matrices and explicit gaps make omissions visible. Existing
callers remain on v1 until the whole Phase 1 catalog passes.

### Mutable existing types

Existing lifecycles and actions were not designed as immutable IR nodes. The
protocol avoids shared lifecycle instances and copies action slices at its
boundary. It does not attempt a risky rewrite of realizer internals.

### Current inconsistencies

Characterization may expose mismatches between comments, action switches, and
actual planner results. Phase 1 records these without changing them. A later bug
fix can update v1 and v2 deliberately with dedicated tests.

### Performance and scale

Compilation is linear in facts, entities, lifecycle edges, and action bindings.
Indexes provide constant-time entity and action lookup. A tenfold larger catalog
increases startup work and memory linearly but does not change planning
complexity, which remains governed by the existing route finder.

### Concurrency and crashes

A protocol is published only after successful compilation. Its indexes are not
mutated afterward. Fresh lifecycles prevent concurrent plans from sharing FSM
state. Compilation or planning failure returns an error before any action is
driven.

### Security

The package introduces no network, persistence, or external input surface.
Existing action realizers retain their current authority. Compilation never
executes a realizer.

## Phase 1 exit criteria

Phase 1 is complete when:

- `tests/umpire/protocolv2` is the only new code package involved.
- No existing Umpire source file or API has changed.
- `Default` compiles the current decoder facts and five default entities.
- Monitor registration derives solely from the v2 declaration.
- Workflow and NexusOperation edge-action planning derives solely from the same
  declaration.
- Known unsupported combinations remain unsupported and are explicit gaps.
- Equivalence tests demonstrate no loss of current behavior.
- Targeted tests pass with `-tags test_dep` and `make lint-code` passes.

Adopting v2 in existing suites, deleting v1 registries, and adding relations,
properties, or refinement are separate follow-up phases.
