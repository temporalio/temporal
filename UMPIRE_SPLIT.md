# Umpire model boundaries

## Recommendation

Umpire should have **one authoritative model family, not one monolithic state space**.

The protocol should remain the single source of entity, action, relation, property, and refinement
vocabulary. From that source, Umpire should derive several bounded verification profiles:

1. a reusable **task-delivery foundation**;
2. separate **feature models** for Workflow, Activity, Nexus, Callback, and later features;
3. small **feature + foundation compositions** for cross-layer properties; and
4. focused **foundation submodels** for algorithms such as backlog acknowledgement, routing, and
   ownership fencing.

These are separate generated verification targets, not independently authored specifications. They
integrate through explicit interfaces and refinement mappings. The same action or relation must not
quietly acquire a different meaning in different targets.

This is the useful middle ground:

| Shape | Advantage | Failure mode | Verdict |
| --- | --- | --- | --- |
| One closed model containing every feature and infrastructure detail | Can state arbitrary global properties | State-space product grows quickly; feature work destabilizes infrastructure proofs; most explored combinations are irrelevant | Do not use as the default |
| Independent models with separately maintained vocabularies | Each model is small | They drift, duplicate semantics, and leave feature/infrastructure refinement unchecked | Do not use |
| One source with modules, contracts, profiles, and selected compositions | Reuses semantics while controlling each state space | Requires first-class composition and refinement in the verification IR | Recommended |

The user's intuition is directionally correct: infrastructure has the highest marginal value for
formal model generation. Task persistence, matching, ownership, acknowledgement, retry, and routing
contain exactly the identity, concurrency, and failure interleavings that testing samples poorly.
Feature models remain valuable, particularly for lifecycle and cross-entity safety, but they should
usually consume an abstract delivery contract rather than embed the matching implementation.

## What “deep enough” means

The model should be deep enough to distinguish executions that have different correctness outcomes,
but no deeper.

Include a boundary or state distinction when at least one of these is true:

- it survives a crash or ownership change;
- retry can repeat it, skip it, or create a new attempt;
- it lies on one side of an asynchronous or transactional boundary;
- another component may observe it before the next transition;
- it changes which actions are enabled;
- an identity, generation, route, or cardinality is needed by a property; or
- collapsing it would hide a known failure mode.

Do not include a distinction merely because it exists as a Go type, RPC, goroutine, cache entry,
metric, or protobuf field. Those are realization details unless they alter the abstract choices
above.

A practical stop rule is:

> If replacing a subsystem with a nondeterministic component satisfying a small contract preserves
> every property in the current profile, keep the subsystem behind that contract.

This gives four useful infrastructure depths:

| Depth | Content | Use |
| --- | --- | --- |
| L0: delivery contract | A semantic work obligation is pending, accepted, completed, retried, expired, or terminal | Interface consumed by feature models |
| L1: durable dispatch | History intent/transfer task, sync match or persisted backlog, reservation, start handshake, dispatch, acknowledgement, retry | Default foundation model; highest priority |
| L2: topology and routing | Logical/physical queues, partitions, forwarding, ownership generation, poller compatibility, build/version routing | Separate profiles and selected integration checks |
| L3: queue algorithms | Read/ack levels, backlog GC, fair/priority readers, batching and rate limits | Focused algorithm models, not the default global model |

L1 is the minimum useful infrastructure depth. L0 alone can verify feature lifecycles but cannot
find task loss, premature acknowledgement, duplicate acceptance, or stale-owner bugs. L2 and L3
should be added property by property; expanding every run to those depths would mostly multiply
states without strengthening the property under test.

## What the current model actually contains

The canonical source is `tests/umpire2/protocol`, lowered through the
[verification IR](./common/testing/umpire/verify/model.go) to
[`model.ir.json`](./tests/umpire2/genmodels/model.ir.json) and the generated TLA+, P, and Ivy files.
[`UMPIRE.md`](./UMPIRE.md) and [`UMPIRE_VERIFY.md`](./UMPIRE_VERIFY.md) correctly describe these as
derived views of one protocol.

At the time of this analysis, the generated snapshot has:

- 7 entity types, 6 relations, 70 actions, and 10 properties;
- 40 lifecycle actions marked unrealized;
- 63 recorded abstractions and 70 action refinement records;
- feature lifecycle coverage for Activity, Nexus Operation, Workflow, Workflow Run, and Workflow
  Task; and
- one lowered cross-feature regression action, `nexus.start_activity`, plus a strengthening property
  coupling the terminal Nexus Operation and Activity states.

Of the 10 properties, 3 are safety properties and 7 are quiescent-progress properties. Of the six
inventoried rule families, only `EntityProgress` and `NexusActivityLinkConsistency` are lowered;
`SpeculativeTaskCreation`, `NexusOperationClosure`, `NexusOperationTimeoutSemantics`, and
`WorkflowTaskStarvation` remain outside the property algebra. The regression inventory similarly
lowers only 1 of 24 actions. This is an honest and useful gap inventory, but it means a successful
generated run currently establishes only the selected lifecycle/relation slice. It does not yet
establish a matching, persistence, ownership, routing, or task-loss invariant.

`TaskQueue` currently has identity and observational data but no lifecycle. `WorkflowTask` has a
useful coarse path through created, added, stored, polled, discarded, and terminated, yet it is not
related to a task queue, workflow run, delivery attempt, poller, backlog position, or ownership
generation. Its transitions are abstract environment actions in the generated kernel. That means
the model recognizes a sync-versus-stored shape, but it does not explain why either path is safe.

The current IR is also flat. `verify.Model` has entities, relations, actions, properties,
abstractions, inventory, and refinements, but no module, imported interface, composition, or
verification profile. The existing refinement records connect verification actions to lifecycle and
regression vocabulary; they do not yet express that a concrete delivery execution refines a feature
level `WorkAccepted` step.

The generators faithfully expose that flat shape:

- [TLA+](./tests/umpire2/genmodels/tla/Umpire.tla) emits one global state and one `Next`
  disjunction;
- [P](./tests/umpire2/genmodels/p/Umpire.p) emits one `UmpireWorld` machine which steps itself
  through atomic actions; and
- [Ivy](./tests/umpire2/genmodels/ivy/Umpire.ivy) emits a flat set of actions and invariants rather
  than isolates with contracts.

The one-world P model is an appropriate equivalence target for the current atomic protocol. It is
not yet a model of Temporal's actors, queues, messages, or failure schedules. This distinction must
remain explicit; introducing P mailboxes without a refinement boundary would silently change the
source semantics.

## Runtime model versus verification profiles

The split does not require separate live Umpire monitors. A live test follows one concrete execution,
so retaining a unified runtime entity/relation graph is useful and does not create the formal
state-space cross-product. It lets a Nexus observation link to an Activity or Callback without
moving evidence between model instances.

Formal verification is different: it generates possible entities, choices, failures, and schedules.
Its cost grows with the product of enabled domains. Verification should therefore project the same
compiled protocol into a closed profile before lowering it to the Go interpreter, Ivy, TLA+, or P.
Coverage and planner scenarios may select the same profiles, but the facts and vocabulary remain
globally canonical.

In short: keep one runtime graph; split exploration and proof obligations.

## The foundation model

The first L1 infrastructure model should represent the durable History-to-Matching delivery path
shared by Workflow and Activity Tasks. Nexus should reuse the L0 work contract through a sibling
synchronous-relay implementation, not pretend to use that durable path. Temporal's
[History architecture](./docs/architecture/history-service.md) and
[Workflow lifecycle](./docs/architecture/workflow-lifecycle.md)
describe the key seam: History atomically persists mutable-state changes and History tasks; a
transfer processor eventually creates the corresponding Matching task; a worker polls Matching; and
History validates the start before the task is delivered. This is a transactional-outbox protocol,
not a single lifecycle edge. The current Matching
[`AddTask` interface](./service/matching/task_queue_partition_manager_interface.go) explicitly tries
synchronous matching before durable spooling, while the
[backlog reader](./service/matching/pri_task_reader.go) distinguishes retry-in-memory, respooling,
and acknowledgement outcomes. History also distinguishes definite write failure, ownership loss,
and an ambiguous write that must be resolved after reacquiring a fresh `RangeID` in
[`handleWriteErrorLocked`](./service/history/shard/context_impl.go). Those are semantic branches,
not implementation trivia.

“Task processing” needs one further boundary. The foundation owns creation, persistence, matching,
reservation, dispatch authorization, delivery, response correlation where Matching participates,
and retry. The worker's user-code computation is an environment step. Handling its completion in
History belongs to the Workflow or Activity feature model because it changes product state. Nexus's
[synchronous response relay](./docs/architecture/nexus.md)
is a foundation adapter because Matching holds the dispatch request while the worker responds. This
prevents “infrastructure” from becoming a second copy of every feature state machine.

The foundation is itself a composition, not a requirement for one giant infrastructure module:

- a **History outbox** module atomically creates valid delivery intent and keeps it eligible for
  submission until a declared terminal outcome;
- a **Matching delivery** module chooses sync match or durable backlog, reserves work, and routes it;
- a **start-authorization** module fences dispatch against current feature state; and
- a **response/acknowledgement** module correlates the outcome and decides retry or retirement.

The L1 foundation profile composes these modules because task-loss properties cross their seams.
Ownership, backlog arithmetic, and advanced routing can still be substituted by their contracts in
that profile and checked concretely in L2/L3 profiles.

### Foundation entities

Names should stay semantic rather than copy implementation structs:

- `WorkObligation`: feature-level requirement to perform a unit of work;
- `DeliveryTask`: durable or synchronously offered representation of that obligation;
- `DeliveryAttempt`: a particular try to reserve and start the work;
- `TaskQueue`: logical destination, task type, and namespace;
- `QueuePartition`: optional L2 physical routing/ownership unit;
- `Poller`: worker-side recipient with routing compatibility;
- `OwnerGeneration`: fencing identity scoped to an ownership domain; History shard `RangeID` and
  Matching queue `RangeID` are distinct sorts and must never be compared as one global counter; and
- `BacklogPosition`: an abstract ordered position used only by the queue-algorithm profile.

An attempt must be separate from an obligation. Retries preserve the semantic obligation but may
create a new attempt; deduplication and at-most-once acceptance are generally attempt- or
generation-scoped, not global statements about a feature operation.

There are also two retry identities. A delivery retry after transport failure may retry the same
feature attempt; an Activity retry after user-code failure creates a new feature attempt under the
same Activity obligation. The adapter must preserve both distinctions instead of using one generic
retry counter.

### Foundation relations

The important structure is relational:

- a delivery task realizes exactly one work obligation;
- a task belongs to exactly one logical queue and task type;
- an attempt belongs to exactly one delivery task;
- a reservation, if present, is owned by one compatible poller;
- a physical queue is owned under one current generation;
- a persisted task occupies at most one live backlog position; and
- a dispatch authorization identifies the feature attempt it authorizes.

The model need not carry arbitrary payloads. Namespace, queue, task type, attempt identity, route
class, and owner generation are enough when those are the fields used by the properties.

### Foundation transitions

The L1 action vocabulary should cover observable semantic choices, not method calls:

1. attempt to persist a feature transition and its work obligation atomically, with distinct
   success, definite-failure, and ambiguous-outcome branches;
2. after an ambiguous result, reacquire/fence ownership and resolve the persisted outcome before a
   semantic retry;
3. offer the delivery task for synchronous matching;
4. fall back to or read from the durable backlog;
5. reserve an eligible task for a compatible poller;
6. request that the relevant owner authorize dispatch (the History start handshake for Workflow and
   Activity, or the profile's corresponding authorization for another task type);
7. accept or reject that authorization because of current feature state, duplicate attempt, or stale
   generation;
8. dispatch an authorized task;
9. acknowledge a completed delivery attempt;
10. release, respool, or retry after a failure; and
11. expire or terminally invalidate work according to the feature contract.

Crash, timeout, RPC loss, and ownership transfer should normally be nondeterministic outcomes on
these actions. Model a concrete failure mechanism only when its ordering or persistence semantics is
itself under examination.

### Foundation properties

The initial model is useful if it can state and falsify at least these properties:

- **No split commit:** a committed feature transition and its required History task appear together,
  or neither appears.
- **Ambiguous commit is resolved:** an indeterminate persistence response cannot cause the semantic
  transition to be blindly repeated under the same ownership view.
- **No phantom dispatch:** every dispatched task is backed by a valid feature obligation and any
  authorization required by that task type.
- **No premature loss:** a persisted eligible task remains live until accepted, terminally
  invalidated, or expired by declared policy.
- **Single accepted start:** two live attempts cannot both receive start authorization for the same
  scoped feature attempt.
- **Failed start is not success:** rejection, timeout, or transport failure during the start
  handshake cannot acknowledge or silently drop the task.
- **Retry preserves obligation:** a retry may replace an attempt but cannot change the work's
  feature identity or destination.
- **Backlog safety:** acknowledgement and garbage collection cannot pass a backlog position without
  a recorded dispatch, invalidation, expiry, or other declared terminal drop outcome.
- **Ownership fencing:** a persistence mutation guarded by an old owner generation cannot commit
  after the new generation is authoritative.
- **Path equivalence:** sync match and persisted-backlog delivery refine the same abstract delivery
  contract.
- **Routing isolation:** work cannot cross namespace, task-queue, task-type, or declared
  build/version compatibility boundaries.
- **No resurrection:** terminal or invalid feature work cannot re-enter delivery.

Progress claims require explicit assumptions. “Every eligible task is eventually dispatched” is
false without some combination of a compatible poller, continued ownership, successful storage and
RPCs, retry, and fair scheduling. The model should name those assumptions and otherwise make the
claim bounded or quiescent, following the qualification already used by Umpire.

## Feature models

Feature models should own product semantics: the legal lifecycle, operation identity, retry policy,
terminal outcomes, and relations to other feature entities. They should not own the implementation
of matching.

Each feature instead adapts its vocabulary to the L0 delivery interface:

| Feature event | Foundation meaning |
| --- | --- |
| Workflow Task scheduled | Create a workflow-task work obligation |
| Activity scheduled or retried | Create an activity-attempt work obligation |
| Matching start accepted | Accept the corresponding feature attempt |
| Worker completion/failure/timeout | Resolve the attempt according to feature policy |
| Nexus handler dispatch | Create/route a handler work obligation and wait for its permitted response |
| Callback dispatch | Create a callback delivery obligation when callback transport enters scope |

This adapter is a refinement mapping, not just a name mapping. It must establish which feature state
authorizes the work, how identity is preserved, which concrete steps may stutter at the feature
level, and which completion changes the feature state.

The L0 interface must remain smaller than Matching. Workflow and Activity can refine it through the
durable History/Matching path; Nexus can refine it through Matching's synchronous request/response
path. Callback HTTP delivery is likely a sibling delivery implementation with different retry and
acknowledgement semantics. It should implement the common obligation/outcome contract where that is
true, not inherit Matching-specific backlog states merely to reuse a model.

The current Nexus/Activity relation is a good example of a property that belongs above an
individual feature but below the whole server. It should be checked in a small `Nexus + Activity`
composition. If task delivery becomes relevant to the counterexample, add the abstract delivery
contract first and use a targeted `Nexus + Activity + Delivery` profile; do not enable every
Workflow and Callback state in the same run.

## How the models integrate

The integration should be explicit in the source model:

```text
                         shared semantic vocabulary
                                   |
                    +--------------+--------------+
                    |                             |
          task-delivery foundation         feature lifecycles
                    |                    /     |      |      \
              exports contract       workflow activity nexus callback
                    |                    \     |      |      /
                    +----------- refinement adapters --------+
                                   |
                         selected compositions
```

The foundation exports actions such as `OfferWork`, `AcceptStart`, `RejectStart`, `Dispatch`, and
`ResolveAttempt`, plus guarantees about identity, routing, and cardinality. Feature models export
when an obligation may be created and when an attempt is still valid. A composition connects those
actions and discharges both sides of the contract.

Cross-layer properties then become precise:

- a committed feature transition creates exactly the required work obligation;
- dispatched work corresponds to the current feature attempt;
- accepted/completed delivery enables only a legal feature transition;
- retry replaces the permitted attempt without duplicating the semantic obligation; and
- cancellation or terminal completion invalidates delivery according to an explicit race policy.

| Property scope | Default target |
| --- | --- |
| No task loss, duplicate authorization, stale-owner mutation, premature ack, or route crossing | Foundation module/profile |
| Legal Activity, Workflow, Nexus, or Callback lifecycle | Individual feature profile over L0 assumptions |
| Scheduled work corresponds to the delivered/completed feature attempt | Feature + delivery composition |
| Nexus/Activity terminal and reciprocal-link consistency | Nexus + Activity composition |
| Callback belongs to the right operation/handler run and cannot outlive its policy | Callback + Nexus/Workflow composition |

The model family needs three kinds of checks:

1. **Module checks:** prove or explore a foundation or feature under the assumptions of its
   imported interface.
2. **Contract/refinement checks:** establish that an implementation module provides the guarantees
   required by the abstract interface.
3. **Composition checks:** explore a small concrete combination to catch bad adapters and emergent
   cross-module interleavings.

Assume-guarantee reasoning reduces the state space, but it can preserve a shared mistaken
assumption. The composed checks and Umpire's existing live refinement/reconciliation path are
therefore essential.

## Fit by verification backend

The three generators should consume the same selected profile, but they need not generate the same
proof shape.

| Backend | Best Umpire role | Model shape | Main caution |
| --- | --- | --- | --- |
| Ivy | Foundation safety and contract proofs | Relational state, quantified identity/cardinality invariants, isolates with imported/exported actions and assume-guarantee contracts | Broad arithmetic-heavy queue algorithms may leave Ivy's supported decidable fragment; abstract order/prefixes or isolate them |
| TLA+ | Reference semantics, integration, refinement, fairness and liveness | Modules composed over an explicit action interface; small TLC configurations; refinement mappings between L1 and L0 | Shared-variable composition and unbounded liveness need care; every finite result must retain bounds and fairness assumptions |
| P | Actorized execution refinement and adversarial scheduling | History shard, Matching partition, poller, and synchronous relay as machines; monitors state safety/liveness | Mailboxes change the current atomic semantics, so actorization must be a separate refinement target rather than a generator accident |

### Ivy

Ivy is the strongest immediate fit for the infrastructure safety kernel. Its isolate model is
designed to verify a component against the contracts of objects it calls, and its `require`/`ensure`
style supports explicit ownership of assumptions and guarantees, as described by Ivy's
[isolate and specification guide](https://microsoft.github.io/ivy/examples/specification.html) and
[language reference](https://microsoft.github.io/ivy/language.html). The task-delivery foundation is
mostly opaque identities, relations, cardinality, and action preservation: a natural shape for Ivy.
This advantage is conditional: Ivy rejects verification conditions outside its
[supported decidable fragments](https://microsoft.github.io/ivy/decidability.html). An Ivy result
can establish parameterized safety over uninterpreted identities when those proof obligations pass;
it is not merely another finite one-ID exploration.

The generated Ivy should therefore become a set of isolates rather than one flat file. A delivery
isolate can prove no phantom dispatch and single accepted start while assuming a feature-validity
interface; feature isolates can prove that they meet that interface without importing backlog
internals. Algorithm-specific integer or sequence reasoning should be abstracted to an order/prefix
contract or verified in a smaller isolate.

### TLA+

TLA+ should remain the semantic reference and the place for small integrated behaviors. It makes
atomicity and fairness choices visible, supports specification composition, and can express that the
L1 dispatch protocol refines the L0 delivery contract through stuttering and auxiliary/history
variables. There are two distinct operations here. At one abstraction level, splitting a TLA+
specification into component modules and composing their behaviors is often an organizational
choice, with shared-state subtleties. Between L1 and L0, the split is semantic and requires an
actual refinement mapping. Lamport covers atomicity, fairness, composition, and interface
refinement in *[Specifying Systems](https://lamport.azurewebsites.net/tla/book-02-02-28.pdf)*
(Chapters 7, 8, and 10); the current
[safety-proof guide](https://lamport.azurewebsites.net/tla/proving-safety.pdf) gives the concrete
`Spec => HL!Spec` refinement form. TLC can explore finite instances; inductive safety requires a
separate proof obligation rather than a larger finite run.

This is the best backend for questions such as whether a retry/ownership-transfer interleaving can
strand an obligation, provided the profile declares the poller, failure, and fairness assumptions.
It should not compensate for a missing source-level module system by accumulating one ever-growing
`Next` relation.

### P

P is most valuable after the abstract foundation contract is stable. Its machines, asynchronous
events, [module system](https://p-org.github.io/P/manual/modulesystem/), and
[monitor specifications](https://p-org.github.io/P/manual/monitors/) align well with History,
Matching partitions, pollers, and responses. That actorized P model can find ordering bugs which the
current atomic world machine cannot express.

The ordinary P checker explores the requested finite test scenarios and schedule count; a run with
no bug is therefore bounded evidence, not an unqualified proof. P's compositional argument comes
from replacing dependencies with abstractions and checking trace-refinement obligations, as
formalized in the [ModP paper](https://ankushdesai.github.io/assets/papers/modp.pdf), not from merely
placing machines in separate source files.

Keep both meanings explicit:

- `p-semantic`: the current one-world lowering used to cross-check the shared atomic kernel; and
- `p-actors`: a separately selected execution refinement whose machines and message steps must
  refine that kernel or its delivery contract.

## Required source-model capabilities

The current verification IR cannot represent the recommended split without adding first-class
structure. The conceptual additions are:

- `Module`: owned entities, relations, actions, properties, and internal state;
- `Interface`: imported/exported actions plus assumptions and guarantees;
- `RefinementMap`: concrete steps, stuttering steps, identity mapping, and abstract effects;
- `Composition`: modules and adapters instantiated together; and
- `VerificationProfile`: selected roots, properties, bounds, fairness, failure policy, and backend
  requirements.

A profile is a projection of the canonical protocol, not permission to omit dependencies silently.
Generation should reject a selected property when the profile excludes an entity, action, relation,
assumption, or refinement obligation it needs.

Projection also needs defined hiding semantics. An excluded internal action may refine to stuttering;
an action controlled by an omitted peer may need to remain as a contract-constrained environment
action. Simply deleting either from `Next` can make safety vacuous or make progress impossible.

Illustrative targets are:

```text
foundation-delivery-safety
foundation-backlog-ack
foundation-ownership-fencing
feature-workflow
feature-activity
feature-nexus
feature-callback
integration-workflow-delivery
integration-activity-delivery
integration-nexus-activity
integration-nexus-delivery
```

Each target should have independent bounds. For example, an ownership proof may need two owners but
only one queue; a duplicate-start check needs two attempts; a routing isolation check needs two
namespaces or queues; a feature lifecycle check may need no physical partition at all. A single
global identity bound wastes states and can accidentally make an invariant vacuous.

For the first non-vacuous finite profiles, use at least two obligations or attempts for duplicate
and aliasing properties, two owner generations for fencing, two queues or namespaces for isolation,
and competing pollers where reservation matters. These are test-design minima, not a cutoff theorem.
TLC exhaustiveness is only for the constants in a particular
[finite model](https://lamport.azurewebsites.net/tla/model-popup.html), and P's `-s` option controls
the number of schedules explored by the
[checker](https://p-org.github.io/P/getstarted/usingP/). Unless Umpire proves a profile-specific
cutoff, results must not be extrapolated from those bounds to arbitrary cluster size. Report a claim
as parameterized only when an Ivy or inductive proof actually establishes it.

## Failure modes of the split

| Failure mode | Consequence | Required control |
| --- | --- | --- |
| Two modules assume the same unproved fact | Both pass while their composition is invalid | Give each assumption an owning guarantee and reject undischarged contracts |
| Adapter maps the wrong identity or terminal state | Module checks pass but the system-level claim is false | Generate explicit refinement obligations and run small concrete compositions |
| Projection silently deletes an interfering action | Safety passes vacuously or liveness changes | Validate dependency closure and make hidden/environment actions explicit |
| One-identity bounds remove contention | Cardinality, routing, and ownership properties pass trivially | Define per-property minimum non-vacuous bounds and mutation tests |
| Fairness is implicit | A liveness result claims more than the system guarantees | Record fairness and availability assumptions in the profile and result |
| All exporters share a lowering bug | Cross-backend agreement repeats the same error | Retain the Go interpreter, tiny reachable-world equivalence, and seeded semantic mutations |
| Formal actions have no live realization/observation | The abstraction is sound but says little about Temporal | Preserve Umpire refinement, causal-footprint, and observation-gap reporting |
| A common interface is made too feature-specific | Nexus or Callback inherits false Matching semantics | Keep L0 minimal and allow sibling implementations behind it |

## What should remain outside these models

Unless a property explicitly depends on them, exclude:

- arbitrary payload bytes and most protobuf fields;
- exact goroutine, channel, cache, and RPC-handler structure;
- logging, metrics, tracing transport, and error text;
- precise timeout durations rather than timeout ordering;
- every task-queue kind, routing policy, or deployment-version combination in one target;
- performance, capacity, and 10x-load claims; and
- implementation-specific fairness heuristics in the default foundation profile.

Some of these deserve specialized tests or dedicated models. They should not inflate the semantic
acceptance model merely because Umpire can observe them.

## Boundary decisions for the first profile

The model family can be designed now, but the first L0/L1 profile must make these choices explicit:

- **Acceptance linearization:** for Workflow and Activity Tasks, treat the successful History
  start-authorization transaction as `AcceptStart`; Matching reservation and worker receipt remain
  distinct surrounding steps. If Temporal intends a different product guarantee, change the L0
  vocabulary instead of hiding the difference in a generator.
- **Ownership domains:** model History shard and Matching task-queue generations separately, with a
  mapping only where an action carries both. A single global “owner epoch” would prove a system
  Temporal does not implement.
- **Speculative Workflow Tasks:** keep them explicitly out of the first durable L1 profile, then add
  a sibling direct-dispatch-with-durable-fallback profile. The current inventory already identifies
  `SpeculativeTaskCreation` as an uncovered rule; silently treating it as an ordinary persisted task
  would erase its defining race.
- **Terminal drops:** enumerate which stale, expired, canceled, or otherwise invalid tasks may
  advance acknowledgement. “No task loss” is meaningful only after those permitted drops are part
  of the contract.
- **Live observation:** identify which IDs and outcomes can be reconstructed from Umpire facts before
  claiming that a formal action refines production behavior. A proof-only ghost identity may be
  useful internally, but it cannot discharge live conformance by itself.

These are contained design decisions, not reasons to merge all features into the foundation model.

## Suggested order of work

1. Define L0 `WorkObligation`/`DeliveryAttempt` semantics and the feature adapter contract.
2. Derive an L1 foundation profile for one queue, bounded obligations and attempts, sync/backlog
   choice, start acceptance/rejection, acknowledgement, retry, and crash/ownership outcomes.
3. Seed incorrect transitions and confirm that the Go interpreter plus Ivy and TLA+ expose the
   intended counterexamples.
4. Connect Workflow and Activity adapters; check each feature alone and in a small concrete
   composition.
5. Connect Nexus dispatch and preserve the existing lowered `nexus.start_activity` action plus its
   reciprocal-link and terminal-strengthening properties.
6. Add L2 ownership, partition, and routing profiles one property at a time.
7. Introduce `p-actors` and check that its externally visible deliveries refine the L0/L1 model.
8. Add L3 acknowledgement/fairness algorithms only where production invariants demand their exact
   structure.

The success criterion is not that every backend accepts a larger model. Each layer should detect a
seeded fault unique to that layer, and the integration profiles should detect a deliberately wrong
feature/foundation adapter. Without those tests, splitting may improve runtime while merely moving
blind spots between models.

## Answers to the design questions

- **How deep should Umpire go?** Through the durable dispatch and start-handshake protocol (L1) by
  default. Add ownership/routing and queue algorithms as focused profiles when a stated invariant
  requires them. Stop before implementation mechanics that do not change enabled actions or
  correctness outcomes.
- **Same model or separate models?** One source model and semantic vocabulary; multiple generated
  modules and bounded profiles. Do not use one universal state space or independently maintained
  specifications.
- **How do they integrate?** Through an explicit work-delivery interface, feature adapters,
  refinement maps, and selected composed verification targets.
- **Where is Ivy/P/TLA+ generation most valuable?** First in infrastructure safety and concurrency.
  Ivy is especially promising for relational safety contracts, TLA+ for integrated refinement and
  fairness, and P for a deliberately actorized execution refinement. Feature lifecycle generation
  remains useful but should usually sit above the delivery contract.

## Primary sources

- Temporal's [History Service architecture](./docs/architecture/history-service.md)
  documents the atomic mutable-state/task transaction, transfer processing, acknowledgement,
  ownership generations, and transactional-outbox relationship with Matching.
- Temporal's [Workflow lifecycle](./docs/architecture/workflow-lifecycle.md)
  traces Workflow and Activity Tasks through History, Matching, polling, and the start handshake.
- Temporal's [Matching Service architecture](./docs/architecture/matching-service.md) describes
  logical queues, partitions, forwarding, and reassignment; the current
  [`AddTask`](./service/matching/task_queue_partition_manager_interface.go),
  [backlog reader](./service/matching/pri_task_reader.go), and
  [History ownership-error](./service/history/shard/context_impl.go) paths supply the deeper
  implementation evidence used above.
- Temporal's [Nexus architecture](./docs/architecture/nexus.md)
  describes Nexus task dispatch through Matching and its synchronous response path.
- Ivy's [specification and isolate guide](https://microsoft.github.io/ivy/examples/specification.html),
  [language reference](https://microsoft.github.io/ivy/language.html), and
  [decidability guide](https://microsoft.github.io/ivy/decidability.html) describe isolates, action
  contracts, assume-guarantee reasoning, invariant preservation, and the logic-fragment constraint.
- P's [module system](https://p-org.github.io/P/manual/modulesystem/),
  [monitor documentation](https://p-org.github.io/P/manual/monitors/), and
  [checker guide](https://p-org.github.io/P/getstarted/usingP/) describe machine composition,
  abstraction replacement, safety/liveness observers, and bounded schedule exploration. The
  [ModP paper](https://ankushdesai.github.io/assets/papers/modp.pdf) formalizes module composition,
  trace refinement, and assume-guarantee validation.
- Lamport's *[Specifying Systems](https://lamport.azurewebsites.net/tla/book-02-02-28.pdf)* covers
  abstraction level, action granularity, fairness, specification composition, and interface
  refinement. The [safety-proof guide](https://lamport.azurewebsites.net/tla/proving-safety.pdf)
  shows refinement mappings as proof obligations, and the
  [safety/liveness note](https://lamport.azurewebsites.net/tla/safety-liveness.pdf) defines weak and
  strong fairness rather than leaving progress assumptions implicit.
