# Umpire model boundaries

## Recommendation

Umpire should have **one authoritative model family, not one monolithic state space**.

The protocol should remain the single source of entity, action, relation, property, and refinement
vocabulary. From that source, Umpire should derive several bounded semantic verification targets:

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
| One source with modules, contracts, targets, and selected compositions | Reuses semantics while controlling each state space | Requires first-class composition and refinement in the verification IR | Recommended |

The user's intuition is directionally correct: infrastructure has the highest marginal value for
formal model generation. Task persistence, matching, ownership, acknowledgement, retry, and routing
contain exactly the identity, concurrency, and failure interleavings that testing samples poorly.
Feature models remain valuable, particularly for lifecycle and cross-entity safety, but they should
usually consume an abstract delivery contract rather than embed the matching implementation.

## Organizational ownership and Conway's law

The model family should follow stable capability boundaries as well as technical boundaries.
History, Matching, Workflow, Activity, Nexus, and Callback evolve under different ownership and
release pressures. A model which centralizes their semantics behind one review queue will either
become a coordination bottleneck or drift from the systems it claims to describe.

"One authoritative model family" therefore means one compiled semantic graph, not one source file,
package, or central modeling team. Declarations should be federated across capability-owned
packages and joined by the protocol compiler. The stable ownership identifiers are capabilities,
not current organization or team names:

| Capability owner | Owns in the model | Important boundary |
| --- | --- | --- |
| `history` | Durable mutation/outbox semantics, shard fencing, and start-authorization mechanics | Exports durable intent and authorization outcomes to delivery and feature adapters |
| `matching` | Sync match, durable backlog, reservation, routing, dispatch, response correlation, and acknowledgement | Consumes valid intent; exports delivery outcomes without owning feature lifecycle policy |
| `workflow` | Workflow and Workflow Task product semantics and their delivery adapter | Consumes the delivery contract and History authorization mechanism |
| `activity` | Activity lifecycle, attempt/retry policy, result races, and its delivery adapter | Consumes the delivery contract without inheriting Workflow-specific policy |
| `nexus` | Nexus Operation lifecycle, handler semantics, operation-side relations, and Nexus adapters | Uses the common work contract only where its synchronous relay preserves that contract |
| `callback` | Callback lifecycle, transport-specific retry/acknowledgement policy, and callback-side adapters | Integrates with Nexus or Workflow through an explicit relation and adapter contract |
| `umpire-framework` | Protocol compiler, verification IR, projection, exporters, and result plumbing | Supplies modeling machinery but does not own another capability's semantics |

An illustrative source layout is therefore federated even though compilation produces one graph:

```text
tests/umpire2/protocol/
    history/                 owner: history
    matching/                owner: matching
    workflow/                owner: workflow
    activity/                owner: activity
    nexus/                   owner: nexus
    callback/                owner: callback
    integration/nexusactivity/  co-owners: nexus, activity
```

Interfaces should live with their provider declaration or in a contract package with an explicit
provider owner. A catch-all shared package with no semantic owner merely recreates central
ownership under another name.

The repository maps those stable identifiers to current reviewers through
[`CODEOWNERS`](./.github/CODEOWNERS). Reorganizations update that mapping without renaming semantic
modules, interfaces, targets, or generated artifacts. Capability ownership is organizational;
runtime values such as `OwnerGeneration` continue to mean persistence fencing and are unrelated.

Ownership rules should be enforceable rather than ceremonial:

- every module has one capability owner responsible for its declarations, local properties,
  adapters, and non-vacuous verification target;
- every interface identifies a provider owner and its consumer owners; the provider owns its
  guarantees, while each consumer owns declaring the assumptions and adapter by which it uses
  them; generation requires every such assumption to be discharged by a provider guarantee;
- integration targets are co-owned by every capability whose behavior they compose, with no
  fallback assumption that an Umpire infrastructure owner supplies missing product semantics;
- a change confined behind an unchanged interface can use the owner's local checks, while an
  exported action, identity, assumption, or guarantee change must run affected consumer and
  integration targets and require their owners' review;
- common compiler or exporter changes run all affected targets, but framework ownership does not
  grant authority to redefine capability semantics; and
- generation rejects unowned modules, unknown consumers, undischarged cross-owner assumptions, and
  integration targets without explicit co-owners.

Generated manifests should retain stable owner IDs, interface providers and consumers, and the
targets exercised. They should not embed mutable team names. This makes responsibility visible in
counterexamples and CI results while leaving organization mapping to repository policy.

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
> every property in the current target, keep the subsystem behind that contract.

This gives four useful infrastructure depths:

| Depth | Content | Use |
| --- | --- | --- |
| L0: delivery contract | A semantic work obligation is pending, accepted, completed, retried, expired, or terminal | Interface consumed by feature models |
| L1: durable dispatch | History intent/transfer task, sync match or persisted backlog, reservation, start handshake, dispatch, acknowledgement, retry | Default foundation model; highest priority |
| L2: topology and routing | Logical/physical queues, partitions, forwarding, ownership generation, poller compatibility, build/version routing | Separate targets and selected integration checks |
| L3: queue algorithms | Read/ack levels, backlog GC, fair/priority readers, batching and rate limits | Focused algorithm models, not the default global model |

L1 is the minimum useful infrastructure depth. L0 alone can verify feature lifecycles but cannot
find task loss, premature acknowledgement, duplicate acceptance, or stale-owner bugs. L2 and L3
should be added property by property; expanding every run to those depths would mostly multiply
states without strengthening the property under test.

## What the current model actually contains

The canonical source is `tests/umpire2/protocol`, lowered through the
[verification IR](./common/testing/umpire/verify/model.go) to
the compatibility target's
[`model.ir.json`](./tests/umpire2/genmodels/protocol-atomic/model.ir.json) and generated TLA+, P,
and Ivy files.
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

Before this split, the IR was also flat. `verify.Model` has entities, relations, actions, properties,
abstractions, inventory, and refinements, but no module, imported interface, composition, or
verification target. The structural `ModelFamily` layer now projects those closed models while the
existing refinement records continue to connect verification actions to lifecycle and regression
vocabulary; later delivery adapters must express that a concrete delivery execution refines a
feature-level `WorkAccepted` step.

The generators faithfully expose that flat shape:

- [TLA+](./tests/umpire2/genmodels/protocol-atomic/tla/Umpire.tla) emits one global state and one `Next`
  disjunction;
- [P](./tests/umpire2/genmodels/protocol-atomic/p/Umpire.p) emits one `UmpireWorld` machine which steps itself
  through atomic actions; and
- [Ivy](./tests/umpire2/genmodels/protocol-atomic/ivy/Umpire.ivy) emits a flat set of actions and invariants rather
  than isolates with contracts.

The one-world P model is an appropriate equivalence target for the current atomic protocol. It is
not yet a model of Temporal's actors, queues, messages, or failure schedules. This distinction must
remain explicit; introducing P mailboxes without a refinement boundary would silently change the
source semantics.

## Runtime model versus verification targets

The split does not require separate live Umpire monitors. A live test follows one concrete execution,
so retaining a unified runtime entity/relation graph is useful and does not create the formal
state-space cross-product. It lets a Nexus observation link to an Activity or Callback without
moving evidence between model instances.

Formal verification is different: it generates possible entities, choices, failures, and schedules.
Its cost grows with the product of enabled domains. Verification should therefore project the same
compiled protocol into a closed target before lowering it to the Go interpreter, Ivy, TLA+, or P.
Coverage and planner scenarios may select the same targets, but the facts and vocabulary remain
globally canonical.

In short: keep one runtime graph; split exploration and proof obligations.

The existing command already uses `profile` for the `smoke` and `nightly` execution budgets. Keep
that meaning. A **verification target** selects semantic roots, closure, properties, bounds, and
backend requirements; an **execution profile** selects schedule count, exploration depth, timeout,
and CI cadence. `cmd/umpire-genmodels` should therefore gain `-target` while retaining `-profile`
for `smoke` or `nightly`. A run is identified by both values, for example
`-target foundation-delivery-safety -profile smoke`.

`generate` and `check-generated` always iterate every declared target so the checked-in tree is
complete. `-target` applies only to `verify`, which accepts one target or `all` and defaults to
`all`. Consequently
`make umpire-verify-smoke` and `make umpire-verify-nightly` run every target with the corresponding
execution budget, while backend requirements may cause a target to select only the sound backends
for its proof shape.

Generated artifacts should be target-scoped:

```text
tests/umpire2/genmodels/
    manifest.json                 target index and hashes
    protocol-atomic/              compatibility target for the current flat kernel
        manifest.json
        model.ir.json
        tla/
        p/
        ivy/
    foundation-delivery-safety/
        ...
```

The top-level manifest inventories targets. Each target manifest records its semantic owner or
co-owners, interface providers and consumers, selected roots and properties, target-specific bounds,
backend requirements, and the source-model hash. `smoke` and `nightly` results remain external run
artifacts and must identify both the target and execution profile. During migration,
`protocol-atomic` must produce behavior equivalent to the current generated snapshot so the split
does not silently change the atomic kernel.

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

- a **History outbox** module, owned by `history`, atomically creates valid delivery intent and
  keeps it eligible for submission until a declared terminal outcome;
- a **Matching delivery** module, owned by `matching`, chooses sync match or durable backlog,
  reserves work, and routes it;
- a **History start-authorization** module, owned by `history`, fences and commits dispatch against
  current persisted state;
- feature-validity adapters, owned by `workflow`, `activity`, or another applicable feature,
  define which feature attempt that authorization may accept; and
- a **response/acknowledgement** module, owned by `matching`, correlates the outcome and decides
  retry or retirement under the exported feature policy.

The L1 foundation target composes these modules because task-loss properties cross their seams.
Ownership, backlog arithmetic, and advanced routing can still be substituted by their contracts in
that target and checked concretely in L2/L3 targets.

### Foundation entities

Names should stay semantic rather than copy implementation structs:

- `WorkObligation`: feature-level requirement to perform a unit of work;
- `DeliveryTask`: durable or synchronously offered representation of that obligation;
- `DeliveryAttempt`: a particular try to reserve and start the work;
- `TaskQueue`: logical destination, task type, and namespace;
- `QueuePartition`: optional L2 physical routing/ownership unit;
- `Poller`: worker-side recipient with routing compatibility;
- `OwnerGeneration`: optional L2 fencing identity scoped to an ownership domain; History shard
  `RangeID` and Matching queue `RangeID` are distinct sorts and must never be compared as one global
  counter; and
- `BacklogPosition`: an abstract ordered position used only by the L3 queue-algorithm target.

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
- at L2, a physical queue is owned under one current generation;
- at L3, a persisted task occupies at most one live backlog position; and
- a dispatch authorization identifies the feature attempt it authorizes.

The model need not carry arbitrary payloads. Namespace, queue, task type, attempt identity, route
class, and owner generation are enough when those are the fields used by the properties.

### Foundation transitions

The L1 action vocabulary should cover observable semantic choices, not method calls:

1. attempt to persist a feature transition and its work obligation atomically, with distinct
   success, definite-failure, and ambiguous-outcome branches;
2. after an ambiguous result, obtain a fresh ownership view and resolve the persisted outcome before
   a semantic retry; L1 treats freshness as a contract, while L2 refines it with concrete owner
   generations and fencing;
3. offer the delivery task for synchronous matching;
4. fall back to or read from the durable backlog;
5. reserve an eligible task for a compatible poller;
6. request that the relevant owner authorize dispatch (the History start handshake for Workflow and
   Activity, or the target's corresponding authorization for another task type);
7. accept or reject that authorization because of current feature state or a duplicate attempt; L2
   adds rejection under a stale owner generation;
8. dispatch an authorized task;
9. acknowledge a completed delivery attempt;
10. release, respool, or retry after a failure; and
11. expire or terminally invalidate work according to the feature contract.

Crash, timeout, RPC loss, and ownership transfer should normally be nondeterministic outcomes on
these actions. Model a concrete failure mechanism only when its ordering or persistence semantics is
itself under examination.

### Foundation properties

The initial L1 target is useful if it can state and falsify at least these properties without
embedding concrete owner-generation or queue-level arithmetic:

- **No split commit:** a committed feature transition and its required History task appear together,
  or neither appears.
- **Ambiguous commit is resolved:** an indeterminate persistence response cannot cause the semantic
  transition to be blindly repeated under the same ownership view.
- **No phantom dispatch:** every dispatched task is backed by a valid feature obligation and any
  authorization required by that task type.
- **Coarse retirement safety:** an eligible task remains live until the delivery contract records
  acceptance, terminal invalidation, expiry, or another enumerated terminal outcome.
- **Single accepted start:** two live attempts cannot both receive start authorization for the same
  scoped feature attempt.
- **Failed start is not success:** rejection, timeout, or transport failure during the start
  handshake cannot acknowledge or silently drop the task.
- **Retry preserves obligation:** a retry may replace an attempt but cannot change the work's
  feature identity or destination.
- **Path equivalence:** sync match and persisted-backlog delivery refine the same abstract delivery
  contract.
- **Destination isolation:** work cannot change its namespace, logical task queue, or task type while
  moving through the abstract delivery path.
- **No resurrection:** terminal or invalid feature work cannot re-enter delivery.

The deeper targets refine those contracts rather than duplicating their machinery in L1:

- **L2 ownership fencing:** a persistence mutation guarded by an old owner generation cannot commit
  after the new generation is authoritative.
- **L2 routing isolation:** forwarding, partition ownership, poller compatibility, and build/version
  routing cannot move work across a declared route boundary.
- **L3 backlog safety:** read/ack advancement and garbage collection cannot pass a backlog position
  without a recorded dispatch, invalidation, expiry, or other declared terminal drop outcome.

Progress claims require explicit assumptions. “Every eligible task is eventually dispatched” is
false without some combination of a compatible poller, continued ownership, successful storage and
RPCs, retry, and fair scheduling. The model should name those assumptions and otherwise make the
claim bounded or quiescent, following the qualification already used by Umpire.

## Feature models

Feature models should own product semantics: the legal lifecycle, operation identity, retry policy,
terminal outcomes, and relations to other feature entities. They should not own the implementation
of matching.

Each feature declaration belongs to its stable capability owner. The `activity` owner can evolve
Activity attempts and retry policy without importing Nexus internals; the `nexus` owner can evolve
operation and callback behavior without owning Activity delivery. Shared relations name a schema
owner, while properties spanning those relations live in a co-owned integration target.

Each feature instead adapts its vocabulary to the L0 delivery interface:

| Feature event | Foundation meaning |
| --- | --- |
| Workflow Task scheduled | Create a workflow-task work obligation |
| Activity scheduled or retried | Create an activity-attempt work obligation |
| History start-authorization transaction commits | Accept the corresponding feature attempt |
| Worker completion/failure/timeout | Resolve the attempt according to feature policy |
| Nexus handler dispatch | Create/route a handler work obligation and wait for its permitted response |
| Callback dispatch | Create a callback delivery obligation when callback transport enters scope |

This adapter is a refinement mapping, not just a name mapping. It must establish which feature state
authorizes the work, how identity is preserved, which concrete steps may stutter at the feature
level, and which completion changes the feature state.

For Workflow and Activity Tasks, Matching reservation, the authorization RPC response, and worker
receipt surround `AcceptStart` but do not define it. Lost responses and repeated reservations must
therefore stutter at L0 unless a successful History authorization transaction has committed.

The L0 interface must remain smaller than Matching. Workflow and Activity can refine it through the
durable History/Matching path; Nexus can refine it through Matching's synchronous request/response
path. Callback HTTP delivery is likely a sibling delivery implementation with different retry and
acknowledgement semantics. It should implement the common obligation/outcome contract where that is
true, not inherit Matching-specific backlog states merely to reuse a model.

The current Nexus/Activity relation is a good example of a property that belongs above an
individual feature but below the whole server. It should be checked in a small `Nexus + Activity`
composition co-owned by `nexus` and `activity`. If task delivery becomes relevant to the
counterexample, add the abstract delivery contract first and use a targeted
`Nexus + Activity + Delivery` target, adding the delivery provider as a co-owner; do not enable
every Workflow and Callback state in the same run.

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

The interface is also the organizational handoff. For example, `history` owns the guarantee that a
committed transition produces durable intent, `matching` owns the guarantee that accepted intent is
not lost or acknowledged prematurely, and `activity` owns the rule that a delivered attempt still
belongs to the current Activity state. No one owner can weaken the end-to-end claim unilaterally.

Cross-layer properties then become precise:

- a committed feature transition creates exactly the required work obligation;
- dispatched work corresponds to the current feature attempt;
- accepted/completed delivery enables only a legal feature transition;
- retry replaces the permitted attempt without duplicating the semantic obligation; and
- cancellation or terminal completion invalidates delivery according to an explicit race policy.

| Property scope | Default target |
| --- | --- |
| No task loss, duplicate authorization, stale-owner mutation, premature ack, or route crossing | Foundation target at the corresponding L1/L2/L3 depth |
| Legal Activity, Workflow, Nexus, or Callback lifecycle | Individual feature target over L0 assumptions |
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

The three generators should consume the same selected target, but they need not generate the same
proof shape.

| Backend | Best Umpire role | Model shape | Main caution |
| --- | --- | --- | --- |
| Ivy | Foundation safety and contract proofs | Relational state, quantified identity/cardinality invariants, isolates with imported/exported actions and assume-guarantee contracts | Broad arithmetic-heavy queue algorithms may leave Ivy's supported decidable fragment; abstract order/prefixes or isolate them |
| TLA+ | Reference semantics, integration, refinement, fairness and liveness | Modules composed over an explicit action interface; small TLC configurations; refinement mappings between L1 and L0 | Shared-variable composition and unbounded liveness need care; every finite result must retain bounds and fairness assumptions |
| P | Cross-check the selected atomic target; later support actorized refinement | Initially retain one `UmpireWorld` per target; a follow-up may add History shard, Matching partition, poller, and relay machines | Mailboxes change the current atomic semantics, so actorization must be a separate project and refinement target rather than a generator accident |

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
strand an obligation, provided the target declares the poller, failure, and fairness assumptions.
It should not compensate for a missing source-level module system by accumulating one ever-growing
`Next` relation.

### P

The initial split should keep P as the one-world semantic lowering for each selected target. This
preserves cross-backend comparison without introducing a second execution semantics.

After the split is independently useful, P's machines, asynchronous events,
[module system](https://p-org.github.io/P/manual/modulesystem/), and
[monitor specifications](https://p-org.github.io/P/manual/monitors/) align well with History,
Matching partitions, pollers, and responses. That actorized P model can find ordering bugs which the
current atomic world machine cannot express, but it is a follow-up refinement project rather than a
completion requirement for model splitting.

The ordinary P checker explores the requested finite test scenarios and schedule count; a run with
no bug is therefore bounded evidence, not an unqualified proof. P's compositional argument comes
from replacing dependencies with abstractions and checking trace-refinement obligations, as
formalized in the [ModP paper](https://ankushdesai.github.io/assets/papers/modp.pdf), not from merely
placing machines in separate source files.

Keep both meanings explicit:

- `p-semantic`: the current one-world lowering used to cross-check the shared atomic kernel; and
- `p-actors`: a future, separately selected execution refinement whose machines and message steps must
  refine that kernel or its delivery contract.

## Required source-model capabilities

The current verification IR cannot represent the recommended split without adding first-class
structure. The conceptual additions are:

- `ModelFamily`: the canonical compiled graph of modules, interfaces, compositions, refinements,
  and targets; projection lowers one target from this graph into the existing closed `Model` shape
  consumed by interpreters and exporters;
- `CapabilityOwner`: a stable semantic identifier whose current reviewers are resolved through
  repository ownership policy;
- `Module`: one capability owner plus its entities, relations, actions, properties, adapters, and
  internal state;
- `Interface`: one provider owner, declared consumer owners, imported/exported actions, assumptions,
  and guarantees;
- `RefinementMap`: concrete steps, stuttering steps, identity mapping, and abstract effects;
- `Composition`: modules and adapters instantiated together plus the capability owners responsible
  for their cross-boundary properties; and
- `VerificationTarget`: an owner or co-owners, selected roots, properties, bounds, fairness,
  failure policy, and backend requirements.

A target is a projection of the canonical protocol, not permission to omit dependencies silently.
Generation should reject a selected property when the target excludes an entity, action, relation,
assumption, or refinement obligation it needs.

### Contract discharge

The first contract representation should prefer stable identity over a general-purpose theorem
language. An interface declares named obligations. A provider defines each obligation once as
exported actions plus safety or progress properties in the verification IR; consumers import those
same obligation IDs rather than restating similar expressions. Structural validation checks that:

- every imported obligation names exactly one provider guarantee;
- the provider and all declared consumers agree on the interface and identity sorts;
- every provider guarantee is included in a module check which verifies it under only that module's
  external assumptions; and
- every adapter has a refinement target for each imported or exported action, including explicit
  stuttering mappings.

Exact obligation identity is the initial discharge rule; the compiler must not guess logical
implication between separately written expressions. The Go validator checks ownership, references,
dependency closure, and exact discharge. The generated backend check establishes each provider
property and refinement obligation. A target is successful only if both stages pass.

Assume-guarantee dependencies form a directed graph from a provider guarantee to the external
obligations used to prove it. The compiler topologically orders an acyclic graph. It rejects a cycle
unless a declared composition closes that strongly connected component and checks its internal
guarantees without assuming any guarantee from within the same component. This prevents feature
validity and delivery guarantees from proving each other circularly.

### Target projection and hiding

Projection is a deterministic compiler pass:

1. Start from a target's selected modules, compositions, properties, and refinement maps.
2. Compute transitive closure over referenced entities, relations, actions, interfaces, identity
   maps, assumptions, guarantees, and properties.
3. Retain every action owned by the closure. An omitted action which cannot affect retained state or
   an exported observation may disappear.
4. Permit an omitted internal action to stutter only when an explicit refinement map classifies it
   as stuttering and it has no unmodeled effect on retained state.
5. If an omitted module controls an action which can affect retained state, retain a constrained
   environment action derived from an imported interface guarantee. Reject the target when no such
   guarantee exists.
6. Reject a target whose selected property, progress premise, refinement obligation, or minimum
   non-vacuous bound is not closed by the result.

The projector emits a closure report listing retained, stuttering, environment, and rejected
actions. Exporters consume only this validated projected IR; they do not implement independent
hiding rules. This makes a projection error visible before it can be repeated by every backend.

Illustrative targets are:

```text
protocol-atomic
foundation-delivery-safety
foundation-backlog-ack
foundation-ownership-fencing
feature-workflow
feature-workflow-speculative-delivery
feature-activity
feature-nexus
feature-callback
integration-workflow-delivery
integration-activity-delivery
integration-nexus-activity
integration-nexus-delivery
integration-callback-nexus
integration-callback-workflow
```

Each target should have independent bounds. For example, an ownership proof may need two owners but
only one queue; a duplicate-start check needs two attempts; a routing isolation check needs two
namespaces or queues; a feature lifecycle check may need no physical partition at all. A single
global identity bound wastes states and can accidentally make an invariant vacuous.

For the first non-vacuous finite targets, use at least two obligations or attempts for duplicate
and aliasing properties, two owner generations for fencing, two queues or namespaces for isolation,
and competing pollers where reservation matters. These are test-design minima, not a cutoff theorem.
TLC exhaustiveness is only for the constants in a particular
[finite model](https://lamport.azurewebsites.net/tla/model-popup.html), and P's `-s` option controls
the number of schedules explored by the
[checker](https://p-org.github.io/P/getstarted/usingP/). Unless Umpire proves a target-specific
cutoff, results must not be extrapolated from those bounds to arbitrary cluster size. Report a claim
as parameterized only when an Ivy or inductive proof actually establishes it.

## Failure modes of the split

| Failure mode | Consequence | Required control |
| --- | --- | --- |
| Two modules assume the same unproved fact | Both pass while their composition is invalid | Give each assumption an owning guarantee and reject undischarged contracts |
| Feature and delivery guarantees depend on each other | Both module checks succeed through circular assumptions | Reject the contract cycle or prove its closed composition without internal assumptions |
| Adapter maps the wrong identity or terminal state | Module checks pass but the system-level claim is false | Generate explicit refinement obligations and run small concrete compositions |
| Projection silently deletes an interfering action | Safety passes vacuously or liveness changes | Validate dependency closure and make hidden/environment actions explicit |
| One-identity bounds remove contention | Cardinality, routing, and ownership properties pass trivially | Define per-property minimum non-vacuous bounds and mutation tests |
| Fairness is implicit | A liveness result claims more than the system guarantees | Record fairness and availability assumptions in the target and result |
| All exporters share a lowering bug | Cross-backend agreement repeats the same error | Retain the Go interpreter, tiny reachable-world equivalence, and seeded semantic mutations |
| Formal actions have no live realization/observation | The abstraction is sound but says little about Temporal | Preserve Umpire refinement, causal-footprint, and observation-gap reporting |
| A common interface is made too feature-specific | Nexus or Callback inherits false Matching semantics | Keep L0 minimal and allow sibling implementations behind it |
| One canonical source becomes centrally owned | Cross-team changes bottleneck on a modeling group and domain declarations drift | Federate declarations by stable capability owner and compile them into one graph |
| A provider changes a contract unilaterally | Consumer proofs still pass against an obsolete assumption | Require affected consumer review and contract/refinement CI for exported changes |
| An integration target has no durable owner | Cross-capability properties decay while every local target stays green | Record explicit co-owners and reject ownerless compositions |
| An organization rename changes model identity | Artifacts and history churn for a non-semantic change | Keep capability IDs stable and update only the `CODEOWNERS` mapping |

## What should remain outside these models

Unless a property explicitly depends on them, exclude:

- arbitrary payload bytes and most protobuf fields;
- exact goroutine, channel, cache, and RPC-handler structure;
- logging, metrics, tracing transport, and error text;
- precise timeout durations rather than timeout ordering;
- every task-queue kind, routing policy, or deployment-version combination in one target;
- performance, capacity, and 10x-load claims; and
- implementation-specific fairness heuristics in the default foundation target.

Some of these deserve specialized tests or dedicated models. They should not inflate the semantic
acceptance model merely because Umpire can observe them.

## Boundary decisions for the first target

The model family can be designed now, but the first L0/L1 target must make these choices explicit:

- **Acceptance linearization:** for Workflow and Activity Tasks, treat the successful History
  start-authorization transaction as `AcceptStart`; Matching reservation and worker receipt remain
  distinct surrounding steps. If Temporal intends a different product guarantee, change the L0
  vocabulary instead of hiding the difference in a generator.
- **Ownership domains:** model History shard and Matching task-queue generations separately, with a
  mapping only where an action carries both. A single global “owner epoch” would prove a system
  Temporal does not implement.
- **Speculative Workflow Tasks:** keep them explicitly out of the first durable L1 target, then add
  a sibling direct-dispatch-with-durable-fallback target. The current inventory already identifies
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

1. Add the structural authoring and IR prerequisites. Extend
   `tests/umpire2/protocol/protocol.go`, `compile.go`,
   `common/testing/umpire/verify/model.go`, and `validate.go` with `ModelFamily`, capability owners,
   modules, interfaces, compositions, refinement maps, and verification targets. Keep the existing
   closed `verify.Model` as the exporter input. Validate exact obligation discharge, ownership,
   references, and contract cycles. Keep `protocol.Default()` compiling the same full runtime graph
   used by the monitor, planner, relations, and coverage.
2. Declare the initial stable capability owners, map their source paths through `CODEOWNERS`, and
   assign provider, consumer, relation-schema, and integration ownership for the current protocol.
   Generation must reject unowned declarations and ownerless compositions.
3. Implement target projection in a focused `common/testing/umpire/verify/project.go` deep module.
   Its interface should accept a `ModelFamily` plus a target name and return either one closed
   `verify.Model` with a closure report or a validation error. Test dependency closure,
   explicit stuttering, constrained environment actions, missing contracts, and non-vacuous bounds
   in isolation before changing any exporter.
4. Teach `tests/umpire2/protocol.VerificationModel`, manifests, each exporter, and
   `cmd/umpire-genmodels` to consume projected targets. Add `-target`, retain `-profile` for
   `smoke`/`nightly`, and emit the target-scoped artifact layout. First reproduce the current model
   as `protocol-atomic` and require reachable-world and generated-action/property equivalence.
5. Define L0 `WorkObligation` and `DeliveryAttempt`, the exact `AcceptStart` linearization, stable
   obligation IDs, terminal outcomes, and the feature adapter contract. Seed wrong identity,
   terminal-state, and stuttering mappings and require contract/refinement checks to reject them or
   expose their counterexamples.
6. Derive the first L1 foundation target for one logical queue, bounded obligations and attempts,
   sync/backlog choice, History start acceptance/rejection, coarse retirement, retry, and abstract
   crash/ownership outcomes. Keep owner generations, routing topology, and backlog positions behind
   contracts. Seed task-loss, duplicate-start, split-commit, retry-identity, and premature-retirement
   faults in the Go interpreter and generated Ivy/TLA+/P targets.
7. Connect Workflow and Activity adapters; check each feature alone and in small concrete delivery
   compositions. Then add the speculative Workflow Task sibling target with direct dispatch,
   durable fallback, cancellation, and duplicate-start races. Seed a broken fallback or acceptance
   mapping so `SpeculativeTaskCreation` no longer remains an untested inventory entry.
8. Connect Nexus dispatch while preserving the existing lowered `nexus.start_activity` action and
   its reciprocal-link and terminal-strengthening properties. Check both the Nexus-only target and
   the Nexus + Activity composition, including an intentionally wrong feature/foundation adapter.
9. Add Callback's L0 sibling delivery adapter and Callback + Nexus and Callback + Workflow targets.
   Preserve its transport-specific retry and acknowledgement policy, and seed wrong operation/run
   identity plus terminal-lifetime faults.
10. Add L2 owner-generation fencing, partition, forwarding, poller-compatibility, and routing
    targets one property at a time. Each addition must refine the corresponding L1 contract and
    detect a target-specific stale-owner or route-crossing mutation.
11. Add L3 read/ack advancement, garbage collection, and fairness algorithms only where a production
    invariant demands their concrete structure. Each target must detect a backlog-position or
    acknowledgement mutation not expressible at L1.

`p-actors` is a separate follow-up after these targets are independently useful. Its machines,
mailboxes, and message failures must refine `p-semantic` or L0/L1, but actorization is not a
completion condition for this split.

The success criterion is not that every backend accepts a larger model. Each layer should detect a
seeded fault unique to that layer, every projection/refinement boundary should reject or expose a
mutation introduced at that boundary, and the integration targets should detect a deliberately
wrong feature/foundation adapter. Without those tests, splitting may improve runtime while merely
moving blind spots between models.

## Verification matrix

Verification is a gate at each milestone, not a final cleanup pass:

| Surface | Test anchor | Required evidence |
| --- | --- | --- |
| Authoring and IR | `tests/umpire2/protocol/compile_test.go`, `common/testing/umpire/verify/model_test.go` | Unknown owners/consumers, duplicate guarantees, undischarged obligations, and unclosed contract cycles are rejected |
| Projection | new `common/testing/umpire/verify/project_test.go` | Dependency closure is complete; omitted interference becomes a constrained environment action or an error; only declared actions stutter; minimum bounds are enforced |
| Runtime preservation | `tests/umpire2/protocol/default_test.go`, planner, coverage, relation, and monitor tests | `protocol.Default()` retains the unified runtime entity/action/relation graph and existing consumers do not depend on a projected target |
| Multi-target generation | `cmd/umpire-genmodels/main_test.go` and exporter tests | `protocol-atomic` matches the current kernel; every target has deterministic IR, manifest, backend artifacts, bounds, owners, and closure report; `-target` and execution `-profile` remain independent |
| Contract and adapters | protocol verification/refinement tests | Wrong obligation identity, terminal mapping, acceptance point, stuttering classification, and cyclic discharge fail independently |
| Semantic layers | focused L0/L1/L2/L3 and feature/integration mutation tests | Each layer detects a seeded fault which a shallower contract intentionally abstracts |
| Cross-backend semantics | interpreter and tiny reachable-world equivalence tests | A projected fault cannot disappear between the source graph, projected IR, and generated Ivy/TLA+/P target |

Run the focused pure Go checks first, always with the required build tag:

```sh
go test -tags test_dep ./common/testing/umpire/verify/... ./tests/umpire2/protocol/... ./cmd/umpire-genmodels/...
```

Then verify deterministic artifacts and the bounded smoke execution profile:

```sh
make umpire-check-genmodels
make umpire-verify-smoke
make lint-code
```

Backend-dependent tests may skip when their pinned tool is unavailable locally, but CI must exercise
the configured target/backend matrix. A milestone is incomplete if its seeded mutation is not
observed failing before the correct model passes.

## Answers to the design questions

- **How deep should Umpire go?** Through the durable dispatch and start-handshake protocol (L1) by
  default. Add ownership/routing and queue algorithms as focused targets when a stated invariant
  requires them. Stop before implementation mechanics that do not change enabled actions or
  correctness outcomes.
- **Same model or separate models?** One source model and semantic vocabulary; multiple generated
  modules and bounded targets. Do not use one universal state space or independently maintained
  specifications.
- **How do they integrate?** Through an explicit work-delivery interface, feature adapters,
  refinement maps, and selected composed verification targets.
- **How are they owned?** By stable capability owners whose declarations compile into one model
  family. Provider and consumer owners share interface changes; integration targets are explicitly
  co-owned; current team mappings live in `CODEOWNERS` rather than semantic identifiers.
- **Where is Ivy/P/TLA+ generation most valuable?** First in infrastructure safety and concurrency.
  Ivy is especially promising for relational safety contracts, TLA+ for integrated refinement and
  fairness, and the existing one-world P lowering for cross-checking atomic targets. A deliberately
  actorized P refinement is valuable later. Feature lifecycle generation remains useful but should
  usually sit above the delivery contract.

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
