# Namespace and History-layer architecture spike

## Decision

**Selected on 2026-09-04: Option A.** Local-first synchronization uses a purpose-built
active-History path that works for both local and global namespaces. The alternatives below remain
as supporting analysis, not open choices.

The user-facing requirement is that an existing namespace can adopt local-first without being
recreated or having its workflows manually migrated. Namespace mode is an implementation choice,
not a product objective.

## What namespace promotion actually entails

Promoting one namespace is already a supported, simple, one-way operation:

```text
temporal operator namespace update --namespace <name> --promote-global
```

`UpdateNamespace` persists `is_global_namespace = true`, chooses the next failover version, and is
a no-op for an already-global namespace. There is no corresponding demotion. Existing executions
are not rewritten: server code explicitly handles histories created with `EmptyVersion` before a
local namespace was promoted, and XDC tests cover promotion with an existing workflow.

The important qualification is that this command only works after global namespaces have been
enabled for the whole cluster. `clusterMetadata.enableGlobalNamespace` is static server
configuration, is false in the ordinary development and Docker configurations, and is persisted
as a one-way cluster-metadata transition when the server starts. Enabling it therefore requires a
correct cluster/failover-version configuration and a deployment rollout or restart before the
per-namespace command can succeed. A single-cluster global namespace is valid, so a second cluster
is not required, but this is still more than an ordinary namespace feature toggle.

Consequences of requiring promotion:

- The namespace and its existing workflows do not need to be recreated or copied.
- Operators must first enable a cluster-wide, effectively irreversible capability if it is off.
- The namespace then permanently acquires global-namespace failover/version semantics even if it
  never uses cross-cluster replication.
- Hosted or locked-down installations may not allow the Worker owner to perform either operation.

## What the existing replication layer provides

The public History handlers for `ImportWorkflowExecution`, `ReplicateEventsV2`, and related APIs
reject requests when global namespaces are disabled. The guard is based on the cluster capability;
removing it would not by itself make the stack an active-history synchronization API.

Below that guard, the NDC machinery provides useful components:

- raw event decoding and version-history validation;
- application of multiple event batches to mutable state;
- branch selection, rebuilding, duplicate detection, and conflict resolution;
- create/update handling for a new run.

However, its transaction managers deliberately model events arriving from another cluster. They
normally close imported state with `TransactionPolicyPassive`, compare competing version-history
branches, suppress one run with another, and create or update replication branches. Those are not
the semantics of an official local bridge returning events for the still-active upstream run.
The current `ReplicateEventsV2` loop also commits each batch independently, so promotion does not
solve whole-delta atomicity.

The most reusable lower-level component is
`service/history/workflow.MutableStateRebuilder.ApplyEvents`: it already applies a sequence of
server-produced history batches and reconstructs mutable state. It is also used from active-side
code in some rebuild paths. It needs extraction or a local-sync wrapper because it currently has
passive-rebuild behavior such as clearing stickiness and generating tasks while walking
intermediate events.

## Options

### Option A: purpose-built active-History synchronization path

Add a local-sync operation inside History that runs under the normal workflow lock and supports
both local and global namespaces. Reuse the event-application/rebuild primitives, but do not route
the operation through the NDC transaction managers. The new path would:

1. Validate ownership, cursor, versions, limits, and the complete request.
2. Apply all trusted event batches to a working mutable state without persisting intermediate
   states.
3. Discard tasks produced for intermediate states and refresh only tasks required at the final
   cursor, conditioned on whether ownership is retained or released.
4. Commit the history, final mutable state, sync fingerprint/cursor, and ownership outcome through
   the ordinary active workflow persistence path.
5. Let a global namespace's normal active transaction produce its ordinary replication tasks only
   after the local sync commits.

Benefits:

- Existing local namespaces work without an administrative conversion.
- The operation models what is really happening: the active server accepts an extension of its
  own fenced execution, rather than pretending another cluster won a replication conflict.
- Local and global namespaces share one local-first contract; global namespaces retain their
  usual downstream replication behavior after commit.
- The atomicity mechanism described in the companion spike fits the ordinary workflow transaction
  boundary.

Costs and risks:

- Event application must be separated cleanly from passive NDC conflict policy.
- Final-state task regeneration and local-ownership task suppression need explicit tests.
- The trusted-history validation boundary remains bespoke until the later command/result-journal
  work.

This is the recommended option. It is more code than calling `ReplicateEventsV2`, but less semantic
coupling and operational burden than making global namespaces a prerequisite. Much of the hard
event-to-mutable-state logic is still reused.

### Option B: require promotion and refactor NDC into an atomic importer

Require global namespaces and extract a new atomic operation from the importer/replicator beneath
their current per-batch entrypoints. The operation would still need a special mode that treats the
incoming branch as the fenced active branch, regenerates the right tasks, and commits the entire
delta once.

Benefits:

- Version-history and import concepts stay near the subsystem that already consumes foreign
  history.
- Namespace promotion itself is supported and does not rewrite existing workflows.

Costs and risks:

- Installations with global namespaces disabled need a cluster rollout and irreversible metadata
  transition before enabling local-first.
- NDC's passive/conflict semantics are not removed by promotion; substantial refactoring is still
  required for the active owner and atomicity contract.
- A replication-oriented abstraction remains responsible for a same-cluster execution lease,
  making future maintenance and task-generation reasoning harder.

Choose this only if server maintainers consider NDC the required ownership boundary for accepting
raw history and are comfortable making global enablement an explicit product prerequisite.

### Option C: retain the global-only wrapper for the prototype, replace it later

Keep the current guarded `ReplicateEventsV2` implementation while adding a separate atomic active
path later.

This has the least immediate churn, but it cannot meet the accepted whole-delta contract and would
make Phase 2 validation target a path intended for deletion. It is useful as a feasibility test,
not as the next implementation architecture.

## Selected rationale

**Option A** was selected: a purpose-built active-History sync path for both namespace types, while
extracting and reusing the NDC/workflow event-application primitives that are policy-neutral.
Promotion is operationally possible, but it does not eliminate the required atomic-commit,
active-policy, ownership, or task-regeneration work. Its cluster-wide and one-way consequences are
therefore cost without a compensating simplification.

The alternatives considered were:

- **A — Active path for local and global namespaces (recommended).**
- **B — Global namespace required; refactor NDC for the new atomic active-owner mode.**
- **C — Keep the global-only feasibility wrapper temporarily despite deferring contract-compliant
  synchronization.**

## Code evidence

- Namespace promotion API: `temporal-api/temporal/api/workflowservice/v1/request_response.proto`
  (`UpdateNamespaceRequest.promote_namespace`).
- CLI command: `temporal-cli/internal/temporalcli/commands.operator_namespace.go`.
- One-way namespace update and failover-version change:
  `temporal-server/service/frontend/namespace_handler.go`.
- One-way cluster capability persistence: `temporal-server/temporal/fx.go` and
  `temporal-server/common/cluster/metadata.go`.
- Global-capability handler guard: `temporal-server/service/history/api/replication_util.go` and
  `temporal-server/service/history/handler.go`.
- Passive NDC transaction policy: `temporal-server/service/history/ndc/history_importer.go` and
  `temporal-server/service/history/ndc/transaction_manager*.go`.
- Reusable event application: `temporal-server/service/history/workflow/mutable_state_rebuilder.go`.
