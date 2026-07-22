# Visibility

This page documents Temporal's **Visibility** subsystem: what it is, which APIs depend on it, how it is
rate limited, and its consistency guarantees. The target audience is server developers and operators. For
the user-facing feature documentation, see [docs.temporal.io/visibility](https://docs.temporal.io/visibility).

> [!IMPORTANT]
> "Visibility" is broader than most people assume. It is **not** limited to `ListWorkflowExecutions`. Any API
> that needs to *list*, *count*, *search*, or *filter* over a collection — including `ListSchedules`,
> `ListBatchOperations`, `GetWorkerTaskReachability`, and the worker/deployment listing APIs — is served by the
> Visibility subsystem. These APIs are governed by a **separate rate limiter** and have **different consistency
> guarantees** (eventual, not read-your-writes) than the workflow execution APIs. See
> [Which APIs are Visibility APIs](#which-apis-are-visibility-apis) for the authoritative list.

## What Visibility is

Temporal stores two kinds of data in two different places:

1. **Primary (mutable state / history) store** — the authoritative, strongly-consistent record of each
   Workflow Execution, keyed by Namespace + Workflow ID + Run ID. This is what the History Service reads and
   writes to drive durable execution. APIs like `DescribeWorkflowExecution` and `GetWorkflowExecutionHistory`
   read from here.

2. **Visibility store** — a secondary, **denormalized, queryable index** of executions and their
   [Search Attributes](https://docs.temporal.io/search-attribute). It exists so operators can answer questions
   like *"which workflows of type `X` are currently running?"* or *"how many executions closed with status
   `Failed` in the last hour?"* without scanning the primary store. It is typically backed by Elasticsearch (or
   OpenSearch), or by a SQL database (the "standard"/advanced visibility SQL stores).

The Visibility store is accessed exclusively through the `VisibilityManager` interface
([`common/persistence/visibility/manager/visibility_manager.go`](../../common/persistence/visibility/manager/visibility_manager.go)):

```go
// Write APIs (invoked asynchronously by the History Service, not by the frontend read path)
RecordWorkflowExecutionStarted(...)   // execution started
RecordWorkflowExecutionClosed(...)    // execution closed
UpsertWorkflowExecution(...)          // search attributes / memo changed
DeleteWorkflowExecution(...)          // execution deleted

// Read APIs (invoked by frontend list/count/search RPCs)
ListWorkflowExecutions(...)
CountWorkflowExecutions(...)
GetWorkflowExecution(...)
ListChasmExecutions(...)              // CHASM archetypes (e.g. Schedules)
CountChasmExecutions(...)

// Admin APIs
AddSearchAttributes(...)              // register custom search attributes
```

The crucial takeaway: **the read APIs above are the foundation for a much larger set of frontend gRPC RPCs**.
Many APIs that don't have "workflow" in their name are, under the hood, a `ListWorkflowExecutions` /
`CountWorkflowExecutions` call with a canned query filter.

## Which APIs are Visibility APIs

There are two lenses here, and both matter:

- **Rate-limit / SLO classification** — which RPCs the frontend routes through the dedicated *visibility rate
  limiter* (and therefore inherit visibility's SLOs and limits). This is the authoritative,
  code-enforced answer and is the one that surprises people.
- **Store-access reality** — which handler actually issues a query against the Visibility store.

These overlap heavily but not perfectly, so both are listed below.

### Authoritative list (rate-limit classification)

The authoritative list lives in
[`service/frontend/configs/quotas.go`](../../service/frontend/configs/quotas.go) as `VisibilityAPIToPriority`.
Every RPC in this map is routed to the visibility rate limiter (see [Rate limiting](#rate-limiting)) instead of
the general execution-API limiter. If you are adding an API that lists/counts/searches, **you must add it
here**, per the comment in that file: *"If APIs rely on visibility, they should be added to
VisibilityAPIToPriority."*

| Frontend RPC | Serves a query over the Visibility store via |
|---|---|
| `ListWorkflowExecutions` | `VisibilityManager.ListWorkflowExecutions` (frontend) |
| `ListOpenWorkflowExecutions` | `VisibilityManager.ListWorkflowExecutions` (frontend) |
| `ListClosedWorkflowExecutions` | `VisibilityManager.ListWorkflowExecutions` (frontend) |
| `ScanWorkflowExecutions` | `VisibilityManager.ListWorkflowExecutions` (frontend) |
| `CountWorkflowExecutions` | `VisibilityManager.CountWorkflowExecutions` (frontend) |
| `ListArchivedWorkflowExecutions` | the **archival** visibility store (rate-limited as visibility, but reads the archival store, not the live index) |
| `ListSchedules` | `VisibilityManager.ListWorkflowExecutions` / `ListChasmExecutions` (frontend) |
| `CountSchedules` | `VisibilityManager.CountWorkflowExecutions` / `CountChasmExecutions` (frontend) |
| `ListBatchOperations` | `VisibilityManager.ListWorkflowExecutions` (frontend) |
| `GetWorkerTaskReachability` | `VisibilityManager.CountWorkflowExecutions` (frontend) |
| `DescribeTaskQueue` (when reachability is requested) | `VisibilityManager.CountWorkflowExecutions` (internal `DescribeTaskQueueWithReachability`) |
| `ListWorkers` | Visibility query issued by the handling service |
| `CountWorkers` | Visibility query issued by the handling service |
| `DescribeWorker` | Visibility query issued by the handling service |
| `ListActivityExecutions` | Visibility query issued by the handling service |
| `CountActivityExecutions` | Visibility query issued by the handling service |
| `ListNexusOperationExecutions` | Visibility query issued by the handling service |
| `CountNexusOperationExecutions` | Visibility query issued by the handling service |
| `ListDeployments` | Visibility query issued by the handling service |
| `GetDeploymentReachability` | Visibility query issued by the handling service |
| `ListWorkerDeployments` | Visibility query issued by the handling service |

> [!NOTE]
> This table is generated from the source of truth (`VisibilityAPIToPriority`). If the map changes, update this
> table. `ListArchivedWorkflowExecutions` is an intentional caveat: it is classified as a Visibility API for
> rate-limiting purposes, but it reads the *archival* visibility store, not the live index.

### `ListSchedules`: a worked example

`ListSchedules` is the canonical example of an API whose visibility dependency is not obvious from its name,
and it was the direct cause of production confusion. It is **not** a dedicated "schedules table" lookup — it is
a Visibility query filtered to the schedule namespace-division:

```
ListSchedules (service/frontend/workflow_handler.go)
  └─ prepareSchedulerQuery: wraps the caller's query in a base query
     (scheduler.VisibilityListQueryV1, or ...QueryChasm for the CHASM scheduler)
  └─ V1 path:    listSchedulesWorkflow
                   └─ visibilityMgr.ListWorkflowExecutions(ListWorkflowExecutionsRequestV2{Query: ...})
  └─ CHASM path: listSchedulesChasm
                   └─ chasm.ListExecutions -> ChasmVisibilityManager -> visibilityMgr.ListChasmExecutions
```

Consequences that follow directly from this, and which callers must account for:

- `ListSchedules` counts against the **visibility rate limit**, not the general namespace rate limit. A tenant
  with plenty of headroom on execution APIs can still be throttled here.
- `ListSchedules` is **eventually consistent**: a schedule created a moment ago may not appear immediately, and
  a deleted one may linger briefly — because the visibility record is written asynchronously (see
  [Consistency](#consistency-eventual-not-read-your-writes)).
- `ListSchedules` is subject to `FrontendVisibilityMaxPageSize` and the same query/Search-Attribute validation
  as `ListWorkflowExecutions`.

`DescribeSchedule` (a point lookup by ID) does **not** go through visibility; only the *list/count* variants do.

### APIs that are NOT Visibility APIs (common confusions)

- `DescribeWorkflowExecution` — reads the primary store via the History Service. It touches the visibility
  manager only for search-attribute *type* metadata (`GetIndexName()`), not for a query.
- `GetWorkflowExecutionHistory` — reads history from the primary store (or archival).
- `DescribeSchedule`, `DescribeBatchOperation`, `DescribeWorkerDeployment` — point lookups, not visibility list/count.

## Write path and Consistency (eventual, not read-your-writes)

The frontend visibility read path and the visibility **write** path are decoupled. The frontend never writes to
visibility on the request path — it is constructed with a `nil` writer. Visibility records are instead written
**asynchronously** by the History Service:

```
Workflow state transition (start / close / SA upsert / delete) on a History shard
  └─ a Visibility Task is enqueued on the shard's Visibility Task Queue
       └─ visibilityQueueProcessor drains the queue and calls
          VisibilityManager.Record*/Upsert/Delete against the Visibility store
```

See the [History Service](./history-service.md#visibility-task-queue) doc for the queue mechanics. The practical
implications:

- **Eventual consistency.** There is a propagation delay (typically on the order of ~1–2 seconds, but not
  bounded or SLO'd, and larger under load or backlog) between a workflow state change and that change being
  reflected in visibility query results. This is why the SA guidance says Search-Attribute updates and
  visibility data are eventually consistent.
- **No read-your-writes guarantee.** Immediately after `StartWorkflowExecution` returns, a
  `ListWorkflowExecutions` / `ListSchedules` may not yet include the new execution. **Do not** use visibility to
  confirm that a just-started workflow exists, or to poll for the Workflow ID / Run ID of a workflow you just
  started — that value is already returned to you synchronously by the start call. Polling visibility for it
  couples correctness to an eventually-consistent index.
- **Do not put visibility queries in workflow business logic.** Visibility is an operational/analytical
  facility. Using it on a hot path (e.g. a workflow that lists other workflows to decide what to do next)
  multiplies load against a shared, rate-limited subsystem and makes business logic depend on eventual
  consistency. For workflow-to-workflow coordination, prefer:
  - Signals / Updates, parent-child workflows, or [Nexus](./nexus.md) for cross-workflow orchestration;
  - Queries or an external datastore for reading a running workflow's own state;
  - the deterministic result of the start/signal/update call itself for IDs and outcomes.

> [!NOTE]
> Search Attribute values are **not** encoded by Data Converters or Payload Codecs — they are stored in
> plaintext in the visibility index so they can be searched. Never put sensitive data (PII, secrets, etc.) in
> Search Attributes.

## Rate limiting

Visibility has its **own** rate limiting, independent of the general execution-API limits, applied at two
layers.

### 1. Frontend, per-namespace (what tenants hit first)

The frontend routes every RPC in `VisibilityAPIToPriority` through a dedicated *visibility* rate limiter,
separate from the execution-API limiter, wired in
[`service/frontend/fx.go`](../../service/frontend/fx.go) (`NamespaceRateLimitInterceptorProvider`) and
[`service/frontend/configs/quotas.go`](../../service/frontend/configs/quotas.go) (`NewRequestToRateLimiter`).

| Dynamic config key | Default | Scope | Meaning |
|---|---|---|---|
| `frontend.namespaceRPS.visibility` | `10` | per namespace, per frontend instance | Per-instance RPS limit for Visibility APIs. |
| `frontend.globalNamespaceRPS.visibility` | `0` (disabled → use per-instance) | per namespace, cluster-wide | Cluster-wide RPS for Visibility APIs, evenly divided across frontends. Overrides the per-instance limit when set. |
| `frontend.namespaceBurstRatio.visibility` | `1` | per namespace | Burst allowance as a ratio of the effective visibility RPS. |
| `internal-frontend.globalNamespaceRPS.visibility` | `0` | per namespace, cluster-wide | Same as above for the internal frontend. |

Because this bucket is separate from `frontend.namespaceRPS` (the execution-API limit), a namespace can have
ample capacity for `StartWorkflowExecution`/`SignalWorkflowExecution` and still be throttled on
`ListWorkflowExecutions`/`ListSchedules`. Calls from the Web UI / CLI are given operator priority
(`system.operatorRPSRatio`) so operator traffic is less likely to be starved by application traffic.

> [!NOTE]
> These `*.visibility` rate-limit settings are marked EXPERIMENTAL in the code and may change between releases.

### 2. Persistence, system-wide (protects the store itself)

The Visibility store manager is additionally wrapped by a rate limiter
([`common/persistence/visibility/visibility_manager_rate_limited.go`](../../common/persistence/visibility/visibility_manager_rate_limited.go),
wired in [`factory.go`](../../common/persistence/visibility/factory.go)) that caps how hard any single host will
hit the store:

| Dynamic config key | Default | Meaning |
|---|---|---|
| `system.visibilityPersistenceMaxReadQPS` | `9000` | Max read QPS a host issues against the Visibility store. |
| `system.visibilityPersistenceMaxWriteQPS` | `9000` | Max write QPS a host issues against the Visibility store. |
| `system.visibilityPersistenceSlowQueryThreshold` | `1s` | Queries slower than this are logged as slow. |

Related page-size / behavior knobs: `frontend.visibilityMaxPageSize` (`FrontendVisibilityMaxPageSize`),
`system.visibilityDisableOrderByClause`, `system.visibilityEnableManualPagination`.

## Search Attributes

Visibility queries filter and sort on **Search Attributes** — typed, indexed fields on an execution. System
Search Attributes (e.g. `ExecutionStatus`, `WorkflowType`, `StartTime`, `TemporalNamespaceDivision`) are always
available; custom ones are registered via the Operator API `AddSearchAttributes`
([`service/frontend/operator_handler.go`](../../service/frontend/operator_handler.go)), which calls through to
`VisibilityManager.AddSearchAttributes` to create the corresponding index fields. Several "canned" queries in
the server are built on system Search Attributes — for example `ListSchedules` filters on
`TemporalNamespaceDivision`, and `ListBatchOperations` filters on the batcher division.

## Summary

- Visibility is the **denormalized, queryable index** of executions and their Search Attributes — distinct from
  the strongly-consistent primary store.
- The set of "Visibility APIs" is much broader than `ListWorkflowExecutions`. The authoritative list is
  `VisibilityAPIToPriority` in `service/frontend/configs/quotas.go`; it includes `ListSchedules`,
  `CountSchedules`, `ListBatchOperations`, `GetWorkerTaskReachability`, the worker/deployment listing APIs, and
  more.
- Visibility APIs are **rate limited separately** from execution APIs (`frontend.namespaceRPS.visibility`,
  default 10 RPS/instance) and again at the persistence layer (`system.visibilityPersistence*`).
- Visibility is **eventually consistent** with no read-your-writes guarantee, because records are written
  asynchronously by the History Service's visibility task queue.
- Treat Visibility as operational/analytical only. Keep it out of workflow business-logic hot paths, and never
  store sensitive data in Search Attributes.
