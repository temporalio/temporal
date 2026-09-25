# CHASM: Coordinated Heterogeneous Application State Machines

CHASM is a framework for developing applications as distributed state machines inside the Temporal server. It handles the complexity of concurrency, sharding, routing, replication, and fault tolerance so that developers can focus on actual business logic.

## Why CHASM

- **Expand the product surface**: Use Temporal primitives to build first-class applications other than just Workflows. Standalone activities, schedulers, and future products are all built on CHASM.
- **Improve developer efficiency**: Hide low-level distributed systems details behind a clean, typed API.

---

## Concepts

### Execution

An execution is the unit of concurrency and lifecycle tracking in CHASM—analogous to a workflow execution. Namespace retention, visibility records, and ID reuse policies all apply at the execution level. Each standalone CHASM application (e.g., a standalone activity) is one execution.

### CHASM Tree

Each execution has exactly one CHASM tree. The tree is composed of **nodes**: components, data fields, maps, and pointers. The root of every tree is always a component.

### Archetype

The **Archetype** is the component type of the CHASM tree root—the fully qualified name that identifies what kind of application an execution represents (e.g., `"activity.activity"`).

### Component

A **Component** is a reusable piece of application logic with its own state. Components can be nested: a component may contain data fields, child components, maps of components, or pointers to other components.

When used as the root of an execution, a component becomes the application. The root component's lifecycle state drives the lifecycle of the entire execution—when it transitions to a closed state (completed or failed), the execution is closed and will eventually be cleaned up according to the namespace's retention policy.

### Tasks

A **Task** is a unit of reusable logic executed asynchronously by the framework in the background. Tasks are scheduled by components during state transitions.

There are two kinds of tasks:

| Kind | Runs | Use case |
|------|------|----------|
| **Side Effect Task** | Outside the transaction, after it commits | I/O with external systems: dispatching to matching, calling external APIs |
| **Pure Task** | Inside a transaction with full component access | In-place state transitions, e.g., applying a timeout |

Every task also has a **validator** that runs before the executor. The validator can silently drop a task when it is no longer relevant—for example, when a newer attempt has already superseded an older timer.

### Library

A **Library** is a named collection of related components, tasks, and gRPC service registrations. It is the unit of registration with the CHASM registry. A library groups everything needed to run a CHASM application: the component definitions, the background task executors, and the API service handlers.

### Context

The **CHASM Context** is the gateway for components to access framework features and execution-level information such as the execution key, current time, state transition count, logger, and metrics handler.

There are two variants: an immutable context for read-only access and a mutable context that additionally allows scheduling tasks. The framework automatically provides the appropriate variant depending on the operation—mutable during state-changing transitions, immutable during reads and task validation.

### Engine

The **Engine** is the external interface to the CHASM framework. It provides CRUD-style operations for creating executions, updating components, reading state, and long-polling for state changes. API handlers and side effect task executors use the engine to interact with executions from outside the transaction boundary.

### Visibility

**Visibility** is a built-in child component that manages an execution's visibility record: search attributes and memo. A root component opts into automatic visibility updates by declaring a Visibility child field and implementing the search attributes and/or memo provider interfaces. The framework then generates the necessary visibility tasks at the end of each transaction.

---

## How to Use the CHASM Framework

### 1. Define Your Component

A component is a plain Go struct. The CHASM framework uses reflection after each update to understand tree structure changes, detect which fields are dirty, serialize the component state, and make the update durable.

**Struct layout rules:**
- Embed `chasm.UnimplementedComponent`.
- Include exactly one `proto.Message` field. This is the component's own persisted state, serialized as the component's node in the tree.
- Declare child nodes as `chasm.Field[T]` fields (for data or nested components), `chasm.Map[K, T]` fields, or pointer fields.

```go
// mypb is your generated protobuf package
type MyComponent struct {
    chasm.UnimplementedComponent

    *mypb.MyState  // exactly one proto.Message — the component's own persisted state

    // Child fields — each becomes a separate node in the CHASM tree
    Visibility chasm.Field[*chasm.Visibility]
    Data       chasm.Field[*mypb.MyData]
}
```

**Application logic** is implemented as methods on the struct. Read-only methods should accept `chasm.Context`; methods that mutate state should accept `chasm.MutableContext`:

```go
// Read-only: uses chasm.Context
func (c *MyComponent) BuildResponse(ctx chasm.Context) *mypb.Response { ... }

// Mutating: uses chasm.MutableContext
func (c *MyComponent) HandleStarted(ctx chasm.MutableContext, req *mypb.StartRequest) error { ... }
```

**Lifecycle** is reported via `LifecycleState`. For the root component this controls the lifecycle of the entire execution—once it returns a closed state, the execution's business ID becomes available for reuse and the execution will be deleted after the namespace retention period:

```go
func (c *MyComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
    if c.MyState.GetStatus() == mypb.STATUS_DONE {
        return chasm.LifecycleStateCompleted
    }
    return chasm.LifecycleStateRunning
}
```

**Terminate** must be implemented by any root component. The framework calls it when it needs to force-close an execution (e.g., the tree has grown too large, or a namespace failover creates a conflict):

```go
func (c *MyComponent) Terminate(ctx chasm.MutableContext, req chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
    c.MyState.Status = mypb.STATUS_TERMINATED
    return chasm.TerminateComponentResponse{}, nil
}
```

### 2. Define Your Tasks

Tasks implement the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html): when a component's state is persisted, any tasks it scheduled are atomically written alongside it. The framework then processes those tasks asynchronously, retrying until the executor returns successfully or the validator returns false. This guarantees that scheduled work is never silently lost even if the process crashes between scheduling and execution.

Tasks are processed in FIFO order in general, but strict ordering is not guaranteed.

A **task** is the data payload that gets persisted alongside the component state and processe by the task handler. It can be defined in one of two ways:
- A bare `proto.Message` — for simple tasks that only need a small amount of data.
- A plain Go struct with exactly one embedded `proto.Message` field — the same layout rules as a component, for tasks that need additional non-persisted fields.

Every task handler implements two methods:
- **`Validate`** — called with the current component state before execution. Return `false` to drop the task (it is no longer relevant). Validators should invalidate tasks as early as possible to avoid unnecessary work.
- **`Execute`** — the actual task logic. The framework retries on error.

#### Choosing between Pure and Side Effect

The key distinction is whether the task touches the outside world:

- **Pure tasks** only mutate execution state—no I/O. Because they have no side effects, the framework can hold the execution lock, batch multiple pure tasks together, and flush them to persistence in a single write. This makes pure tasks efficient for in-process state transitions like applying a timeout.
- **Side effect tasks** interact with the outside world (calling matching, writing to an external service, etc.). They run after the transaction commits, without holding the execution lock, so they cannot directly mutate component state—they must use CHASM Engine functions to read/write component state, similar to [API handlers](#4-implement-api-handlers).

> **Important:** A pure task's validator **must** return `false` after the task has been executed (i.e., once the state transition it was waiting for has occurred). If the validator keeps returning `true`, the framework will not process any new delayed pure tasks for that execution.

#### Side Effect Task

```go
type myDispatchTaskHandler struct {
    matchingClient resource.MatchingClient
}

// Validate: drop the task if the component has already moved past the scheduled state.
func (e *myDispatchTaskHandler) Validate(
    ctx chasm.Context,
    c *MyComponent,
    _ chasm.TaskAttributes,
    task *mypb.MyDispatchTask,
) (bool, error) {
    return c.Status == mypb.MY_STATUS_SCHEDULED, nil
}

// Execute: runs outside the transaction; use chasm.ReadComponent to read state before acting.
func (e *myDispatchTaskHandler) Execute(
    ctx context.Context,
    ref chasm.ComponentRef,
    _ chasm.TaskAttributes,
    _ *mypb.MyDispatchTask,
) error {
    request, err := chasm.ReadComponent(ctx, ref, (*MyComponent).buildDispatchRequest, ref.NamespaceID)
    if err != nil {
        return err
    }
    _, err = e.matchingClient.AddActivityTask(ctx, request)
    return err
}
```

#### Pure Task

```go
type myTimeoutTaskHandler struct{}

// Validate: the task is only valid while the component is still in the scheduled state
// for this specific attempt (stamp). Once the state transitions away, this returns false
// and the framework drops the task—satisfying the requirement that pure task validators
// return false after execution.
func (e *myTimeoutTaskHandler) Validate(
    ctx chasm.Context,
    c *MyComponent,
    _ chasm.TaskAttributes,
    task *mypb.MyTimeoutTask,
) (bool, error) {
    return c.Status == mypb.MY_STATUS_SCHEDULED &&
        task.Stamp == c.Stamp, nil
}

// Execute: runs inside a transaction with full mutable access to the component.
func (e *myTimeoutTaskHandler) Execute(
    ctx chasm.MutableContext,
    c *MyComponent,
    _ chasm.TaskAttributes,
    _ *mypb.MyTimeoutTask,
) error {
    c.Status = mypb.MY_STATUS_TIMED_OUT
    return nil
}
```

### 3. Use Visibility

Visibility must be included in every execution today, as certain system operations (e.g., namespace migration) rely on visibility records.

Add a `Visibility` child field to the root component and initialize it in the constructor:

```go
type MyComponent struct {
    chasm.UnimplementedComponent
    *mypb.MyState
    Visibility chasm.Field[*chasm.Visibility]
}

func NewMyComponent(ctx chasm.MutableContext, req *mypb.StartRequest) *MyComponent {
    c := &MyComponent{MyState: &mypb.MyState{}}
    c.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibility(ctx))
    return c
}
```

There are two categories of search attributes and memo:

**1. Component search attributes & memo** — predefined by the application developer and tied to the component's own state. Implement `VisibilitySearchAttributesProvider` and/or `VisibilityMemoProvider` on the root component. The framework automatically calls these at the end of every update transaction and, if the values have changed, generates a visibility task to update the record:

```go
// Implement VisibilitySearchAttributesProvider to expose component state as search attributes.
func (c *MyComponent) SearchAttributes(ctx chasm.Context) []chasm.SearchAttributeKeyValue {
    return []chasm.SearchAttributeKeyValue{
        chasm.SearchAttributeKeyword("MyStatus").Value(c.GetStatus().String()),
    }
}
```

Component search attributes are also declared at registration time so the framework knows their types and aliases (see step 4, `WithSearchAttributes`).

**2. Custom search attributes & memo** — set at runtime by the caller to store arbitrary business information. Use the methods on the built-in `Visibility` component directly:

```go
// Merge caller-supplied search attributes into the visibility record
func (c *MyComponent) HandleStarted(ctx chasm.MutableContext, req *mypb.StartRequest) error {
    vis, _ := c.Visibility.TryGet(ctx)
    vis.MergeCustomSearchAttributes(ctx, req.GetSearchAttributes().GetIndexedFields())
    vis.MergeCustomMemo(ctx, req.GetMemo().GetFields())
    return nil
}
```

The `Visibility` component provides four methods: `MergeCustomSearchAttributes`, `ReplaceCustomSearchAttributes`, `MergeCustomMemo`, and `ReplaceCustomMemo`. Merge applies a diff (nil-value keys delete existing entries); Replace overwrites everything.

### 4. Implement API Handlers

CHASM engine functions are the bridge between the outside world and component business logic. An API handler receives an external request, calls an engine function with a closure containing the component logic to run, and the CHASM framework sits in the middle: it resolves the execution, acquires the appropriate lock, loads the CHASM tree, invokes the closure, persists any state changes and tasks, and finally returns control to the handler. This means the handler author only writes the business logic inside the closure—routing, sharding, locking, replication, and persistence are handled entirely by the framework.

The Go context passed into the gRPC handler already has the engine injected, so engine functions can be called directly:

| Function | Description |
|----------|-------------|
| `chasm.StartExecution` | Create a new execution and invoke the start closure to initialize its root component |
| `chasm.UpdateWithStartExecution` | Create-or-update in one atomic call |
| `chasm.UpdateComponent` | Load an existing component, invoke the update closure, and persist changes |
| `chasm.ReadComponent` | Load a component and invoke the read closure without persisting anything |
| `chasm.PollComponent` | Long-poll: repeatedly evaluate the predicate until it returns true or the deadline is reached |

> **Pro tip:** Component methods whose signatures match the expected closure type can be passed directly to engine functions—there is no need to wrap them in an anonymous function. For example, `chasm.UpdateComponent(ctx, ref, c.HandleCompleted, req)` works as long as `HandleCompleted` has the right signature. Use inline closures only when you need to combine multiple calls or add extra logic around the component method.

```go
// Start a new execution
func (h *myHandler) StartMyExecution(ctx context.Context, req *mypb.StartRequest) (*mypb.StartResponse, error) {
    result, err := chasm.StartExecution(
        ctx,
        chasm.ExecutionKey{
            NamespaceID: req.GetNamespaceId(),
            BusinessID:  req.GetMyId(),
        },
        func(mutableCtx chasm.MutableContext, req *mypb.StartRequest) (*MyComponent, error) {
            // Component logic: create and initialize the component.
            // The framework persists the returned component and any scheduled tasks.
            c := NewMyComponent(mutableCtx, req)
            return c, c.HandleStarted(mutableCtx, req)
        },
        req,
        chasm.WithRequestID(req.GetRequestId()),
        chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyFail),
    )
    if err != nil {
        return nil, err
    }
    return &mypb.StartResponse{RunId: result.ExecutionKey.RunID}, nil
}

// Update an existing component
func (h *myHandler) CompleteMyExecution(ctx context.Context, req *mypb.CompleteRequest) (*mypb.CompleteResponse, error) {
    _, _, err := chasm.UpdateComponent(
        ctx,
        ref, // chasm.ComponentRef or serialized []byte
        func(c *MyComponent, mutableCtx chasm.MutableContext, req *mypb.CompleteRequest) (chasm.NoValue, error) {
            // Component logic: mutate state. Framework persists changes after this returns.
            return nil, c.HandleCompleted(mutableCtx, req)
        }, // (*MyComponent).HandleCompleted,
        req,
    )
    return &mypb.CompleteResponse{}, err
}

// Long-poll until a condition is true
func (h *myHandler) PollMyExecution(ctx context.Context, req *mypb.PollRequest) (*mypb.PollResponse, error) {
    resp, _, err := chasm.PollComponent(
        ctx,
        ref,
        func(c *MyComponent, chasmCtx chasm.Context, _ *mypb.PollRequest) (*mypb.PollResponse, bool, error) {
            // Framework re-evaluates this predicate each time the execution is updated.
            // Return (result, true, nil) to stop polling; (nil, false, nil) to keep waiting.
            if c.LifecycleState(chasmCtx).IsClosed() {
                return buildPollResponse(c), true, nil
            }
            return nil, false, nil
        },
        req,
    )
    return resp, err
}
```

### 5. Register a Library

Create a library that collects your component and tasks, and registers any gRPC service handlers:

```go
type myLibrary struct {
    chasm.UnimplementedLibrary

    handler             *myHandler
    dispatchExecutor    *myDispatchTaskExecutor
    timeoutExecutor     *myTimeoutTaskExecutor
}

func (l *myLibrary) Name() string { return "mylib" }

func (l *myLibrary) Components() []*chasm.RegistrableComponent {
    return []*chasm.RegistrableComponent{
        chasm.NewRegistrableComponent[*MyComponent](
            "mycomponent",
            chasm.WithSearchAttributes(
                chasm.NewSearchAttributeKeyword("MyStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01),
            ),
            chasm.WithBusinessIDAlias("MyId"),
        ),
    }
}

func (l *myLibrary) Tasks() []*chasm.RegistrableTask {
    return []*chasm.RegistrableTask{
        chasm.NewRegistrableSideEffectTask(
            "dispatch",
            l.dispatchHandler,
            l.dispatchHandler,
        ),
        chasm.NewRegistrablePureTask(
            "timeout",
            l.timeoutHandler,
            l.timeoutHandler,
        ),
    }
}

func (l *myLibrary) RegisterServices(server *grpc.Server) {
    mypb.RegisterMyServiceServer(server, l.handler)
}
```

Register the library with the CHASM registry (typically via an Uber Fx module):

```go
var HistoryModule = fx.Module(
    "mylib-history",
    fx.Provide(
        newMyDispatchTaskHandler,
        newMyTimeoutTaskHandler,
        newMyHandler,
        newMyLibrary,
    ),
    fx.Invoke(func(l *myLibrary, registry *chasm.Registry) error {
        return registry.Register(l)
    }),
)

// Frontend nodes only need the component registered (for ComponentRef serialization)
var FrontendModule = fx.Module(
    "mylib-frontend",
    fx.Invoke(func(registry *chasm.Registry) error {
        return registry.Register(newComponentOnlyLibrary())
    }),
)
```

---

## Real-World Example: Standalone Activity

The [lib/activity](lib/activity/) library is the canonical example of a complete CHASM application.

**Component** ([activity.go](lib/activity/activity.go)): `Activity` struct with inline `*activitypb.ActivityState` and child fields for visibility, last attempt, heartbeat, request data, and outcome.

**State machine** ([statemachine.go](lib/activity/statemachine.go)): Declares all lifecycle transitions—`TransitionScheduled`, `TransitionStarted`, `TransitionCompleted`, `TransitionFailed`, `TransitionTimedOut`, `TransitionCanceled`, etc.—each specifying valid source states, destination state, and the mutation function.

**Tasks** ([activity_tasks.go](lib/activity/activity_tasks.go)):
- `activityDispatchTaskExecutor` — side effect task that calls `matching.AddActivityTask`
- `scheduleToStartTimeoutTaskExecutor` — pure task applying `TransitionTimedOut`
- `scheduleToCloseTimeoutTaskExecutor` — pure task applying `TransitionTimedOut`
- `startToCloseTimeoutTaskExecutor` — pure task with retry support
- `heartbeatTimeoutTaskExecutor` — pure task with heartbeat freshness validation

**Library** ([library.go](lib/activity/library.go)): Registers the component with search attributes (`ActivityType`, `ExecutionStatus`, `TaskQueue`) and a `BusinessIDAlias` of `"ActivityId"`. Registers all five tasks and wires the gRPC service handler.

**Fx modules** ([fx.go](lib/activity/fx.go)): `HistoryModule` wires the full library; `FrontendModule` registers only the component (for ComponentRef serialization on the frontend).

---

## Limitations

- **No history event support**: CHASM executions do not produce Temporal history events. There is no event history replay or versioning.
- **Single persistence unit**: The entire CHASM tree is stored as a single collection or blob in certain persistence implementations. Total tree size is bounded.
- **No partial loading**: The entire CHASM tree is loaded on every access. There is no partial read capability.
- **No test framework**: There is currently no dedicated testing framework for CHASM components and tasks.
- **Visibility is required**: Every execution must include a `Visibility` child component. System operations such as namespace migration depend on visibility records being present for all executions.
