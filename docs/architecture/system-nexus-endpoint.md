# System Nexus Endpoint Design

## Overview

This document proposes a **System Nexus Endpoint** feature that enables workflows to invoke built-in Temporal server operations using the existing Nexus infrastructure. Instead of adding new commands and history events for each new workflow capability, we leverage the Nexus operation model to extend workflow functionality in a uniform, extensible manner.

### Motivation

Issue [#680](https://github.com/temporalio/temporal/issues/680) requests the ability for a workflow to wait for the completion of an external workflow. The traditional approach would require:
- New `WaitForExternalWorkflowCompletion` command type
- New history events (Scheduled, Completed, Failed, etc.)
- Wiring through multiple system components (frontend, history, SDK)

This pattern doesn't scale well as we accumulate feature requests:
- SignalWithStartWorkflow from within a workflow 
- UpdateWorkflowExecution to another workflow
- Query another workflow
- Other cross-workflow coordination patterns

### Solution: System Nexus Endpoint

Rather than adding new commands, we introduce a **system nexus endpoint** - a built-in endpoint provided by the Temporal server that workflows can invoke like any user-defined Nexus endpoint. The key differences:

1. **No user-defined endpoint required** - workflows specify they're calling the "system" endpoint
2. **Server-side handling** - instead of routing to an external HTTP handler or worker, requests route directly to History service
3. **Reuses existing infrastructure** - Nexus operation state machines, outbound queues, callbacks, and events
4. **Extensible** - new system operations can be added without new command types

## Design Goals

1. **Minimal SDK changes** - SDKs should be able to invoke system operations with minimal new API surface
2. **Reuse existing Nexus infrastructure** - leverage outbound queues, state machines, callbacks, timeouts
3. **Extensible operation model** - easy to add new system operations (SignalWithStart, Update, etc.)
4. **Strict namespace isolation** - system operations only work within the same namespace (see [Namespace Isolation](#namespace-isolation) section)
5. **No new history events** - use existing NexusOperation* events
6. **Consistent semantics** - system operations behave like regular Nexus operations from the workflow's perspective

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CALLER WORKFLOW                                 │
│                                                                             │
│  workflow.WaitExternalWorkflow(ctx, workflow.WaitExternalWorkflowOptions{   │
│      WorkflowID: "target-workflow-id",                                      │
│      ScheduleToCloseTimeout: 30 * time.Second,                              │
│  })                                                                         │
│  // Translates to: ScheduleNexusOperation with endpoint="__temporal_system" │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WORKFLOW TASK COMPLETED HANDLER                          │
│                                                                             │
│  ScheduleNexusOperation command with endpoint="__temporal_system"           │
│  → Validate system endpoint and operation via Registry.IsKnownOperation()   │
│  → Create NexusOperationScheduledEvent (EndpointId="" for system endpoint)  │
│  → Create Operation state machine (SCHEDULED state)                         │
│  → Generate InvocationTask on outbound queue                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         OUTBOUND QUEUE PROCESSOR                            │
│                                                                             │
│  InvocationTask executor (executeInvocationTask):                           │
│  1. Load operation state and endpoint info                                  │
│  2. Detect system endpoint (nexus.IsSystemEndpoint(endpoint))               │
│  3. Route to executeSystemOperation() instead of HTTP client                │
│  4. Calls SystemOperationExecutor.Execute() which dispatches to handler     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SYSTEM OPERATION HANDLER                             │
│                                                                             │
│  For "WaitExternalWorkflowCompletion":                                      │
│  1. Parse JSON input: extract target workflow_id, run_id                    │
│  2. Check if workflow is closed via GetWorkflowExecutionHistoryReverse      │
│  3. If already closed → Return sync response with result from close event   │
│  4. If running → Register callback via UpdateWorkflowExecutionOptions       │
│     - Callback URL: "temporal://system-nexus-complete"                      │
│     - Header: callback token identifying caller's Nexus operation           │
│  5. Return async operation response (OperationToken = workflowID:runID)     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TARGET WORKFLOW COMPLETES                           │
│                                                                             │
│  1. processCloseCallbacks() triggers registered callbacks                   │
│  2. Callback executor sees URL "temporal://system-nexus-complete"           │
│  3. Routes to systemNexusInvocation handler (not HTTP)                      │
│  4. Handler extracts callback token, builds CompleteNexusOperation request  │
│  5. Calls HistoryClient.CompleteNexusOperation() to complete caller's op    │
│  6. Caller workflow receives NexusOperationCompletedEvent                   │
│  7. SDK processes completion, WaitExternalWorkflow returns result           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Detailed Design

### 1. System Endpoint Identification

We define a reserved endpoint name that identifies system operations:

```go
const SystemEndpointName = "__temporal_system"
```

**Alternatives considered:**
- Empty endpoint name: Could conflict with validation
- Special prefix (e.g., `temporal://`): More complex parsing
- Separate field in command: Requires proto changes

The `__temporal_system` name:
- Uses double underscore prefix (reserved, unlikely to conflict with user endpoints)
- Clear and explicit
- Works with existing string endpoint field

### 2. System Service and Operations

System operations are organized under a service namespace:

```
Service: "temporal.system.v1"

Operations:
├── WaitExternalWorkflowCompletion
├── SignalWithStartWorkflow (future)
├── UpdateWorkflowExecution (future)
├── QueryWorkflow (future)
└── ... (extensible)
```

**Operation Input/Output Schemas** (using JSON serialization):

```go
// WaitExternalWorkflowCompletionInput is the input for the operation.
type WaitExternalWorkflowCompletionInput struct {
    WorkflowID string `json:"workflow_id"`
    RunID      string `json:"run_id,omitempty"` // Optional - if empty, uses the current run
}

// WaitExternalWorkflowCompletionOutput is the output of the operation.
type WaitExternalWorkflowCompletionOutput struct {
    Status  enumspb.WorkflowExecutionStatus `json:"status"`  // COMPLETED, FAILED, CANCELED, etc.
    Result  *commonpb.Payload               `json:"result,omitempty"`  // Result if completed
    Failure *failurepb.Failure              `json:"failure,omitempty"` // Failure details
}
```

**Continue-as-new behavior**: When the target workflow performs continue-as-new, the wait operation automatically follows the chain until the workflow reaches a terminal state. This is consistent with existing Temporal behavior for child workflows and other workflow-waiting patterns throughout the system.

### 3. Command Handling Changes

**File:** `components/nexusoperations/workflow/commands.go`

Modify `HandleScheduleCommand()` to handle system endpoint:

```go
func (v commandHandler) HandleScheduleCommand(...) error {
    attrs := command.GetScheduleNexusOperationCommandAttributes()

    // Check for system endpoint
    if attrs.Endpoint == SystemEndpointName {
        return v.handleSystemEndpointCommand(ctx, ms, validator, workflowTaskCompletedEventID, attrs)
    }

    // Existing user endpoint handling...
}

func (v commandHandler) handleSystemEndpointCommand(...) error {
    // 1. Validate operation is known
    if !isKnownSystemOperation(attrs.Service, attrs.Operation) {
        return serviceerror.NewInvalidArgument("unknown system operation: %s/%s", attrs.Service, attrs.Operation)
    }

    // 2. Validate operation-specific input
    if err := validateSystemOperationInput(attrs.Service, attrs.Operation, attrs.Input); err != nil {
        return err
    }

    // 3. Create history event (no endpoint ID since it's system)
    event := ms.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, func(he *historypb.HistoryEvent) {
        he.Attributes = &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
            NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
                Endpoint:   SystemEndpointName,
                EndpointId: "",  // Empty for system endpoint
                Service:    attrs.Service,
                Operation:  attrs.Operation,
                Input:      attrs.Input,
                // ... other fields
            },
        }
    })

    // 4. Create state machine
    return nexusoperations.ScheduledEventDefinition{}.Apply(root, event)
}
```

### 4. System Operation Registry and Handlers

**New package:** `components/nexusoperations/system/`

The system operation infrastructure is organized as follows:

**File: `components/nexusoperations/system/registry.go`**

```go
// OperationHandler handles a specific system operation.
type OperationHandler interface {
    // Name returns the name of the operation this handler handles.
    Name() string
    // Execute runs the system operation and returns the result.
    Execute(ctx context.Context, execCtx ExecutionContext, args OperationArgs) (OperationResult, error)
}

// ExecutionContext provides access to history service internals.
type ExecutionContext struct {
    ShardContext               historyi.ShardContext
    WorkflowConsistencyChecker api.WorkflowConsistencyChecker
    // CallbackTokenGenerator generates a callback token for async completion.
    // Called lazily only when the operation needs to go async.
    CallbackTokenGenerator     func() (string, error)
}

// OperationArgs contains the arguments passed to a system operation handler.
type OperationArgs struct {
    NamespaceID      namespace.ID
    NamespaceName    namespace.Name
    CallerWorkflowID string
    CallerRunID      string
    CallerRef        hsm.Ref
    Service          string
    Operation        string
    Input            *commonpb.Payload  // Single payload, JSON serialized
    Header           map[string]string
    RequestID        string
}

// OperationResult contains the result of a system operation execution.
// Completion is inferred: if Output or Failure is set, completed synchronously.
// If both are nil, the operation is async and OperationToken should be set.
type OperationResult struct {
    Output         *commonpb.Payload   // Sync success result
    Failure        *failurepb.Failure  // Sync failure result
    OperationToken string              // Async operation token
}

// Registry manages registered system operations and dispatches execution requests.
type Registry struct {
    handlers map[string]OperationHandler  // operation name -> handler
}

func (r *Registry) Register(handler OperationHandler) { ... }
func (r *Registry) Execute(ctx, execCtx, args) (OperationResult, error) { ... }
func (r *Registry) IsKnownOperation(service, operation string) bool { ... }
```

**File: `components/nexusoperations/system/fx.go`** (FX wiring)

```go
var Module = fx.Module(
    "component.nexusoperations.system",
    fx.Provide(RegistryProvider),
    fx.Provide(SystemOperationExecutorProvider),
)

func RegistryProvider() *Registry {
    registry := NewRegistry()
    RegisterWaitExternalWorkflowCompletion(registry)
    return registry
}
```

### 5. Invocation Task Executor Changes

**File:** `components/nexusoperations/executors.go`

Modify `executeInvocationTask()` to handle system endpoint:

```go
func (e taskExecutor) executeInvocationTask(ctx context.Context, ...) error {
    args, err := e.loadOperationArgs(ctx, ns, env, ref)
    if err != nil {
        return err
    }

    // Check for system endpoint
    if args.endpointName == SystemEndpointName {
        return e.executeSystemOperation(ctx, ns, env, ref, task, args)
    }

    // Existing user endpoint execution...
}

func (e taskExecutor) executeSystemOperation(ctx context.Context, ...) error {
    // Generate callback token for async completion
    token, err := e.CallbackTokenGenerator.Tokenize(&tokenspb.NexusOperationCompletion{
        NamespaceId: ref.WorkflowKey.NamespaceID,
        WorkflowId:  ref.WorkflowKey.WorkflowID,
        RunId:       ref.WorkflowKey.RunID,
        Ref:         smRef,
        RequestId:   args.requestID,
    })
    if err != nil {
        return err
    }

    // Execute via system operation registry
    result, err := e.SystemOperationRegistry.Execute(ctx, SystemOperationArgs{
        NamespaceID:   namespace.ID(ref.WorkflowKey.NamespaceID),
        NamespaceName: ns.Name(),
        CallerRef:     ref,
        Service:       args.service,
        Operation:     args.operation,
        Input:         args.payload,
        Header:        args.header,
        CallbackToken: token,
        RequestID:     args.requestID,
    })

    if err != nil {
        // Handle retryable vs non-retryable errors
        return e.saveOperationError(ctx, env, ref, err)
    }

    if result.Completed {
        // Sync completion - transition to succeeded/failed
        return e.saveOperationResult(ctx, env, ref, result)
    }

    // Async operation - transition to started state
    return e.saveOperationStarted(ctx, env, ref, result.OperationToken)
}
```

### 6. WaitExternalWorkflowCompletion Implementation

**File:** `components/nexusoperations/system/wait_external_workflow.go`

```go
type WaitExternalWorkflowCompletionHandler struct{}

func (h *WaitExternalWorkflowCompletionHandler) Name() string {
    return commonnexus.WaitExternalWorkflowCompletionOperationName
}

func (h *WaitExternalWorkflowCompletionHandler) Execute(
    ctx context.Context,
    execCtx ExecutionContext,
    args OperationArgs,
) (OperationResult, error) {
    // 1. Parse JSON input
    var input WaitExternalWorkflowCompletionInput
    if err := json.Unmarshal(args.Input.GetData(), &input); err != nil {
        return OperationResult{}, serviceerror.NewInvalidArgument("invalid input")
    }

    // 2. Get history client from shard context for cross-shard routing
    historyClient := execCtx.ShardContext.GetHistoryClient()

    // 3. Check if workflow is already closed via GetWorkflowExecutionHistoryReverse
    //    (Efficient: fetches only the last event with page size 1)
    histResp, err := historyClient.GetWorkflowExecutionHistoryReverse(ctx, ...)
    if err != nil {
        if _, ok := err.(*serviceerror.NotFound); ok {
            return OperationResult{
                Failure: &failurepb.Failure{Message: "workflow not found", ...},
            }, nil
        }
        return OperationResult{}, err
    }

    // 4. If last event is a close event, return sync result
    lastEvent := histResp.GetResponse().GetHistory().GetEvents()[0]
    if status, isClose := h.getCloseEventStatus(lastEvent); isClose {
        output := &WaitExternalWorkflowCompletionOutput{Status: status}
        h.populateOutputFromCompletionEvent(output, status, lastEvent)
        return h.buildResultFromOutput(output)  // Sync completion
    }

    // 5. Workflow is running - generate callback token lazily
    callbackToken, err := execCtx.CallbackTokenGenerator()
    if err != nil {
        return OperationResult{}, err
    }

    // 6. Build callback with system nexus completion URL
    callback := &commonpb.Callback{
        Variant: &commonpb.Callback_Nexus_{
            Nexus: &commonpb.Callback_Nexus{
                Url: commonnexus.SystemNexusCompletionURL,  // "temporal://system-nexus-complete"
                Header: map[string]string{
                    strings.ToLower(commonnexus.CallbackTokenHeader): callbackToken,
                },
            },
        },
    }

    // 7. Attach callback via UpdateWorkflowExecutionOptions
    _, err = historyClient.UpdateWorkflowExecutionOptions(ctx, &historyservice.UpdateWorkflowExecutionOptionsRequest{
        NamespaceId: args.NamespaceID.String(),
        UpdateRequest: &workflowservice.UpdateWorkflowExecutionOptionsRequest{
            Namespace:         args.NamespaceName.String(),
            WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: input.WorkflowID, RunId: runID},
            UpdateMask:        &fieldmaskpb.FieldMask{},  // Empty - only attaching callbacks
            Identity:          "system-nexus-wait-external-workflow",
        },
        CompletionCallbacks: []*commonpb.Callback{callback},
        AttachRequestId:     args.RequestID,
    })
    if err != nil {
        return OperationResult{}, err
    }

    // 8. Return async result
    return OperationResult{
        OperationToken: fmt.Sprintf("%s:%s", input.WorkflowID, runID),
    }, nil
}
```

### 7. Callback Completion Handling

When the target workflow completes, a new `systemNexusInvocation` handler processes the callback:

**File:** `components/callbacks/system_nexus_invocation.go`

1. Target workflow closes → `processCloseCallbacks()` triggers registered callbacks
2. Callback executor sees URL `temporal://system-nexus-complete`
3. Routes to `systemNexusInvocation.Invoke()` instead of HTTP client
4. Handler extracts callback token from header, decodes it to get caller's operation reference
5. Builds `CompleteNexusOperationRequest` with the workflow completion result:
   - Successful completion → wraps result in `WaitExternalWorkflowCompletionOutput`
   - Failed/canceled/terminated → maps to appropriate status and failure
6. Calls `HistoryClient.CompleteNexusOperation()` to complete caller's Nexus operation
7. Caller workflow receives `NexusOperationCompletedEvent`

```go
type systemNexusInvocation struct {
    nexus      *persistencespb.Callback_Nexus
    completion nexusrpc.OperationCompletion
    ...
}

func (h systemNexusInvocation) Invoke(ctx context.Context, ...) invocationResult {
    // Extract callback token from header
    encodedToken := header.Get(commonnexus.CallbackTokenHeader)
    token, _ := commonnexus.DecodeCallbackToken(encodedToken)
    completionToken, _ := tokenGenerator.DecodeCompletion(token)

    // Build completion request based on workflow completion state
    request := h.buildCompleteRequest(completionToken)

    // Complete the caller's Nexus operation
    _, err := e.HistoryClient.CompleteNexusOperation(ctx, request)
    return invocationResultOK{}
}
```

### 8. SDK API (Conceptual)

SDKs would expose system operations through a dedicated API:

**Go SDK Example:**
```go
// Option 1: Dedicated method
result, err := workflow.WaitExternalWorkflow(ctx, workflow.WaitExternalWorkflowOptions{
    WorkflowID: "target-workflow-id",
    RunID:      "",  // Empty = current run; always follows continue-as-new chain
})

// Option 2: Generic system operation
result, err := workflow.ExecuteSystemOperation(ctx, workflow.SystemOperationOptions{
    Operation: "WaitExternalWorkflowCompletion",
    Input:     WaitExternalWorkflowCompletionInput{WorkflowID: "target"},
})

// Under the hood, both translate to:
// ScheduleNexusOperation with endpoint="__temporal_system"
```

## Extension: Future System Operations

### SignalWithStartWorkflow

```go
type SignalWithStartHandler struct {
    historyClient historyservice.HistoryServiceClient
}

func (h *SignalWithStartHandler) Execute(ctx context.Context, args SystemOperationArgs) (SystemOperationResult, error) {
    var input SignalWithStartInput
    // Parse input...

    resp, err := h.historyClient.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
        NamespaceId: args.NamespaceID.String(),
        // ... build request from input
    })

    // Can return sync or async based on whether we want to wait for result
    return SystemOperationResult{
        Completed: true,
        Output:    encodeOutput(SignalWithStartOutput{RunId: resp.RunId}),
    }, nil
}
```

### UpdateWorkflowExecution

```go
type UpdateWorkflowHandler struct {
    historyClient historyservice.HistoryServiceClient
}

func (h *UpdateWorkflowHandler) Execute(ctx context.Context, args SystemOperationArgs) (SystemOperationResult, error) {
    var input UpdateWorkflowInput
    // Parse input...

    // This is naturally async - update needs to wait for workflow task
    resp, err := h.historyClient.UpdateWorkflowExecution(ctx, &historyservice.UpdateWorkflowExecutionRequest{
        // ...
    })

    // Return async - update handler will callback when complete
    return SystemOperationResult{
        Completed:      false,
        OperationToken: resp.GetUpdateRef().GetUpdateId(),
    }, nil
}
```

## Configuration

```go
// Dynamic config keys
SystemNexusEndpointEnabled = NewGlobalBoolSetting(
    "component.nexusoperations.systemEndpoint.enabled",
    true,
    "Enable system nexus endpoint for workflow-to-server operations",
)
```

System operations reuse existing Nexus operation configuration:
- Timeouts: Use standard Nexus operation timeout settings
- Callback limits: Use `system.maxCallbacksPerWorkflow` (shared with all callbacks)
- Pending operations: Use existing `MaxConcurrentOperations` limit per workflow

## Implementation Phases

### Phase 1: Infrastructure (Foundation)
1. Add `SystemEndpointName` constant and validation
2. Create `SystemOperationRegistry` and `SystemOperationExecutor` interface
3. Modify command handler to recognize system endpoint
4. Modify invocation task executor to route to system executor
5. Add configuration and metrics

### Phase 2: WaitExternalWorkflowCompletion
1. Implement `WaitExternalWorkflowHandler`
2. Handle sync completion (workflow already closed)
3. Handle async completion (attach callback, wait for close)
4. Handle edge cases (workflow not found, continue-as-new, etc.)
5. Add integration tests

### Phase 3: SDK Support
1. Add Go SDK support for system operations
2. Add TypeScript SDK support
3. Add other SDK support
4. Documentation

### Phase 4: Additional Operations
1. SignalWithStartWorkflow
2. UpdateWorkflowExecution
3. QueryWorkflow (sync operation)
4. Others as needed

## Namespace Isolation

**System Nexus Endpoint does NOT support cross-namespace operations.** This is a deliberate design decision, not a limitation to be relaxed later.

### Rationale

Namespaces are Temporal's primary resource isolation boundary. Cross-namespace access is intentionally restricted:

1. **User-defined Nexus endpoints**: When users create their own Nexus endpoints and operations, they explicitly opt-in to expose cross-namespace access. The target namespace consciously decides to offer certain operations to callers from other namespaces.

2. **System Nexus Endpoint**: This is a built-in feature offered by the Temporal server. It should not break the namespace isolation promise that users rely on. Allowing system operations to bypass namespace boundaries would be a security and isolation violation.

3. **Simplified security model**: By not allowing cross-namespace access, we avoid the complexity of implementing authorization checks, permission models, and access control policies for system operations. The security model is straightforward: system operations can only affect resources within the caller's namespace. This eliminates an entire class of potential security vulnerabilities and configuration errors.

### Cross-Namespace Use Case: Recommended Pattern

If a workflow needs to wait for completion of a workflow in another namespace, the recommended approach is:

1. **Target namespace exposes a dedicated Nexus operation**: The target namespace creates a Nexus endpoint with an operation like `WaitWorkflowCompletion` that allows external callers.

2. **Caller invokes the user-defined Nexus operation**: The caller workflow uses the standard Nexus operation invocation to call the target namespace's endpoint.

```go
// In caller workflow (namespace A):
result, err := workflow.ExecuteNexusOperation(ctx, workflow.NexusOperationOptions{
    Endpoint:  "namespace-b-workflow-service",  // User-defined endpoint
    Service:   "workflow-management",
    Operation: "WaitWorkflowCompletion",
    Input:     WaitWorkflowInput{WorkflowID: "target-workflow"},
})
```

This pattern:
- Maintains explicit opt-in for cross-namespace access
- Allows target namespace to control authorization and access policies
- Follows the principle of least privilege
- Is consistent with how cross-namespace communication works elsewhere in Temporal

## Design Decisions

### Callback Limits

System Nexus Endpoint operations that register callbacks (e.g., `WaitExternalWorkflowCompletion`) use the same callback limit as regular Nexus operations. There is no separate limit for system operations.

The max callbacks per workflow limit is being increased from 32 to 2000 (`system.maxCallbacksPerWorkflow`), which provides sufficient headroom for typical use cases. This simplifies the design by avoiding special-case limits for system operations.

### Timeout Semantics

System Nexus Endpoint operations use the same timeout configuration as standard Nexus operations. This includes:
- `ScheduleToCloseTimeout` - overall operation timeout
- `ScheduleToStartTimeout` - time to start the operation
- `StartToCloseTimeout` - time after operation starts

No separate timeout configuration is needed for system operations. This maintains consistency and simplifies the configuration model.

### Error Handling

System operations follow standard retry semantics based on error type:

**Retryable errors** (operation will retry automatically):
- Network/RPC errors (Unavailable, DeadlineExceeded, ResourceExhausted)
- Shard ownership changes (ShardOwnershipLost)
- Temporary database errors, transaction conflicts
- Rate limiting errors

**Race condition errors** (special handling):
- Workflow completed between describe and callback registration: Re-describe to get final result, return sync completion
- Workflow continued-as-new between calls: Follow the new run ID and retry

**Non-retryable errors** (fail immediately with sync failure response):
- `NOT_FOUND`: Target workflow doesn't exist - return sync failure to caller
- Callback limit exceeded: Target workflow has max callbacks - return sync failure to caller
- Invalid input: Empty workflow ID, malformed payload - return sync failure to caller

### Cancellation Semantics

When a `WaitExternalWorkflowCompletion` operation is canceled:

1. **Cancel the wait operation**: The caller's Nexus operation transitions to canceled state
2. **Remove the callback from target**: The registered callback is removed from the target workflow
3. **Do NOT cancel the target workflow**: The target workflow continues running unaffected

This follows the principle that canceling a "wait" operation only stops the waiting, not the thing being waited on. The caller is saying "I don't want to wait anymore" but has no authority to interrupt the target workflow's execution.

## Metrics

```go
// New metrics for system operations
system_operation_requests_total{service, operation, status}
system_operation_latency_seconds{service, operation}
system_operation_callback_registrations_total{operation, status}
system_operation_sync_completions_total{operation}
system_operation_async_completions_total{operation}
```

## Security Considerations

1. **Namespace isolation**: System operations strictly enforce namespace boundaries - cross-namespace operations are not supported (see [Namespace Isolation](#namespace-isolation))
2. **Rate limiting**: System operations should be subject to rate limiting to prevent abuse
3. **Callback validation**: Ensure callbacks can only target the original caller workflow
4. **Input validation**: All system operation inputs must be validated to prevent injection attacks or resource exhaustion

## References

- [Issue #680: Add ability for workflow to wait for completion of an external workflow](https://github.com/temporalio/temporal/issues/680)
- [Nexus Architecture Documentation](./nexus.md)
- [Callback System Implementation](../../components/callbacks/)
- [Nexus Operations Implementation](../../components/nexusoperations/)
