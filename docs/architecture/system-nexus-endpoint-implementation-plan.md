# System Nexus Endpoint Implementation Plan

Based on the design document at `docs/architecture/system-nexus-endpoint.md`, this plan outlines the implementation tasks across the Temporal server and Go SDK repositories.

---

## Overview

The System Nexus Endpoint enables workflows to invoke built-in Temporal server operations (like `WaitExternalWorkflowCompletion`) using the existing Nexus infrastructure. Key aspects:

- **Reserved endpoint name**: `__temporal_system`
- **No cross-namespace operations** (strict namespace isolation)
- **Reuses existing Nexus state machines, callbacks, timeouts**
- **First operation**: `WaitExternalWorkflowCompletion`

---

## Phase 1: Infrastructure (Temporal Server) ✅ COMPLETED

### 1.1 Add System Endpoint Constants and Validation ✅

**File:** `common/nexus/constants.go`

**Implemented:**
- [x] `SystemEndpointName = "__temporal_system"` constant
- [x] `SystemServiceName = "temporal.system.v1"` constant
- [x] `WaitExternalWorkflowCompletionOperationName = "WaitExternalWorkflowCompletion"` constant
- [x] `SystemNexusCompletionURL = "temporal://system-nexus-complete"` for callback completion
- [x] `IsSystemEndpoint(endpoint string) bool` validation function

### 1.2 Create System Operation Registry and Handler Interface ✅

**New package:** `components/nexusoperations/system/`

**Files created:**
- `components/nexusoperations/system/registry.go` - Registry and interfaces
- `components/nexusoperations/system/fx.go` - FX module wiring

**Implemented:**
- [x] `OperationHandler` interface with `Name()` and `Execute()` methods
- [x] `ExecutionContext` struct with:
  - `ShardContext` (historyi.ShardContext)
  - `WorkflowConsistencyChecker`
  - `CallbackTokenGenerator func() (string, error)` - lazy token generation
- [x] `OperationArgs` struct with:
  - NamespaceID, NamespaceName, CallerWorkflowID, CallerRunID
  - CallerRef (hsm.Ref)
  - Service, Operation
  - Input (`*commonpb.Payload` - single payload, JSON serialized)
  - Header, RequestID
- [x] `OperationResult` struct with:
  - Output (`*commonpb.Payload`) - sync success
  - Failure (`*failurepb.Failure`) - sync failure
  - OperationToken (string) - async operation
  - Note: Completion inferred from Output/Failure being set (no explicit `Completed` bool)
- [x] `Registry` with `Register()`, `Execute()`, and `IsKnownOperation()` methods

### 1.3 Modify Command Handler for System Endpoint ✅

**File:** `components/nexusoperations/workflow/commands.go`

**Implemented:**
- [x] Detect system endpoint via `nexus.IsSystemEndpoint(attrs.Endpoint)`
- [x] Validate operation via `SystemOperationRegistry.IsKnownOperation()`
- [x] Create `NexusOperationScheduledEvent` with `EndpointId=""` for system endpoint
- [x] Skip endpoint registry lookup for system endpoints

### 1.4 Modify Invocation Task Executor for System Endpoint ✅

**File:** `components/nexusoperations/executors.go`

**Implemented:**
- [x] Added `SystemOperationExecutor` interface in executors.go
- [x] Added `SystemExecutionContext` and `SystemOperationArgs` types for executor interface
- [x] In `executeInvocationTask()`, detect system endpoint and route to `executeSystemOperation()`
- [x] `executeSystemOperation()` method that:
  - Builds `SystemOperationArgs` from operation state
  - Creates `SystemExecutionContext` with lazy callback token generator
  - Calls `SystemOperationExecutor.Execute()`
  - Handles sync completion (Output or Failure set)
  - Handles async operation (OperationToken set)
- [x] `saveSystemOperationResult()` for persisting results

### 1.5 Add Configuration and Metrics

**Status:** Not yet implemented (deferred to future iteration)

### 1.6 Wire Up System Operation Registry in FX Module ✅

**Files:**
- `components/nexusoperations/system/fx.go` - New FX module
- `service/history/fx.go` - Include system module

**Implemented:**
- [x] `system.Module` FX module with `RegistryProvider` and `SystemOperationExecutorProvider`
- [x] `systemOperationExecutorAdapter` to adapt `*Registry` to `nexusoperations.SystemOperationExecutor`
- [x] Module included in history service FX graph

---

## Phase 2: WaitExternalWorkflowCompletion (Temporal Server) ✅ COMPLETED

### 2.1 Define Input/Output Types ✅

**Decision:** Used Go structs with JSON serialization (not proto)

**File:** `components/nexusoperations/system/wait_external_workflow.go`

**Implemented:**
- [x] `WaitExternalWorkflowCompletionInput` struct:
  - `WorkflowID string` (json: `workflow_id`)
  - `RunID string` (json: `run_id`, optional)
- [x] `WaitExternalWorkflowCompletionOutput` struct:
  - `Status enumspb.WorkflowExecutionStatus` (json: `status`)
  - `Result *commonpb.Payload` (json: `result`, optional)
  - `Failure *failurepb.Failure` (json: `failure`, optional)

### 2.2 Implement WaitExternalWorkflowCompletion Handler ✅

**File:** `components/nexusoperations/system/wait_external_workflow.go`

**Implemented:**
- [x] `WaitExternalWorkflowCompletionHandler` struct (stateless - no stored clients)
- [x] Gets `historyClient` from `execCtx.ShardContext.GetHistoryClient()` for cross-shard routing
- [x] `Execute()` method:
  1. Parse JSON input and validate (workflow_id required)
  2. Check if workflow is closed via `GetWorkflowExecutionHistoryReverse` (efficient: page size 1)
  3. If workflow not found → return sync failure with `WorkflowNotFound` type
  4. If workflow already closed → return sync result from close event
  5. If workflow running → attach callback via `UpdateWorkflowExecutionOptions`
  6. Return async result with `OperationToken = workflowID:runID`
- [x] Helper methods: `getCloseEventStatus()`, `populateOutputFromCompletionEvent()`, `buildResultFromOutput()`

### 2.3 Implement Callback Attachment and Completion ✅

**Files:**
- `components/nexusoperations/system/wait_external_workflow.go` - Callback attachment
- `components/callbacks/system_nexus_invocation.go` - Callback completion handler (NEW)

**Implemented:**
- [x] Callback attachment via `historyClient.UpdateWorkflowExecutionOptions()`:
  - Uses `CompletionCallbacks` field (not `AttachedCompletionCallbacks`)
  - Uses `AttachRequestId` for idempotency
  - Callback URL: `temporal://system-nexus-complete` (not `temporal://system`)
  - Callback token in header (lowercase key for compatibility)
- [x] `systemNexusInvocation` handler in callbacks package:
  - Handles callbacks with URL `temporal://system-nexus-complete`
  - Extracts callback token from header
  - Decodes token to get caller's Nexus operation reference
  - Builds `CompleteNexusOperationRequest` with workflow completion state
  - Maps workflow completion to `WaitExternalWorkflowCompletionOutput`
  - Calls `HistoryClient.CompleteNexusOperation()` to complete caller's operation
- [x] Lazy callback token generation (only generated when going async)

### 2.4 Handle Sync Completion Results ✅

**Implemented:**
- [x] `populateOutputFromCompletionEvent()` extracts result from close event:
  - COMPLETED → extract result from `WorkflowExecutionCompletedEventAttributes`
  - FAILED → extract failure from `WorkflowExecutionFailedEventAttributes`
  - CANCELED → create `CanceledFailure` with details
  - TERMINATED → create `TerminatedFailure` with reason
  - TIMED_OUT → create `TimeoutFailure`
  - CONTINUED_AS_NEW → handled as close event
- [x] `buildResultFromOutput()` serializes output to JSON payload

### 2.5 Functional Tests ✅

**File:** `tests/system_nexus_wait_workflow_test.go`

**Implemented:**
- [x] `TestWaitExternalWorkflowCompletion` - Full async completion flow:
  - Workflow A starts Workflow B
  - A spawns coroutine to signal B after 1s
  - A waits for B with `workflow.WaitExternalWorkflow()`
  - B receives signal and completes
  - A receives result and verifies
- [x] `TestSimpleWaitExternalWorkflowSchedule` - Tests operation scheduling
- [x] `TestWaitExternalWorkflowAlreadyCompleted` - Sync completion path
- [x] `TestWaitExternalWorkflowFailed` - Target workflow fails

---

## Phase 3: SDK Support (sdk-go) ✅ COMPLETED

SDK implementation is complete. See sdk-go repository for details.

### 3.1 Add System Nexus Client ✅

**Implemented:**
- [x] Allow `__temporal_` prefix for system endpoints
- [x] Internal system Nexus client uses `__temporal_system` endpoint

### 3.2 Add WaitExternalWorkflow API ✅

**Implemented:**
- [x] `WaitExternalWorkflowOptions` struct with WorkflowID, RunID, ScheduleToCloseTimeout
- [x] `WaitExternalWorkflowResult` struct with Status, Get(), GetFailure() methods
- [x] `WaitExternalWorkflow(ctx Context, opts WaitExternalWorkflowOptions) WaitExternalWorkflowFuture`
- [x] `WaitExternalWorkflowFuture` interface

### 3.3-3.5 Internal Implementation and Interceptors ✅

**Implemented:**
- [x] Internal `WaitExternalWorkflow()` function
- [x] Interceptor interface and base implementation
- [x] JSON serialization for input/output matching server expectations

---

## Phase 4: End-to-End Testing ✅ COMPLETED

### 4.1 Setup: Reference Local SDK in Server ✅

**File:** `go.mod`

**Implemented:**
- [x] `replace` directive references local sdk-go for development

### 4.2 Functional Tests ✅

**File:** `tests/system_nexus_wait_workflow_test.go`

**Test Suite:** `SystemNexusWaitWorkflowSuite`

**Implemented Tests:**

1. **TestWaitExternalWorkflowCompletion** - Full async flow:
   - [x] Workflow A starts Workflow B via activity
   - [x] Workflow B waits for signal, then completes with result
   - [x] Workflow A spawns coroutine to signal B after 1s delay
   - [x] Workflow A calls `workflow.WaitExternalWorkflow()` on B
   - [x] A receives B's result and returns success
   - [x] Verifies `NexusOperationScheduled` and `NexusOperationCompleted` events in history

2. **TestSimpleWaitExternalWorkflowSchedule** - Operation scheduling:
   - [x] Verifies `NexusOperationScheduled` event is recorded
   - [x] Verifies endpoint=`__temporal_system`, service=`temporal.system.v1`

3. **TestWaitExternalWorkflowAlreadyCompleted** - Sync completion:
   - [x] Workflow B completes before A calls WaitExternalWorkflow
   - [x] Tests synchronous result path
   - [x] Verifies status is COMPLETED

4. **TestWaitExternalWorkflowFailed** - Failed workflow:
   - [x] Workflow B fails intentionally after signal
   - [x] Workflow A receives FAILED status
   - [x] Verifies failure details are accessible via `GetFailure()`

### 4.3 Additional Tests (Future)

**Pending:**
- [ ] Test namespace isolation (cross-namespace should fail)
- [ ] Test timeout behavior
- [ ] Test continue-as-new workflows
- [ ] Test cancellation semantics

---

## Implementation Summary

**Actual implementation order followed:**

1. ✅ Constants and interfaces (Phase 1.1-1.2)
2. ✅ Command handler modifications (Phase 1.3)
3. ✅ Invocation task executor modifications (Phase 1.4)
4. ✅ WaitExternalWorkflow handler with sync/async paths (Phase 2.1-2.4)
5. ✅ Callback completion handler (systemNexusInvocation)
6. ✅ FX wiring (Phase 1.6)
7. ✅ SDK API implementation (Phase 3)
8. ✅ End-to-end functional tests (Phase 4)
9. ⏳ Config and metrics (Phase 1.5) - deferred

---

## Resolved Questions

1. **Callback attachment mechanism**: ✅ RESOLVED
   - Uses `UpdateWorkflowExecutionOptions` API (not `UpdateWorkflowExecution` with `AttachedCompletionCallbacks`)
   - Uses `CompletionCallbacks` field and `AttachRequestId` for idempotency

2. **Continue-as-new handling**: ⏳ Pending testing
   - Callbacks should carry over during continue-as-new via existing infrastructure

3. **Cancellation callback removal**: ⏳ Not yet implemented
   - Currently, canceling WaitExternalWorkflow does not remove the callback from target

4. **Result serialization**: ✅ RESOLVED
   - Uses JSON serialization for input/output
   - `json/plain` encoding metadata on payloads
   - Uses `protojson` for proto message fields within JSON

---

## Files Modified/Created

**Temporal Server (17 files):**

New files:
- `common/nexus/constants.go` - System endpoint constants
- `components/nexusoperations/system/registry.go` - Registry and interfaces
- `components/nexusoperations/system/fx.go` - FX module
- `components/nexusoperations/system/wait_external_workflow.go` - Handler
- `components/callbacks/system_nexus_invocation.go` - Callback completion
- `tests/system_nexus_wait_workflow_test.go` - Functional tests

Modified files:
- `components/nexusoperations/executors.go` - System operation routing
- `components/nexusoperations/workflow/commands.go` - System endpoint handling
- `components/callbacks/executors.go` - Route to systemNexusInvocation
- `service/history/fx.go` - Include system module
- `service/history/statemachine_environment.go` - Provide execution context
- `go.mod` / `go.sum` - SDK dependency

**SDK-Go (8+ files):**
- `workflow/workflow.go` - Public API
- `internal/workflow.go` - Internal implementation
- `internal/wait_external_workflow.go` - Implementation details
- `internal/interceptor.go` - Interceptor interface
- `internal/interceptor_base.go` - Base implementation

---

## Dependencies

- ✅ Existing Nexus infrastructure (state machines, callbacks, executors)
- ✅ HSM framework
- ✅ Callback component (`components/callbacks/`)
- ✅ History service client for cross-shard routing
- ✅ `UpdateWorkflowExecutionOptions` API for callback attachment
