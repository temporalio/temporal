package system

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// ExecutionContext provides access to history service internals for system operation handlers.
type ExecutionContext struct {
	// ShardContext provides access to shard-level services
	ShardContext historyi.ShardContext
	// WorkflowConsistencyChecker for getting workflow leases
	WorkflowConsistencyChecker api.WorkflowConsistencyChecker
	// CallbackTokenGenerator generates a callback token for async completion.
	// Called lazily only when the operation needs to go async.
	CallbackTokenGenerator func() (string, error)
}

// OperationHandler handles a specific system operation.
type OperationHandler interface {
	// Name returns the name of the operation this handler handles.
	Name() string

	// Execute runs the system operation and returns the result.
	// The handler should return a sync result if the operation can complete immediately,
	// or an async result if it needs to wait for an external event (e.g., workflow completion).
	Execute(ctx context.Context, execCtx ExecutionContext, args OperationArgs) (OperationResult, error)
}

// OperationArgs contains the arguments passed to a system operation handler.
type OperationArgs struct {
	// NamespaceID is the ID of the namespace containing the caller workflow.
	NamespaceID namespace.ID
	// NamespaceName is the name of the namespace containing the caller workflow.
	NamespaceName namespace.Name
	// CallerWorkflowID is the workflow ID of the caller.
	CallerWorkflowID string
	// CallerRunID is the run ID of the caller workflow.
	CallerRunID string
	// CallerRef is the HSM reference to the caller's operation state machine.
	CallerRef hsm.Ref
	// Service is the system service name (e.g., "temporal.system.v1").
	Service string
	// Operation is the operation name (e.g., "WaitExternalWorkflowCompletion").
	Operation string
	// Input is the operation input payload.
	Input *commonpb.Payload
	// Header contains the Nexus headers from the operation request.
	Header map[string]string
	// RequestID is the unique request ID for idempotency.
	RequestID string
}

// OperationResult contains the result of a system operation execution.
// Completion is inferred: if Output or Failure is set, the operation completed synchronously.
// If both are nil, the operation is async and OperationToken should be set.
type OperationResult struct {
	// Output is the operation result payload (for sync success).
	Output *commonpb.Payload

	// Failure is the operation failure (for sync failure).
	Failure *failurepb.Failure

	// OperationToken is an opaque token for tracking async operations.
	// This is returned to the caller and can be used to correlate callbacks.
	OperationToken string
}

// Registry manages registered system operations and dispatches execution requests.
type Registry struct {
	// handlers maps operation names to their handlers
	handlers map[string]OperationHandler
}

// NewRegistry creates a new system operation registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]OperationHandler),
	}
}

// Register adds an operation handler to the registry.
// Panics if a handler with the same operation name is already registered.
func (r *Registry) Register(handler OperationHandler) {
	name := handler.Name()
	if _, exists := r.handlers[name]; exists {
		panic(fmt.Sprintf("system operation handler already registered: %s", name))
	}
	r.handlers[name] = handler
}

// Execute dispatches the operation to the appropriate handler.
// Returns an error if the operation is unknown or if execution fails.
func (r *Registry) Execute(ctx context.Context, execCtx ExecutionContext, args OperationArgs) (OperationResult, error) {
	// Validate service name
	if args.Service != commonnexus.SystemServiceName {
		return OperationResult{}, serviceerror.NewInvalidArgument(
			fmt.Sprintf("unknown system service: %s (expected %s)", args.Service, commonnexus.SystemServiceName),
		)
	}

	handler, ok := r.handlers[args.Operation]
	if !ok {
		return OperationResult{}, serviceerror.NewInvalidArgument(
			fmt.Sprintf("unknown system operation: %s/%s", args.Service, args.Operation),
		)
	}

	return handler.Execute(ctx, execCtx, args)
}

// IsKnownOperation returns true if the operation is registered.
func (r *Registry) IsKnownOperation(service, operation string) bool {
	if service != commonnexus.SystemServiceName {
		return false
	}
	_, ok := r.handlers[operation]
	return ok
}
