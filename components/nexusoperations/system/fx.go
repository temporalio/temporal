package system

import (
	"context"

	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.nexusoperations.system",
	fx.Provide(RegistryProvider),
	fx.Provide(SystemOperationExecutorProvider),
)

// RegistryProvider provides a system operation registry with all built-in operations registered.
func RegistryProvider() *Registry {
	registry := NewRegistry()
	// Register all built-in system operations
	RegisterWaitExternalWorkflowCompletion(registry)
	return registry
}

// SystemOperationExecutorProvider provides an adapter that implements nexusoperations.SystemOperationExecutor.
func SystemOperationExecutorProvider(registry *Registry) nexusoperations.SystemOperationExecutor {
	return &systemOperationExecutorAdapter{registry: registry}
}

// systemOperationExecutorAdapter adapts *Registry to implement nexusoperations.SystemOperationExecutor.
type systemOperationExecutorAdapter struct {
	registry *Registry
}

// Execute implements nexusoperations.SystemOperationExecutor.
func (a *systemOperationExecutorAdapter) Execute(
	ctx context.Context,
	execCtx nexusoperations.SystemExecutionContext,
	args nexusoperations.SystemOperationArgs,
) (nexusoperations.SystemOperationResult, error) {
	// Convert nexusoperations.SystemOperationArgs to system.OperationArgs (pure data)
	opArgs := OperationArgs{
		NamespaceID:      args.NamespaceID,
		NamespaceName:    args.NamespaceName,
		CallerWorkflowID: args.CallerWorkflowID,
		CallerRunID:      args.CallerRunID,
		CallerRef:        args.CallerRef,
		Service:          args.Service,
		Operation:        args.Operation,
		Input:            args.Input,
		Header:           args.Header,
		RequestID:        args.RequestID,
	}

	// Convert nexusoperations.SystemExecutionContext to system.ExecutionContext
	opExecCtx := ExecutionContext{
		CallbackTokenGenerator: execCtx.CallbackTokenGenerator,
	}

	// Type-assert the shard context and workflow consistency checker
	if execCtx.ShardContext != nil {
		if shardCtx, ok := execCtx.ShardContext.(historyi.ShardContext); ok {
			opExecCtx.ShardContext = shardCtx
		}
	}

	if execCtx.WorkflowConsistencyChecker != nil {
		if wfChecker, ok := execCtx.WorkflowConsistencyChecker.(api.WorkflowConsistencyChecker); ok {
			opExecCtx.WorkflowConsistencyChecker = wfChecker
		}
	}

	// Execute through the registry
	result, err := a.registry.Execute(ctx, opExecCtx, opArgs)
	if err != nil {
		return nexusoperations.SystemOperationResult{}, err
	}

	// Convert system.OperationResult to nexusoperations.SystemOperationResult
	return nexusoperations.SystemOperationResult{
		Output:         result.Output,
		Failure:        result.Failure,
		OperationToken: result.OperationToken,
	}, nil
}
