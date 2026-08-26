package queues

import (
	"context"
	"errors"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/consts"
)

type (
	activeStandbyExecutor struct {
		currentClusterName string
		registry           namespace.Registry
		activeExecutor     Executor
		standbyExecutor    Executor
		logger             log.Logger
		testHooks          testhooks.TestHooks
	}
)

func NewActiveStandbyExecutor(
	currentClusterName string,
	registry namespace.Registry,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
) Executor {
	return newActiveStandbyExecutor(
		currentClusterName,
		registry,
		activeExecutor,
		standbyExecutor,
		logger,
		testhooks.TestHooks{},
	)
}

// NewActiveStandbyExecutorWithTestHooks creates an active/standby router with
// access to test-only execution-mode overrides.
func NewActiveStandbyExecutorWithTestHooks(
	currentClusterName string,
	registry namespace.Registry,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
	testHooks testhooks.TestHooks,
) Executor {
	return newActiveStandbyExecutor(
		currentClusterName,
		registry,
		activeExecutor,
		standbyExecutor,
		logger,
		testHooks,
	)
}

func newActiveStandbyExecutor(
	currentClusterName string,
	registry namespace.Registry,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
	testHooks testhooks.TestHooks,
) Executor {
	return &activeStandbyExecutor{
		currentClusterName: currentClusterName,
		registry:           registry,
		activeExecutor:     activeExecutor,
		standbyExecutor:    standbyExecutor,
		logger:             logger,
		testHooks:          testHooks,
	}
}

func (e *activeStandbyExecutor) Execute(
	ctx context.Context,
	executable Executable,
) ExecuteResponse {
	if testHook, ok := testhooks.Get(
		e.testHooks,
		testhooks.HistoryPassiveReplicationTest,
		testhooks.GlobalScope,
	); ok && testHook.ShouldExecuteTaskAsPassive(executable.GetTask()) {
		return e.executeForPassiveReplicationTest(ctx, executable)
	}
	return e.executeNormally(ctx, executable)
}

func (e *activeStandbyExecutor) executeNormally(
	ctx context.Context,
	executable Executable,
) ExecuteResponse {
	if e.isActiveTask(executable) {
		return e.activeExecutor.Execute(ctx, executable)
	}
	return e.executeStandby(ctx, executable)
}

func (e *activeStandbyExecutor) executeForPassiveReplicationTest(
	ctx context.Context,
	executable Executable,
) ExecuteResponse {
	response := e.executeStandby(ctx, executable)
	if !errors.Is(response.ExecutionErr, consts.ErrTaskRetry) {
		return response
	}
	return e.activeExecutor.Execute(ctx, executable)
}

func (e *activeStandbyExecutor) executeStandby(
	ctx context.Context,
	executable Executable,
) ExecuteResponse {
	// for standby tasks, use preemptable callerType to avoid impacting active traffic
	return e.standbyExecutor.Execute(
		headers.SetCallerType(ctx, headers.CallerTypePreemptable),
		executable,
	)
}

func (e *activeStandbyExecutor) isActiveTask(
	executable Executable,
) bool {
	// Following is the existing task allocator logic for verifying active task

	namespaceID := executable.GetNamespaceID()
	entry, err := e.registry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		e.logger.Warn("Unable to find namespace, process task as active.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
		return true
	}

	if entry.ActiveClusterName(namespace.RoutingKey{ID: executable.GetWorkflowID()}) != e.currentClusterName {
		e.logger.Debug("Process task as standby.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
		return false
	}

	e.logger.Debug("Process task as active.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
	return true
}
