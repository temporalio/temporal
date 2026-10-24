package queues

import (
	"context"
	"errors"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
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
		metricsHandler     metrics.Handler
		testHooks          testhooks.TestHooks
	}
)

func NewActiveStandbyExecutor(
	currentClusterName string,
	registry namespace.Registry,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
	metricsHandler metrics.Handler,
	testHooks testhooks.TestHooks,
) Executor {
	return &activeStandbyExecutor{
		currentClusterName: currentClusterName,
		registry:           registry,
		activeExecutor:     activeExecutor,
		standbyExecutor:    standbyExecutor,
		logger:             logger,
		metricsHandler:     metricsHandler,
		testHooks:          testHooks,
	}
}

func (e *activeStandbyExecutor) Execute(
	ctx context.Context,
	executable Executable,
) ExecuteResponse {
	namespaceID := namespace.ID(executable.GetNamespaceID())
	if testHook, ok := testhooks.Get(
		e.testHooks,
		testhooks.HistoryPassiveReplicationTest,
		namespaceID,
	); ok && testHook.ShouldExecuteTaskAsPassive(executable.GetTask()) {
		metrics.HistoryPassiveReplicationTestHookCounter.With(e.metricsHandler).Record(
			1,
			metrics.OperationTag("ActiveStandbyExecutor"),
		)
		return e.executeForPassiveReplicationTest(ctx, executable)
	}
	return e.executeNormally(ctx, executable, namespaceID)
}

func (e *activeStandbyExecutor) executeNormally(
	ctx context.Context,
	executable Executable,
	namespaceID namespace.ID,
) ExecuteResponse {
	if e.isActiveTask(executable, namespaceID) {
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
	response = e.activeExecutor.Execute(ctx, executable)
	if response.ExecutionErr != nil {
		return response
	}
	return e.executeStandby(ctx, executable)
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
	namespaceID namespace.ID,
) bool {
	// Following is the existing task allocator logic for verifying active task

	entry, err := e.registry.GetNamespaceByID(namespaceID)
	if err != nil {
		e.logger.Warn("Unable to find namespace, process task as active.", tag.WorkflowNamespaceID(namespaceID.String()), tag.Value(executable.GetTask()))
		return true
	}

	if entry.ActiveClusterName(namespace.RoutingKey{ID: executable.GetWorkflowID()}) != e.currentClusterName {
		e.logger.Debug("Process task as standby.", tag.WorkflowNamespaceID(namespaceID.String()), tag.Value(executable.GetTask()))
		return false
	}

	e.logger.Debug("Process task as active.", tag.WorkflowNamespaceID(namespaceID.String()), tag.Value(executable.GetTask()))
	return true
}
