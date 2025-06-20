package queues

import (
	"context"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
)

type (
	activeStandbyExecutor struct {
		currentClusterName string
		registry           namespace.Registry
		activeExecutor     Executor
		standbyExecutor    Executor
		logger             log.Logger
	}
)

func NewActiveStandbyExecutor(
	currentClusterName string,
	registry namespace.Registry,
	activeExecutor Executor,
	standbyExecutor Executor,
	logger log.Logger,
) Executor {
	return &activeStandbyExecutor{
		currentClusterName: currentClusterName,
		registry:           registry,
		activeExecutor:     activeExecutor,
		standbyExecutor:    standbyExecutor,
		logger:             logger,
	}
}

func (e *activeStandbyExecutor) Execute(
	ctx context.Context,
	executable Executable,
) ExecuteResponse {
	if e.isActiveTask(executable) {
		return e.activeExecutor.Execute(ctx, executable)
	}

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

	if !entry.ActiveInCluster(e.currentClusterName) {
		e.logger.Debug("Process task as standby.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
		return false
	}

	e.logger.Debug("Process task as active.", tag.WorkflowNamespaceID(namespaceID), tag.Value(executable.GetTask()))
	return true
}
