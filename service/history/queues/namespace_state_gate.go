package queues

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
)

type namespaceStateGateScheduler struct {
	Scheduler

	namespaceRegistry namespace.Registry
	isExempt          func(tasks.Task) bool
	metricsHandler    metrics.Handler
}

// NewNamespaceStateGateScheduler wraps a scheduler so tasks of soft-deleted and deleted namespaces
// are acked without running, except those isExempt allows.
func NewNamespaceStateGateScheduler(
	base Scheduler,
	namespaceRegistry namespace.Registry,
	isExempt func(tasks.Task) bool,
	metricsHandler metrics.Handler,
) Scheduler {
	return &namespaceStateGateScheduler{
		Scheduler:         base,
		namespaceRegistry: namespaceRegistry,
		isExempt:          isExempt,
		metricsHandler:    metricsHandler,
	}
}

// IsExemptFromNamespaceStateGate reports whether a task runs even though its namespace is gated:
// retention and deletion keep running, so closed executions still expire and hard delete still works.
func IsExemptFromNamespaceStateGate(task tasks.Task) bool {
	switch t := task.(type) {
	case *tasks.DeleteHistoryEventTask, *tasks.DeleteExecutionTask:
		return true
	case *tasks.CloseExecutionTask:
		return t.DeleteAfterClose
	default:
		return false
	}
}

func (s *namespaceStateGateScheduler) Submit(e Executable) {
	if s.discard(e) {
		return
	}
	s.Scheduler.Submit(e)
}

func (s *namespaceStateGateScheduler) TrySubmit(e Executable) bool {
	if s.discard(e) {
		return true
	}
	return s.Scheduler.TrySubmit(e)
}

// HandleBusyWorkflow passes through to the wrapped scheduler, as rateLimitedSchedulerImpl does.
func (s *namespaceStateGateScheduler) HandleBusyWorkflow(e Executable) bool {
	if handler, ok := s.Scheduler.(BusyWorkflowHandler); ok {
		return handler.HandleBusyWorkflow(e)
	}
	return false
}

func (s *namespaceStateGateScheduler) discard(e Executable) bool {
	ns, err := s.namespaceRegistry.GetNamespaceByID(namespace.ID(e.GetNamespaceID()))
	if err != nil {
		return false
	}
	if state := ns.State(); state != enumspb.NAMESPACE_STATE_SOFT_DELETED && state != enumspb.NAMESPACE_STATE_DELETED {
		return false
	}
	if s.isExempt(e.GetTask()) {
		return false
	}

	metrics.TaskDiscardedByNamespaceState.With(s.metricsHandler).Record(
		1,
		metrics.NamespaceTag(ns.Name().String()),
		metrics.NamespaceStateTag(ns.State().String()),
		metrics.TaskTypeTag(e.GetType().String()),
	)
	e.Ack()
	return true
}
