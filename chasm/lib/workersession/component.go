package workersession

import (
	"go.temporal.io/server/chasm"
	workersessionpb "go.temporal.io/server/chasm/lib/workersession/gen/workersessionpb/v1"
)

const (
	Archetype chasm.Archetype = "WorkerSession"
)

// WorkerSession is a Chasm component that tracks worker heartbeats and manages session lifecycle.
type WorkerSession struct {
	chasm.UnimplementedComponent

	// Persisted state for heartbeat tracking.
	*workersessionpb.WorkerSessionState
}

// NewWorkerSession creates a new WorkerSession component.
func NewWorkerSession() *WorkerSession {
	return &WorkerSession{
		WorkerSessionState: &workersessionpb.WorkerSessionState{
			Status: workersessionpb.WORKER_SESSION_STATUS_ACTIVE,
		},
	}
}

// LifecycleState returns the current lifecycle state of the worker session.
func (ws *WorkerSession) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch ws.Status {
	case workersessionpb.WORKER_SESSION_STATUS_ACTIVE:
		return chasm.LifecycleStateRunning
	case workersessionpb.WORKER_SESSION_STATUS_EXPIRED:
		return chasm.LifecycleStateRunning // Still running until cleanup completes.
	case workersessionpb.WORKER_SESSION_STATUS_TERMINATED:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

// State returns the current status (implements StateMachine interface).
func (ws *WorkerSession) State() workersessionpb.WorkerSessionStatus {
	return ws.Status
}

// SetState sets the status (implements StateMachine interface).
func (ws *WorkerSession) SetState(status workersessionpb.WorkerSessionStatus) {
	ws.Status = status
}
