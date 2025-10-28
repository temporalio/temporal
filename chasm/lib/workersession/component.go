package workersession

import (
	"go.temporal.io/server/chasm"
	workersessionpb "go.temporal.io/server/chasm/lib/workersession/gen/workersessionpb/v1"
)

const (
	Archetype chasm.Archetype = "WorkerSession"
)

// WorkerSession is a minimal Chasm component for prototyping
type WorkerSession struct {
	chasm.UnimplementedComponent

	// Minimal persisted state - empty for now
	*workersessionpb.WorkerSessionState
}

// NewWorkerSession creates a new WorkerSession component
func NewWorkerSession() *WorkerSession {
	return &WorkerSession{
		WorkerSessionState: &workersessionpb.WorkerSessionState{
			// Empty for now
		},
	}
}

// LifecycleState returns the current lifecycle state of the worker session
func (ws *WorkerSession) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	// Always running for now - no state to check
	return chasm.LifecycleStateRunning
}
