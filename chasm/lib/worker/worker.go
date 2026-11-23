// Package worker tracks liveness for workers that share a lease.
// Workers with the same worker_grouping_key share a lease under one WorkerGroup.
package worker

import (
	"time"

	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	libraryName        = "worker"
	groupComponentName = "group"
)

var (
	Archetype                     = chasm.FullyQualifiedName(libraryName, groupComponentName)
	_         chasm.RootComponent = (*WorkerGroup)(nil)
)

// WorkerGroup is a CHASM root whose Business ID is the SDK's worker grouping key.
// All workers in the group share one lease.
type WorkerGroup struct {
	chasm.UnimplementedComponent
	*workerstatepb.WorkerGroupState
}

// newWorkerGroup creates an active group without a lease.
func newWorkerGroup() *WorkerGroup {
	return &WorkerGroup{
		WorkerGroupState: &workerstatepb.WorkerGroupState{
			Status: workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
		},
	}
}

func (g *WorkerGroup) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if g.Status == workerstatepb.WORKER_GROUP_STATUS_INACTIVE {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (g *WorkerGroup) ContextMetadata(_ chasm.Context) map[string]string {
	// TODO: Populate request context metadata for the worker group.
	return nil
}

func (g *WorkerGroup) Terminate(
	ctx chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, transitionLeaseExpired.Apply(g, ctx, eventLeaseExpired{})
}

func (g *WorkerGroup) StateMachineState() workerstatepb.WorkerGroupStatus {
	return g.Status
}

func (g *WorkerGroup) SetStateMachineState(status workerstatepb.WorkerGroupStatus) {
	g.Status = status
}

// recordHeartbeat renews the shared lease.
// TODO: When tracking activity bindings, handle ordering so that older heartbeats
// cannot undo newer activity updates and retried heartbeats are not applied twice.
func (g *WorkerGroup) recordHeartbeat(
	ctx chasm.MutableContext,
	heartbeats []*workerpb.WorkerHeartbeat,
	leaseDuration time.Duration,
) error {
	if err := validateHeartbeat(leaseDuration); err != nil {
		return err
	}
	// TODO: Track workers and their activity bindings from these heartbeats.
	return transitionActiveHeartbeat.Apply(g, ctx, eventHeartbeatReceived{
		leaseDuration: leaseDuration,
	})
}

func validateHeartbeat(leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		return serviceerror.NewInvalidArgument("worker lease duration must be positive")
	}
	return nil
}
