// Package worker tracks grouped worker heartbeats and lease expiry in CHASM.
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

// newWorkerGroup applies the first heartbeat so successful creation includes
// the initial lease and its expiry task.
func newWorkerGroup(
	ctx chasm.MutableContext,
	groupingKey string,
	heartbeats []*workerpb.WorkerHeartbeat,
	leaseDuration time.Duration,
) (*WorkerGroup, error) {
	if groupingKey == "" {
		return nil, serviceerror.NewInvalidArgument("worker grouping key is required")
	}
	group := &WorkerGroup{
		WorkerGroupState: &workerstatepb.WorkerGroupState{
			WorkerGroupingKey: groupingKey,
			Status:            workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
		},
	}
	if err := group.recordHeartbeat(ctx, heartbeats, leaseDuration); err != nil {
		return nil, err
	}
	return group, nil
}

func (g *WorkerGroup) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	if g.Status == workerstatepb.WORKER_GROUP_STATUS_INACTIVE {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (g *WorkerGroup) ContextMetadata(_ chasm.Context) map[string]string {
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

// recordHeartbeat validates the batch and renews the shared lease.
// Heartbeat metadata stays in the in-memory registry; CHASM stores only the lease.
// TODO: Handle individual worker shutdown when per-worker state is introduced.
// TODO: Add run-scoped heartbeat tokens and retry deduplication with RPC integration,
// before accepting activity binding deltas.
func (g *WorkerGroup) recordHeartbeat(
	ctx chasm.MutableContext,
	heartbeats []*workerpb.WorkerHeartbeat,
	leaseDuration time.Duration,
) error {
	if !transitionActiveHeartbeat.Possible(g) {
		return serviceerror.NewFailedPrecondition("worker group is inactive")
	}
	if leaseDuration <= 0 {
		return serviceerror.NewInvalidArgument("worker lease duration must be positive")
	}
	if len(heartbeats) == 0 {
		return serviceerror.NewInvalidArgument("at least one worker heartbeat is required")
	}
	// Validate the entire batch before renewing the lease.
	for _, heartbeat := range heartbeats {
		key := heartbeat.GetWorkerInstanceKey()
		if key == "" {
			return serviceerror.NewInvalidArgument("worker instance key is required")
		}
		if heartbeat.GetHostInfo().GetWorkerGroupingKey() != g.WorkerGroupingKey {
			return serviceerror.NewInvalidArgument("worker heartbeat grouping key does not match the group")
		}
	}
	return transitionActiveHeartbeat.Apply(g, ctx, eventHeartbeatReceived{
		leaseDuration: leaseDuration,
	})
}
