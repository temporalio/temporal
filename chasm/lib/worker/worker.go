// Package worker tracks grouped worker heartbeats and lease expiry in CHASM.
package worker

import (
	"time"

	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common"
)

const (
	libraryName         = "worker"
	groupComponentName  = "group"
	workerComponentName = "worker"
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

	Workers chasm.Map[string, *Worker]
}

// Worker stores one worker's metadata. Its liveness is governed by its group's lease.
type Worker struct {
	chasm.UnimplementedComponent
	*workerstatepb.WorkerState
}

// LifecycleState leaves liveness to the group root. Closing the root prevents
// further progress on its children.
func (w *Worker) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
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
		Workers: make(chasm.Map[string, *Worker]),
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

// recordHeartbeat updates the reported workers and renews their shared lease.
// Workers omitted from the batch remain registered.
// TODO: Add run-scoped heartbeat tokens and retry deduplication before accepting
// activity binding deltas.
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
	// Validate the entire batch before changing membership or renewing the lease.
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
		heartbeats:    heartbeats,
		leaseDuration: leaseDuration,
	})
}

func (g *WorkerGroup) updateWorkers(ctx chasm.MutableContext, heartbeats []*workerpb.WorkerHeartbeat) {
	// TODO: Handle individual worker shutdown while other workers keep the group alive.
	// Tolerate repeated worker keys from the SDK; the last entry in the batch wins.
	for _, heartbeat := range heartbeats {
		key := heartbeat.GetWorkerInstanceKey()
		if field, ok := g.Workers[key]; ok {
			field.Get(ctx).WorkerHeartbeat = common.CloneProto(heartbeat)
		} else {
			g.Workers[key] = chasm.NewComponentField(ctx, &Worker{
				WorkerState: &workerstatepb.WorkerState{
					WorkerHeartbeat: common.CloneProto(heartbeat),
				},
			})
		}
	}
}
