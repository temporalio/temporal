// Package wci is the CHASM archetype for a Worker Controller Instance.
package wci

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrClosed: migrated to the workflow or deleted; callers fall back to the workflow backend.
	ErrClosed = serviceerror.NewFailedPrecondition("worker controller instance is closed")
	// ErrMigrationPending: retryable while migrating to the workflow backend.
	ErrMigrationPending = serviceerror.NewUnavailable("worker controller instance is migrating to the workflow backend")
)

var (
	_ chasm.RootComponent                 = (*WorkerControllerInstance)(nil)
	_ chasm.StateMachine[wcipb.WciStatus] = (*WorkerControllerInstance)(nil)
	_ chasm.VisibilityMemoProvider        = (*WorkerControllerInstance)(nil)
)

// WorkerControllerInstance is the root component; the spec lives here, group runtime state on children.
type WorkerControllerInstance struct {
	chasm.UnimplementedComponent

	*wcipb.WorkerControllerInstanceState

	ScalingGroups chasm.Map[string, *ScalingGroup]
	Visibility    chasm.Field[*chasm.Visibility]
}

// NewWorkerControllerInstance creates a running instance with one ScalingGroup per spec entry.
func NewWorkerControllerInstance(
	ctx chasm.MutableContext,
	deploymentName string,
	buildID string,
	specs map[string]*wcipb.ScalingGroupSpec,
) *WorkerControllerInstance {
	w := &WorkerControllerInstance{
		WorkerControllerInstanceState: &wcipb.WorkerControllerInstanceState{
			DeploymentName:    deploymentName,
			BuildId:           buildID,
			Status:            wcipb.WCI_STATUS_RUNNING,
			ScalingGroupSpecs: specs,
		},
		ScalingGroups: make(chasm.Map[string, *ScalingGroup], len(specs)),
		Visibility:    chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}
	w.CreateTime = timestamppb.New(ctx.Now(w))
	for id := range specs {
		w.ScalingGroups[id] = chasm.NewComponentField(ctx, NewScalingGroup())
	}
	return w
}

// NewWorkerControllerInstanceFromMigration restores migrated state as-is, in RUNNING.
func NewWorkerControllerInstanceFromMigration(
	ctx chasm.MutableContext,
	state *wcipb.WorkerControllerInstanceState,
	groups map[string]*wcipb.ScalingGroupState,
) *WorkerControllerInstance {
	w := &WorkerControllerInstance{
		WorkerControllerInstanceState: state,
		ScalingGroups:                 make(chasm.Map[string, *ScalingGroup], len(groups)),
		Visibility:                    chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}
	w.Status = wcipb.WCI_STATUS_RUNNING
	for id, group := range groups {
		w.ScalingGroups[id] = chasm.NewComponentField(ctx, &ScalingGroup{ScalingGroupState: group})
	}
	return w
}

func (w *WorkerControllerInstance) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case wcipb.WCI_STATUS_MIGRATED, wcipb.WCI_STATUS_DELETED:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

func (w *WorkerControllerInstance) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

// Terminate force-closes the instance as deleted.
func (w *WorkerControllerInstance) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	if w.LifecycleState(nil).IsClosed() {
		return chasm.TerminateComponentResponse{}, ErrClosed
	}
	w.Status = wcipb.WCI_STATUS_DELETED
	return chasm.TerminateComponentResponse{}, nil
}

func (w *WorkerControllerInstance) Memo(_ chasm.Context) proto.Message {
	return &wcipb.WorkerControllerInstanceMemo{
		DeploymentName: w.DeploymentName,
		BuildId:        w.BuildId,
		CreateTime:     w.CreateTime,
	}
}

func (w *WorkerControllerInstance) StateMachineState() wcipb.WciStatus {
	return w.Status
}

func (w *WorkerControllerInstance) SetStateMachineState(status wcipb.WciStatus) {
	w.Status = status
}

// CheckMutable maps the root status to the error a mutating request should return.
func (w *WorkerControllerInstance) CheckMutable() error {
	switch w.Status {
	case wcipb.WCI_STATUS_RUNNING:
		return nil
	case wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW:
		return ErrMigrationPending
	default:
		return ErrClosed
	}
}
