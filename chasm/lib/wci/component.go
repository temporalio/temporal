// Package wci is the CHASM archetype for a Worker Controller Instance.
package wci

import (
	"bytes"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
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
	// ErrMigrationNotPending: Activate/Abort on a target that is no longer pending with that token.
	ErrMigrationNotPending = serviceerror.NewFailedPrecondition("worker controller instance migration is not pending")
	// errPendingInvisible: a pending migration target looks absent, so callers keep using the workflow.
	errPendingInvisible = serviceerror.NewNotFound("worker controller instance not found")

	errConflictTokenMismatch = serviceerror.NewFailedPrecondition("conflict token mismatch")
	// errClosedDuringUpdate: retryable, so the router re-probes and finds the new owner.
	errClosedDuringUpdate = serviceerror.NewUnavailable("worker controller instance closed during the update")
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
	now := ctx.Now(w)
	w.CreateTime = timestamppb.New(now)
	w.mintConflictToken()
	for id := range specs {
		w.ScalingGroups[id] = chasm.NewComponentField(ctx, NewScalingGroup(id))
	}
	// Same first-poll delay as the workflow.
	w.armPollTimer(ctx, now.Add(maxPollInterval))
	w.armValidationTimer(ctx, now.Add(validationInterval))
	wcimetrics.InstanceCreated.With(metricsFor(ctx, w, "")).Record(1)
	return w
}

// NewWorkerControllerInstanceFromMigration restores migrated state as a pending target: no timers,
// no scaling, invisible to clients until TransitionMigrationActivated; it aborts itself at expireTime.
func NewWorkerControllerInstanceFromMigration(
	ctx chasm.MutableContext,
	state *wcipb.WorkerControllerInstanceState,
	groups map[string]*wcipb.ScalingGroupState,
	token string,
	expireTime time.Time,
) *WorkerControllerInstance {
	w := &WorkerControllerInstance{
		WorkerControllerInstanceState: state,
		ScalingGroups:                 make(chasm.Map[string, *ScalingGroup], len(groups)),
		Visibility:                    chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}
	w.Status = wcipb.WCI_STATUS_PENDING_ACTIVATION
	w.MigrationToken = token
	w.PendingExpireTime = timestamppb.New(expireTime)
	for id, group := range groups {
		group.Id = id
		w.ScalingGroups[id] = chasm.NewComponentField(ctx, &ScalingGroup{ScalingGroupState: group})
	}
	ctx.AddTask(w, chasm.TaskAttributes{ScheduledTime: expireTime}, &wcipb.PendingExpiryTask{ExpireTime: timestamppb.New(expireTime)})
	return w
}

// armTimersFromState resumes the poll and validation cadence from state; past-due deadlines fire now.
func (w *WorkerControllerInstance) armTimersFromState(ctx chasm.MutableContext) {
	now := ctx.Now(w)
	notBefore := func(t *timestamppb.Timestamp, fallback time.Time) time.Time {
		if t == nil {
			return fallback
		}
		if at := t.AsTime(); at.After(now) {
			return at
		}
		return now
	}
	w.armPollTimer(ctx, notBefore(w.NextPollTime, now.Add(maxPollInterval)))
	w.armValidationTimer(ctx, notBefore(w.NextValidationTime, now.Add(validationInterval)))
}

func (w *WorkerControllerInstance) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case wcipb.WCI_STATUS_MIGRATED, wcipb.WCI_STATUS_DELETED, wcipb.WCI_STATUS_MIGRATION_ABORTED:
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
	case wcipb.WCI_STATUS_PENDING_ACTIVATION:
		return errPendingInvisible
	default:
		return ErrClosed
	}
}

// mintConflictToken is random so tokens from a deleted run never match its successor; CHASM mutations aren't replayed.
func (w *WorkerControllerInstance) mintConflictToken() {
	w.ConflictToken = []byte(uuid.NewString())
}

// checkConflictToken under the lock: two updates holding the same token can both pass the pre-read check.
func (w *WorkerControllerInstance) checkConflictToken(token []byte) error {
	if len(token) > 0 && !bytes.Equal(token, w.ConflictToken) {
		return errConflictTokenMismatch
	}
	return nil
}
