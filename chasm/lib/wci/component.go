// Package wci is the CHASM archetype for a Worker Controller Instance.
package wci

import (
	"bytes"
	"slices"
	"time"

	"github.com/google/uuid"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/worker_versioning"
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
	_ chasm.RootComponent                      = (*WorkerControllerInstance)(nil)
	_ chasm.StateMachine[wcipb.WciStatus]      = (*WorkerControllerInstance)(nil)
	_ chasm.VisibilityMemoProvider             = (*WorkerControllerInstance)(nil)
	_ chasm.VisibilitySearchAttributesProvider = (*WorkerControllerInstance)(nil)
)

const (
	statusSearchAttributeName = "WorkerControllerInstanceStatus"
	statusRunning             = "Running"
	// RunningListQuery lists only live instances. A migrating one is left out: its pending workflow
	// run is already listed by the workflow backend.
	RunningListQuery = statusSearchAttributeName + " = '" + statusRunning + "'"
)

// Search attributes change only on transitions, validation and spec updates; per-poll state stays
// out, since every change is a visibility write.
var (
	StatusSearchAttribute           = chasm.NewSearchAttributeKeyword(statusSearchAttributeName, chasm.SearchAttributeFieldLowCardinalityKeyword01)
	ValidationStatusSearchAttribute = chasm.NewSearchAttributeKeyword("WCIValidationStatus", chasm.SearchAttributeFieldKeyword01)
	ComputeProvidersSearchAttribute = chasm.NewSearchAttributeKeywordList("WCIComputeProviders", chasm.SearchAttributeFieldKeywordList01)
	// Not the predefined TemporalWorkerDeployment*: CHASM components may not register reserved aliases.
	DeploymentNameSearchAttribute    = chasm.NewSearchAttributeKeyword("WorkerDeploymentName", chasm.SearchAttributeFieldKeyword02)
	DeploymentVersionSearchAttribute = chasm.NewSearchAttributeKeyword("WorkerDeploymentVersion", chasm.SearchAttributeFieldKeyword03)

	statusNames = map[wcipb.WciStatus]string{
		wcipb.WCI_STATUS_RUNNING:               statusRunning,
		wcipb.WCI_STATUS_PENDING_ACTIVATION:    "PendingActivation",
		wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW: "MigratingToWorkflow",
		wcipb.WCI_STATUS_MIGRATED:              "Migrated",
		wcipb.WCI_STATUS_DELETED:               "Deleted",
		wcipb.WCI_STATUS_MIGRATION_ABORTED:     "MigrationAborted",
	}
	validationNames = map[wcipb.ValidationResult]string{
		wcipb.VALIDATION_RESULT_SUCCESS: string(iface.ValidationResultSuccess),
		wcipb.VALIDATION_RESULT_FAILED:  string(iface.ValidationResultFailed),
	}
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
	w.recordSpecChange(ctx)
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
		LastUpdateTime: w.LastUpdateTime,
	}
}

func (w *WorkerControllerInstance) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	version := &deploymentpb.WorkerDeploymentVersion{DeploymentName: w.DeploymentName, BuildId: w.BuildId}
	out := []chasm.SearchAttributeKeyValue{
		StatusSearchAttribute.Value(statusNames[w.Status]),
		DeploymentNameSearchAttribute.Value(w.DeploymentName),
		DeploymentVersionSearchAttribute.Value(worker_versioning.ExternalWorkerDeploymentVersionToString(version)),
	}
	if result, ok := validationNames[w.GetValidationStatus().GetResult()]; ok {
		out = append(out, ValidationStatusSearchAttribute.Value(result))
	}
	var providers []string
	for _, group := range w.ScalingGroupSpecs {
		if p := group.GetCompute().GetProviderType(); p != "" && !slices.Contains(providers, p) {
			providers = append(providers, p)
		}
	}
	if len(providers) > 0 {
		slices.Sort(providers)
		out = append(out, ComputeProvidersSearchAttribute.Value(providers))
	}
	return out
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

// recordSpecChange mints a random token so tokens from a deleted run never match its successor; CHASM mutations aren't replayed.
func (w *WorkerControllerInstance) recordSpecChange(ctx chasm.Context) {
	w.ConflictToken = []byte(uuid.NewString())
	w.LastUpdateTime = timestamppb.New(ctx.Now(w))
}

// checkConflictToken under the lock: two updates holding the same token can both pass the pre-read check.
func (w *WorkerControllerInstance) checkConflictToken(token []byte) error {
	if len(token) > 0 && !bytes.Equal(token, w.ConflictToken) {
		return errConflictTokenMismatch
	}
	return nil
}
