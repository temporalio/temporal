package workflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/proto"
)

func failWorkflowTask(
	mutableState historyi.MutableState,
	workflowTask *historyi.WorkflowTaskInfo,
	workflowTaskFailureCause enumspb.WorkflowTaskFailedCause,
) (*historypb.HistoryEvent, error) {

	// IMPORTANT: wtFailedEvent can be nil under some circumstances. Specifically, if WT is transient.
	wtFailedEvent, err := mutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		workflowTaskFailureCause,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		0,
	)
	if err != nil {
		return nil, err
	}

	mutableState.FlushBufferedEvents()
	return wtFailedEvent, nil
}

func ScheduleWorkflowTask(
	mutableState historyi.MutableState,
) error {

	if mutableState.HasPendingWorkflowTask() {
		return nil
	}

	if mutableState.IsWorkflowExecutionStatusPaused() {
		return nil // workflow is paused, do not schedule a workflow task.
	}

	_, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		return serviceerror.NewInternal("Failed to add workflow task scheduled event.")
	}
	return nil
}

func TimeoutWorkflow(
	mutableState historyi.MutableState,
	retryState enumspb.RetryState,
	continuedRunID string,
) error {

	// Check TerminateWorkflow comment bellow.
	eventBatchFirstEventID := mutableState.GetNextEventID()
	if workflowTask := mutableState.GetStartedWorkflowTask(); workflowTask != nil {
		wtFailedEvent, err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		)
		if err != nil {
			return err
		}
		if wtFailedEvent != nil {
			eventBatchFirstEventID = wtFailedEvent.GetEventId()
		}
	}

	_, err := mutableState.AddTimeoutWorkflowEvent(
		eventBatchFirstEventID,
		retryState,
		continuedRunID,
	)
	return err
}

// TerminateWorkflow will write a WorkflowExecutionTerminated event with a fresh
// batch ID. Do not use for situations where the WorkflowExecutionTerminated
// event must fall within an existing event batch (for example, if you've already
// failed a workflow task via `failWorkflowTask` and have an event batch ID).
func TerminateWorkflow(
	mutableState historyi.MutableState,
	terminateReason string,
	terminateDetails *commonpb.Payloads,
	terminateIdentity string,
	deleteAfterTerminate bool,
	links []*commonpb.Link,
) error {

	// Terminate workflow is written as a separate batch and might result in more than one event
	// if there is started WT which needs to be failed before.
	// Failing speculative WT creates 3 events: WTScheduled, WTStarted, and WTFailed.
	// First 2 goes to separate batch and eventBatchFirstEventID has to point to WTFailed event.
	// Failing transient WT doesn't create any events at all and wtFailedEvent is nil.
	// WTFailed event wasn't created (because there were no WT or WT was transient),
	// then eventBatchFirstEventID points to TerminateWorkflow event (which is next event).
	eventBatchFirstEventID := mutableState.GetNextEventID()

	if workflowTask := mutableState.GetStartedWorkflowTask(); workflowTask != nil {
		wtFailedEvent, err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		)
		if err != nil {
			return err
		}
		if wtFailedEvent != nil {
			eventBatchFirstEventID = wtFailedEvent.GetEventId()
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
		deleteAfterTerminate,
		links,
	)

	return err
}

// FindAutoResetPoint returns the auto reset point
func FindAutoResetPoint(
	timeSource clock.TimeSource,
	verifyChecksum func(string) error,
	autoResetPoints *workflowpb.ResetPoints,
) (string, *workflowpb.ResetPointInfo) {
	if autoResetPoints == nil {
		return "", nil
	}
	now := timeSource.Now()
	for _, p := range autoResetPoints.Points {
		if err := verifyChecksum(p.GetBinaryChecksum()); err != nil && p.GetResettable() {
			expireTime := timestamp.TimeValue(p.GetExpireTime())
			if !expireTime.IsZero() && now.After(expireTime) {
				// reset point has expired and we may already deleted the history
				continue
			}
			return err.Error(), p
		}
	}
	return "", nil
}

func WithEffects(effects effect.Controller, ms historyi.MutableState) MutableStateWithEffects {
	return MutableStateWithEffects{
		MutableState: ms,
		Controller:   effects,
	}
}

type MutableStateWithEffects struct {
	historyi.MutableState
	effect.Controller
}

func (mse MutableStateWithEffects) CanAddEvent() bool {
	// Event can be added to the history if workflow is still running.
	return mse.MutableState.IsWorkflowExecutionRunning()
}

// GetEffectiveDeployment returns the effective deployment in the following order:
//  1. DeploymentVersionTransition.Deployment: this is returned when the wf is transitioning to a
//     new deployment
//  2. VersioningOverride.Deployment: this is returned when user has set a PINNED override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  3. Deployment: this is returned when there is no transition and no override (the most
//     common case). Deployment is set based on the worker-sent deployment in the latest WFT
//     completion. Exception: if Deployment is set but the workflow's effective behavior is
//     UNSPECIFIED, it means the workflow is unversioned, so effective deployment will be nil.
//
// Note: Deployment objects are immutable, never change their fields.
//
//nolint:revive // cognitive complexity to reduce after old code clean up
func GetEffectiveDeployment(versioningInfo *workflowpb.WorkflowExecutionVersioningInfo) *deploymentpb.Deployment {
	if versioningInfo == nil {
		return nil
	} else if transition := versioningInfo.GetVersionTransition(); transition != nil {
		if v := transition.GetDeploymentVersion(); v != nil { // v0.32
			return worker_versioning.DeploymentFromExternalDeploymentVersion(v)
		}
		v, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(transition.GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		return worker_versioning.DeploymentFromDeploymentVersion(v)
	} else if transition := versioningInfo.GetDeploymentTransition(); transition != nil { // //nolint:staticcheck // SA1019: worker versioning v0.30
		return transition.GetDeployment()
	} else if override := versioningInfo.GetVersioningOverride(); override != nil &&
		(override.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED || //nolint:staticcheck // SA1019: worker versioning v0.31 and v0.30
			override.GetPinned() != nil) {
		if pinnedVersion := override.GetPinned().GetVersion(); pinnedVersion != nil {
			return worker_versioning.DeploymentFromExternalDeploymentVersion(pinnedVersion)
		}
		if pinned := override.GetPinnedVersion(); pinned != "" { //nolint:staticcheck // SA1019: worker versioning v0.31
			v, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(pinned)
			return worker_versioning.DeploymentFromDeploymentVersion(v)
		}
		return override.GetDeployment() // //nolint:staticcheck // SA1019: worker versioning v0.30
	} else if GetEffectiveVersioningBehavior(versioningInfo) != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED || // v0.30 and v0.31 auto-upgrade
		versioningInfo.GetVersioningOverride().GetAutoUpgrade() { // v0.32 auto-upgrade
		//nolint:revive // nesting will be reduced after old code clean up
		if v := versioningInfo.GetDeploymentVersion(); v != nil { // v0.32 auto-upgrade
			return worker_versioning.DeploymentFromExternalDeploymentVersion(v)
		}
		if v := versioningInfo.GetVersion(); v != "" { // //nolint:staticcheck // SA1019: worker versioning v0.31
			dv, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(v)
			return worker_versioning.DeploymentFromDeploymentVersion(dv)
		}
		return versioningInfo.GetDeployment() // //nolint:staticcheck // SA1019: worker versioning v0.30
	}
	return nil
}

// GetEffectiveVersioningBehavior returns the effective versioning behavior in the following
// order:
//  1. DeploymentVersionTransition: if there is a transition, then effective behavior is AUTO_UPGRADE.
//  2. VersioningOverride.Behavior: this is returned when user has set a behavior override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  3. Behavior: this is returned when there is no override (most common case). Behavior is
//     set based on the worker-sent deployment in the latest WFT completion.
func GetEffectiveVersioningBehavior(versioningInfo *workflowpb.WorkflowExecutionVersioningInfo) enumspb.VersioningBehavior {
	if versioningInfo == nil {
		return enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
	} else if t := versioningInfo.GetVersionTransition(); t != nil {
		return enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	} else if override := versioningInfo.GetVersioningOverride(); override != nil {
		if override.GetAutoUpgrade() || override.GetPinned() != nil { // v0.32 override behavior
			if override.GetAutoUpgrade() {
				return enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
			}
			return enumspb.VERSIONING_BEHAVIOR_PINNED
		}
		return override.GetBehavior() // //nolint:staticcheck // SA1019: worker versioning v0.31 and v0.30
	}
	return versioningInfo.GetBehavior()
}

// shouldReapplyEvent returns true if the event should be reapplied to the workflow execution.
func shouldReapplyEvent(stateMachineRegistry *hsm.Registry, event *historypb.HistoryEvent) bool {
	switch event.GetEventType() { // nolint:exhaustive
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return true
	}

	// events registered in the hsm framework that are potentially cherry-pickable
	if _, ok := stateMachineRegistry.EventDefinition(event.GetEventType()); ok {
		return true
	}

	return false
}

func getCompletionCallbacksAsProtoSlice(ctx context.Context, ms historyi.MutableState) ([]*commonpb.Callback, error) {
	coll := callbacks.MachineCollection(ms.HSM())
	result := make([]*commonpb.Callback, 0, coll.Size())
	for _, node := range coll.List() {
		cb, err := coll.Data(node.Key.ID)
		if err != nil {
			return nil, err
		}
		if _, ok := cb.Trigger.Variant.(*persistencespb.CallbackInfo_Trigger_WorkflowClosed); !ok {
			continue
		}
		cbSpec, err := PersistenceCallbackToAPICallback(cb.Callback)
		if err != nil {
			return nil, err
		}
		result = append(result, cbSpec)
	}

	// Collect CHASM callbacks
	if ms.ChasmEnabled() {
		wf, ctx, err := ms.ChasmWorkflowComponentReadOnly(ctx)
		if err != nil {
			return nil, err
		}

		for _, field := range wf.Callbacks {
			cb := field.Get(ctx)
			// Only include callbacks in STANDBY state (not already triggered)
			if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
				continue
			}
			// Convert CHASM callback to API callback
			cbSpec, err := cb.ToAPICallback()
			if err != nil {
				return nil, err
			}
			result = append(result, cbSpec)
		}
	}
	// }

	return result, nil
}

func PersistenceCallbackToAPICallback(cb *persistencespb.Callback) (*commonpb.Callback, error) {
	res := &commonpb.Callback{
		Links: cb.GetLinks(),
	}
	switch variant := cb.Variant.(type) {
	case *persistencespb.Callback_Nexus_:
		res.Variant = &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: variant.Nexus.GetHeader(),
			},
		}
	default:
		data, err := proto.Marshal(cb)
		if err != nil {
			return nil, err
		}
		res.Variant = &commonpb.Callback_Internal_{
			Internal: &commonpb.Callback_Internal{
				Data: data,
			},
		}
	}
	return res, nil
}
