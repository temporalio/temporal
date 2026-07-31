package api

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

// ErrUseCurrentExecution is a sentinel error to indicate to the caller to
// use the current workflow execution instead of creating a new one
var ErrUseCurrentExecution = errors.New("ErrUseCurrentExecution")

// ResolveDuplicateWorkflowID determines how to resolve a workflow ID duplication upon workflow start according
// to the WorkflowIdReusePolicy (for *completed* workflow) or WorkflowIdConflictPolicy (for *running* workflow).
//
// NOTE: this function assumes the workflow id reuse policy Terminate-if-Running has been migrated
// to the workflow id conflict policy Terminate-Existing before it is invoked.
//
// An action (ie "mitigate and allow"), an error (ie "deny") or neither (ie "allow") is returned.
func ResolveDuplicateWorkflowID(
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	namespaceEntry *namespace.Namespace,
	newRunID string,
	currentState enumsspb.WorkflowExecutionState,
	currentStatus enumspb.WorkflowExecutionStatus,
	currentRequestIDs map[string]*persistencespb.RequestIDInfo,
	currentFirstExecutionRunID string,
	wfIDReusePolicy enumspb.WorkflowIdReusePolicy,
	wfIDConflictPolicy enumspb.WorkflowIdConflictPolicy,
	currentWorkflowStartTime time.Time,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	childWorkflowOnly bool,
) (UpdateWorkflowActionFunc, error) {

	switch currentState {
	// *running* workflow: apply WorkflowIdConflictPolicy
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return ResolveWorkflowIDConflictPolicy(
			shardContext,
			workflowKey,
			namespaceEntry,
			newRunID,
			currentRequestIDs,
			currentFirstExecutionRunID,
			wfIDConflictPolicy,
			currentWorkflowStartTime,
			parentExecutionInfo,
			childWorkflowOnly,
		)

	// *completed* workflow: apply WorkflowIdReusePolicy
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		// no action for the existing workflow
		return nil, ResolveWorkflowIDReusePolicy(
			shardContext,
			workflowKey,
			namespaceEntry,
			currentStatus,
			currentRequestIDs,
			currentFirstExecutionRunID,
			wfIDReusePolicy,
			currentWorkflowStartTime,
		)

	default:
		// persistence.WorkflowStateZombie or unknown type
		return nil, serviceerror.NewInternal(
			fmt.Sprintf("Failed to process workflow, workflow has invalid state: %v.", currentState),
		)
	}
}

func ResolveWorkflowIDConflictPolicy(
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	namespaceEntry *namespace.Namespace,
	newRunID string,
	currentRequestIDs map[string]*persistencespb.RequestIDInfo,
	currentFirstExecutionRunID string,
	wfIDConflictPolicy enumspb.WorkflowIdConflictPolicy,
	currentWorkflowStartTime time.Time,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	childWorkflowOnly bool,
) (UpdateWorkflowActionFunc, error) {
	switch wfIDConflictPolicy { //nolint:exhaustive
	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL:
		msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
		return nil, generateWorkflowAlreadyStartedError(msg, currentRequestIDs, workflowKey, currentFirstExecutionRunID)
	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING:
		return nil, ErrUseCurrentExecution
	case enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING:
		return resolveDuplicateWorkflowStart(shardContext, currentWorkflowStartTime, workflowKey, namespaceEntry, newRunID, parentExecutionInfo, childWorkflowOnly)
	default:
		return nil, serviceerror.NewInternal(
			fmt.Sprintf("Failed to process start workflow id conflict policy: %v.", wfIDConflictPolicy),
		)
	}
}

func ResolveWorkflowIDReusePolicy(
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	namespaceEntry *namespace.Namespace,
	currentStatus enumspb.WorkflowExecutionStatus,
	currentRequestIDs map[string]*persistencespb.RequestIDInfo,
	currentFirstExecutionRunID string,
	wfIDReusePolicy enumspb.WorkflowIdReusePolicy,
	currentWorkflowStartTime time.Time,
) error {
	switch wfIDReusePolicy { //nolint:exhaustive
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE:
		// no error
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		if _, ok := consts.FailedWorkflowStatuses[currentStatus]; !ok {
			msg := "Workflow execution already finished successfully. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: allow duplicate workflow Id if last run failed."
			return generateWorkflowAlreadyStartedError(msg, currentRequestIDs, workflowKey, currentFirstExecutionRunID)
		}
	case enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
		msg := "Workflow execution already finished. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: reject duplicate workflow Id."
		return generateWorkflowAlreadyStartedError(msg, currentRequestIDs, workflowKey, currentFirstExecutionRunID)
	default:
		return serviceerror.NewInternal(
			fmt.Sprintf("Failed to process start workflow id reuse policy: %v.", wfIDReusePolicy),
		)
	}

	nsName := namespaceEntry.Name().String()
	minimalReuseInterval := shardContext.GetConfig().WorkflowIdReuseMinimalInterval(nsName)

	now := shardContext.GetTimeSource().Now()
	timeSinceStart := now.Sub(currentWorkflowStartTime.UTC())

	if minimalReuseInterval == 0 || minimalReuseInterval < timeSinceStart {
		// ie "allow" starting a new workflow
		return nil
	}

	// Since there is a grace period, and the current workflow's start time is within that period,
	// abort the entire request.
	msg := fmt.Sprintf(
		"Too many starts for workflow %s. Time since last start: %d ms",
		workflowKey.WorkflowID,
		timeSinceStart.Milliseconds(),
	)
	return &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: msg,
	}
}

// A minimal interval between workflow starts is used to prevent multiple starts with the same ID too rapidly.
// If the new workflow is started before the interval elapsed, the workflow start is aborted.
func resolveDuplicateWorkflowStart(
	shardContext historyi.ShardContext,
	currentWorkflowStartTime time.Time,
	workflowKey definition.WorkflowKey,
	namespaceEntry *namespace.Namespace,
	newRunID string,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	childWorkflowOnly bool,
) (UpdateWorkflowActionFunc, error) {

	if namespaceEntry == nil {
		return nil, &serviceerror.Internal{
			Message: fmt.Sprintf("Unknown namespace entry for %v.", workflowKey),
		}
	}

	nsName := namespaceEntry.Name().String()
	minimalReuseInterval := shardContext.GetConfig().WorkflowIdReuseMinimalInterval(nsName)

	now := shardContext.GetTimeSource().Now()
	timeSinceStart := now.Sub(currentWorkflowStartTime.UTC())

	if minimalReuseInterval == 0 || minimalReuseInterval < timeSinceStart {
		return terminateWorkflowAction(newRunID, parentExecutionInfo, childWorkflowOnly)
	}

	// Since there is a grace period, and the current workflow's start time is within that period,
	// abort the entire request.
	msg := fmt.Sprintf(
		"Too many restarts for workflow %s. Time since last start: %d ms",
		workflowKey.WorkflowID,
		timeSinceStart.Milliseconds(),
	)
	return nil, &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: msg,
	}
}

func terminateWorkflowAction(
	newRunID string,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	childWorkflowOnly bool,
) (UpdateWorkflowActionFunc, error) {
	return func(workflowLease WorkflowLease) (*UpdateWorkflowAction, error) {
		mutableState := workflowLease.GetMutableState()

		if !mutableState.IsWorkflowExecutionRunning() {
			return nil, consts.ErrWorkflowCompleted
		}

		// if this termination was requested by a parent that was reset, we need to ensure that the current execution is in fact a child of the given parent.
		if parentExecutionInfo != nil && childWorkflowOnly {
			if mutableState.GetExecutionInfo().GetParentWorkflowId() != parentExecutionInfo.Execution.GetWorkflowId() {
				return nil, &serviceerror.Internal{
					Message: fmt.Sprintf("Current workflow %s is not a child of parent %s.", mutableState.GetExecutionInfo().GetWorkflowId(), parentExecutionInfo.Execution.GetWorkflowId()),
				}
			}
		}
		return UpdateWorkflowTerminate, workflow.TerminateWorkflow(
			mutableState,
			"TerminateIfRunning WorkflowIdReusePolicy",
			payloads.EncodeString(fmt.Sprintf("terminated by new runID: %s", newRunID)),
			consts.IdentityHistoryService,
			false,
			nil, // No links necessary.
		)
	}, nil
}

// ZombifyConflictingChildAction validates and marks the conflicting child as a zombie while holding
// its lock. The replacement is then created in the same transaction.
func ZombifyConflictingChildAction(
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
	logger log.Logger,
) UpdateWorkflowActionFunc {
	return func(workflowLease WorkflowLease) (*UpdateWorkflowAction, error) {
		mutableState := workflowLease.GetMutableState()
		if err := validateOrphanedChild(mutableState, parentExecutionInfo); err != nil {
			if !errors.Is(err, consts.ErrWorkflowCompleted) {
				logger.Warn(
					"Unable to zombify conflicting child workflow.",
					tag.WorkflowNamespaceID(mutableState.GetWorkflowKey().NamespaceID),
					tag.WorkflowID(mutableState.GetWorkflowKey().WorkflowID),
					tag.WorkflowRunID(mutableState.GetWorkflowKey().RunID),
					tag.Error(err),
				)
			}
			return nil, err
		}

		if _, err := mutableState.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		); err != nil {
			return nil, err
		}

		logger.Info(
			"Zombified conflicting child workflow.",
			tag.WorkflowNamespaceID(mutableState.GetWorkflowKey().NamespaceID),
			tag.WorkflowID(mutableState.GetWorkflowKey().WorkflowID),
			tag.WorkflowRunID(mutableState.GetWorkflowKey().RunID),
		)
		// Reuse same post-action as a termination: no workflow task, and in-flight Workflow Updates must be
		// aborted because a zombie run will never make progress again.
		return UpdateWorkflowTerminate, nil
	}
}

// validateOrphanedChild allows a conflicting child run to be zombified only when
//  1. the child is still open
//  2. parent execution metadata is present
//  3. the parent namespace ID, workflow ID, and run ID match the requesting parent
//  4. the child's initiated event ID/version tuple differs from the reissued initiation.
func validateOrphanedChild(
	mutableState historyi.MutableState,
	parentExecutionInfo *workflowspb.ParentExecutionInfo,
) error {
	if !mutableState.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowCompleted
	}

	executionState := mutableState.GetExecutionState()
	alreadyStartedErr := func() error {
		return generateWorkflowAlreadyStartedError(
			"Workflow execution is already running. WorkflowId: %v, RunId: %v.",
			executionState.GetRequestIds(),
			mutableState.GetWorkflowKey(),
			executionState.GetFirstExecutionRunId(),
		)
	}
	if parentExecutionInfo == nil {
		return alreadyStartedErr()
	}

	executionInfo := mutableState.GetExecutionInfo()
	if executionInfo.GetParentNamespaceId() != parentExecutionInfo.GetNamespaceId() ||
		executionInfo.GetParentWorkflowId() != parentExecutionInfo.GetExecution().GetWorkflowId() ||
		executionInfo.GetParentRunId() != parentExecutionInfo.GetExecution().GetRunId() {
		return alreadyStartedErr()
	}

	if executionInfo.GetParentInitiatedId() == parentExecutionInfo.GetInitiatedId() &&
		executionInfo.GetParentInitiatedVersion() == parentExecutionInfo.GetInitiatedVersion() {
		return alreadyStartedErr()
	}
	return nil
}

func generateWorkflowAlreadyStartedError(
	errMsg string,
	requestIDs map[string]*persistencespb.RequestIDInfo,
	workflowKey definition.WorkflowKey,
	firstExecutionRunID string,
) error {
	createRequestID := ""
	for requestID, info := range requestIDs {
		if info.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			createRequestID = requestID
		}
	}
	// firstExecutionRunID may be empty for records written before WorkflowExecutionState.first_execution_run_id
	// existed. In that case return empty rather than guessing — callers must not assume RunId.
	return serviceerror.NewWorkflowExecutionAlreadyStartedWithFirstExecutionRunId(
		fmt.Sprintf(errMsg, workflowKey.WorkflowID, workflowKey.RunID),
		createRequestID,
		workflowKey.RunID,
		firstExecutionRunID,
	)
}

func MigrateWorkflowIDReusePolicyForRunningWorkflow(
	wfIDReusePolicy *enumspb.WorkflowIdReusePolicy,
	wfIDConflictPolicy *enumspb.WorkflowIdConflictPolicy,
) {
	// workflow id reuse policy's Terminate-if-Running has been replaced by
	// workflow id conflict policy's Terminate-Existing
	//nolint:staticcheck // SA1019: intentional migration of deprecated policy
	if *wfIDReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
		*wfIDConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING

		// for *closed* workflows, its behavior is defined as ALLOW_DUPLICATE
		*wfIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
}
