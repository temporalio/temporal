package signalwithstartworkflow

import (
	"context"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// startOutcome describes the run that received the signal (or was started).
type startOutcome struct {
	// runID is the run that received the signal or was started.
	runID string
	// firstExecutionRunID is the head-of-chain run id. May be empty when reading from a pre-existing
	// record whose ExecutionState has not been backfilled yet.
	firstExecutionRunID string
	// started is true when a new run was created rather than signaling an existing one.
	started bool
	// deduped is true when this invocation only recovered an already-applied result; callers must
	// not repeat start-only side effects.
	deduped bool
}

func SignalWithStartWorkflow(
	ctx context.Context,
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	currentWorkflowLease api.WorkflowLease,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (startOutcome, error) {
	// workflow is running and restart was not requested, and conflict policy is to use existing
	if currentWorkflowLease != nil &&
		currentWorkflowLease.GetMutableState().IsWorkflowExecutionRunning() &&
		signalWithStartRequest.WorkflowIdConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING &&
		signalWithStartRequest.WorkflowIdConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {

		// current workflow exists & running
		if err := signalWorkflow(
			ctx,
			shard,
			currentWorkflowLease,
			signalWithStartRequest,
		); err != nil {
			return startOutcome{}, err
		}

		firstExecutionRunID, err := currentWorkflowLease.GetMutableState().GetFirstRunID(ctx)
		if err != nil {
			return startOutcome{}, err
		}
		return startOutcome{
			runID:               currentWorkflowLease.GetContext().GetWorkflowKey().RunID,
			firstExecutionRunID: firstExecutionRunID,
			started:             false,
		}, nil
	}
	// else, either workflow is not running or restart requested
	return startAndSignalWorkflow(
		ctx,
		shard,
		namespaceEntry,
		currentWorkflowLease,
		startRequest,
		signalWithStartRequest,
	)
}

func startAndSignalWorkflow(
	ctx context.Context,
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	currentWorkflowLease api.WorkflowLease,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (startOutcome, error) {
	if outcome, err := dedupSignalWithStartRequest(ctx, currentWorkflowLease, signalWithStartRequest.GetRequestId()); err != nil {
		return startOutcome{}, err
	} else if outcome != nil {
		return *outcome, nil
	}

	workflowID := signalWithStartRequest.GetWorkflowId()
	runID := uuid.New().String()
	// TODO(bergundy): Support eager workflow task
	newMutableState, err := api.NewWorkflowWithSignal(
		shard,
		namespaceEntry,
		workflowID,
		runID,
		startRequest,
		signalWithStartRequest,
	)
	if err != nil {
		return startOutcome{}, err
	}

	newWorkflowLease, err := api.NewWorkflowLeaseAndContext(nil, shard, newMutableState)
	if err != nil {
		return startOutcome{}, err
	}

	if err = api.ValidateSignal(
		ctx,
		shard,
		newMutableState,
		signalWithStartRequest.GetSignalInput().Size(),
		signalWithStartRequest.GetHeader().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		return startOutcome{}, err
	}

	workflowMutationFn, err := createWorkflowMutationFunction(
		shard,
		currentWorkflowLease,
		namespaceEntry,
		runID,
		signalWithStartRequest.GetWorkflowIdReusePolicy(),
		signalWithStartRequest.GetWorkflowIdConflictPolicy(),
	)
	if err != nil {
		return startOutcome{}, err
	}
	if workflowMutationFn != nil {
		if err = startAndSignalWithCurrentWorkflow(
			ctx,
			shard,
			currentWorkflowLease,
			workflowMutationFn,
			newWorkflowLease,
		); err != nil {
			return startOutcome{}, err
		}
		// Started a fresh run after terminating the existing one: this run is the head of the chain.
		return startOutcome{runID: runID, firstExecutionRunID: runID, started: true}, nil
	}
	vrid, err := createVersionedRunID(currentWorkflowLease)
	if err != nil {
		return startOutcome{}, err
	}
	return startAndSignalWithoutCurrentWorkflow(
		ctx,
		shard,
		vrid,
		newWorkflowLease,
	)
}

// dedupSignalWithStartRequest returns the outcome of a SignalWithStart the current run already
// handled, or nil when the request still has to be processed. It must run before workflow id
// reuse/conflict policy: a retry asks for an already-accepted result, not to reject or replace an
// execution.
//
// Only IsSignalRequested proves the signal was applied. ExecutionState.RequestIds never records
// SIGNALED events, so an id found only there may belong to another API's request and must not
// dedup; it is read solely to recover the Started value of the original response. Dedup holds only
// while the signaled run is still current: signal request ids do not survive continue-as-new.
func dedupSignalWithStartRequest(
	ctx context.Context,
	currentWorkflowLease api.WorkflowLease,
	requestID string,
) (*startOutcome, error) {
	if currentWorkflowLease == nil || requestID == "" {
		return nil, nil
	}

	mutableState := currentWorkflowLease.GetMutableState()
	if !mutableState.IsSignalRequested(requestID) {
		return nil, nil
	}

	firstExecutionRunID, err := mutableState.GetFirstRunID(ctx)
	if err != nil {
		return nil, err
	}
	// The original request started this run exactly when it attached its id to the started event.
	requestIDInfo := mutableState.GetExecutionState().GetRequestIds()[requestID]
	return &startOutcome{
		runID:               currentWorkflowLease.GetContext().GetWorkflowKey().RunID,
		firstExecutionRunID: firstExecutionRunID,
		started:             requestIDInfo.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		deduped:             true,
	}, nil
}

func createWorkflowMutationFunction(
	shardContext historyi.ShardContext,
	currentWorkflowLease api.WorkflowLease,
	namespaceEntry *namespace.Namespace,
	newRunID string,
	workflowIDReusePolicy enumspb.WorkflowIdReusePolicy,
	workflowIDConflictPolicy enumspb.WorkflowIdConflictPolicy,
) (api.UpdateWorkflowActionFunc, error) {
	if currentWorkflowLease == nil {
		return nil, nil
	}
	currentMutableState := currentWorkflowLease.GetMutableState()
	currentExecutionState := currentMutableState.GetExecutionState()
	currentWorkflowStartTime := time.Time{}
	if shardContext.GetConfig().EnableWorkflowIdReuseStartTimeValidation(namespaceEntry.Name().String()) {
		currentWorkflowStartTime = currentExecutionState.StartTime.AsTime()
	}

	// It is unclear if currentExecutionState.RunId is the same as
	// currentWorkflowLease.GetContext().GetWorkflowKey().RunID
	workflowKey := definition.WorkflowKey{
		NamespaceID: currentWorkflowLease.GetContext().GetWorkflowKey().NamespaceID,
		WorkflowID:  currentWorkflowLease.GetContext().GetWorkflowKey().WorkflowID,
		RunID:       currentExecutionState.RunId,
	}

	workflowMutationFunc, err := api.ResolveDuplicateWorkflowID(
		shardContext,
		workflowKey,
		namespaceEntry,
		newRunID,
		currentExecutionState.State,
		currentExecutionState.Status,
		currentExecutionState.RequestIds,
		currentExecutionState.FirstExecutionRunId,
		workflowIDReusePolicy,
		workflowIDConflictPolicy,
		currentWorkflowStartTime,
		nil,
		false,
	)
	return workflowMutationFunc, err
}

func createVersionedRunID(currentWorkflowLease api.WorkflowLease) (*api.VersionedRunID, error) {
	if currentWorkflowLease == nil {
		return nil, nil
	}
	currentExecutionState := currentWorkflowLease.GetMutableState().GetExecutionState()
	currentCloseVersion, err := currentWorkflowLease.GetMutableState().GetCloseVersion()
	if err != nil {
		return nil, err
	}
	id := api.VersionedRunID{
		RunID: currentExecutionState.RunId,
		// we stop updating last write version in the current record after workflow is closed
		// so workflow close version is the last write version for the current record
		LastWriteVersion: currentCloseVersion,
	}
	return &id, nil
}

func startAndSignalWithCurrentWorkflow(
	ctx context.Context,
	shard historyi.ShardContext,
	currentWorkflowLease api.WorkflowLease,
	currentWorkflowUpdateAction api.UpdateWorkflowActionFunc,
	newWorkflowLease api.WorkflowLease,
) error {
	err := api.UpdateWorkflowWithNew(
		shard,
		ctx,
		currentWorkflowLease,
		currentWorkflowUpdateAction,
		func() (historyi.WorkflowContext, historyi.MutableState, error) {
			return newWorkflowLease.GetContext(), newWorkflowLease.GetMutableState(), nil
		},
	)
	if err != nil {
		return err
	}
	return nil

}

func startAndSignalWithoutCurrentWorkflow(
	ctx context.Context,
	shardContext historyi.ShardContext,
	vrid *api.VersionedRunID,
	newWorkflowLease api.WorkflowLease,
) (startOutcome, error) {
	newWorkflow, newWorkflowEventsSeq, err := newWorkflowLease.GetMutableState().CloseTransactionAsSnapshot(
		ctx,
		historyi.TransactionPolicyActive,
	)
	if err != nil {
		return startOutcome{}, err
	}
	if len(newWorkflowEventsSeq) != 1 {
		return startOutcome{}, serviceerror.NewInternal("unable to create 1st event batch")
	}

	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	if vrid != nil {
		createMode = persistence.CreateWorkflowModeUpdateCurrent
		prevRunID = vrid.RunID
		prevLastWriteVersion = vrid.LastWriteVersion
		err = api.NewWorkflowVersionCheck(
			shardContext,
			vrid.LastWriteVersion,
			newWorkflowLease.GetMutableState(),
		)
		if err != nil {
			return startOutcome{}, err
		}
	}
	err = newWorkflowLease.GetContext().CreateWorkflowExecution(
		ctx,
		shardContext,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		newWorkflowLease.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
		historyi.TransactionPolicyActive,
	)
	if err != nil {
		return startOutcome{}, err
	}
	// Brand-new run: head of the chain == this run id.
	runID := newWorkflowLease.GetContext().GetWorkflowKey().RunID
	return startOutcome{runID: runID, firstExecutionRunID: runID, started: true}, nil
}

func signalWorkflow(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowLease api.WorkflowLease,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) error {
	mutableState := workflowLease.GetMutableState()
	if err := api.ValidateSignal(
		ctx,
		shardContext,
		workflowLease.GetMutableState(),
		request.GetSignalInput().Size(),
		request.GetHeader().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		// in-memory mutable state is still clean, release the lock with nil error to prevent
		// clearing and reloading mutable state
		workflowLease.GetReleaseFn()(nil)
		return err
	}

	if request.GetRequestId() != "" && mutableState.IsSignalRequested(request.GetRequestId()) {
		// duplicate signal
		// in-memory mutable state is still clean, release the lock with nil error to prevent
		// clearing and reloading mutable state
		workflowLease.GetReleaseFn()(nil)
		return nil
	}
	if request.GetRequestId() != "" {
		mutableState.AddSignalRequested(request.GetRequestId())
	}
	if _, err := mutableState.AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetRequestId(),
		request.GetLinks(),
	); err != nil {
		return err
	}

	// Create a transfer task to schedule a workflow task
	if !mutableState.HasPendingWorkflowTask() && !mutableState.IsWorkflowExecutionStatusPaused() {

		executionInfo := mutableState.GetExecutionInfo()
		executionState := mutableState.GetExecutionState()
		if !mutableState.HadOrHasWorkflowTask() && !executionInfo.ExecutionTime.AsTime().Equal(executionState.StartTime.AsTime()) {
			metrics.SignalWithStartSkipDelayCounter.With(shardContext.GetMetricsHandler()).Record(1, metrics.NamespaceTag(request.GetNamespace()))

			workflowKey := workflowLease.GetContext().GetWorkflowKey()
			shardContext.GetThrottledLogger().Info(
				"Skipped workflow start delay for signalWithStart request",
				tag.WorkflowNamespace(request.GetNamespace()),
				tag.WorkflowID(workflowKey.WorkflowID),
				tag.WorkflowRunID(workflowKey.RunID),
			)
		}

		_, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
		if err != nil {
			return err
		}
	}

	// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
	// the history and try the operation again.
	return workflowLease.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	)
}
