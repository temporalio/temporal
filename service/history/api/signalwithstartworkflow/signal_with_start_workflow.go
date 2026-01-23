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

func SignalWithStartWorkflow(
	ctx context.Context,
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	currentWorkflowLease api.WorkflowLease,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (string, bool, error) {
	// workflow is running and restart was not requested
	if currentWorkflowLease != nil &&
		currentWorkflowLease.GetMutableState().IsWorkflowExecutionRunning() &&
		signalWithStartRequest.WorkflowIdConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {

		// current workflow exists & running
		if err := signalWorkflow(
			ctx,
			shard,
			currentWorkflowLease,
			signalWithStartRequest,
		); err != nil {
			return "", false, err
		}
		return currentWorkflowLease.GetContext().GetWorkflowKey().RunID, false, nil
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
) (string, bool, error) {
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
		return "", false, err
	}

	newWorkflowLease, err := api.NewWorkflowLeaseAndContext(nil, shard, newMutableState)
	if err != nil {
		return "", false, err
	}

	if err = api.ValidateSignal(
		ctx,
		shard,
		newMutableState,
		signalWithStartRequest.GetSignalInput().Size(),
		signalWithStartRequest.GetHeader().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		return "", false, err
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
		return "", false, err
	}
	if workflowMutationFn != nil {
		if err = startAndSignalWithCurrentWorkflow(
			ctx,
			shard,
			currentWorkflowLease,
			workflowMutationFn,
			newWorkflowLease,
		); err != nil {
			return "", false, err
		}
		return runID, true, nil
	}
	vrid, err := createVersionedRunID(currentWorkflowLease)
	if err != nil {
		return "", false, err
	}
	return startAndSignalWithoutCurrentWorkflow(
		ctx,
		shard,
		vrid,
		newWorkflowLease,
		signalWithStartRequest.RequestId,
	)
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
	requestID string,
) (string, bool, error) {
	newWorkflow, newWorkflowEventsSeq, err := newWorkflowLease.GetMutableState().CloseTransactionAsSnapshot(
		ctx,
		historyi.TransactionPolicyActive,
	)
	if err != nil {
		return "", false, err
	}
	if len(newWorkflowEventsSeq) != 1 {
		return "", false, serviceerror.NewInternal("unable to create 1st event batch")
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
			return "", false, err
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
	)
	switch failedErr := err.(type) {
	case nil:
		return newWorkflowLease.GetContext().GetWorkflowKey().RunID, true, nil
	case *persistence.CurrentWorkflowConditionFailedError:
		if _, ok := failedErr.RequestIDs[requestID]; ok {
			return failedErr.RunID, false, nil
		}
		return "", false, err
	default:
		return "", false, err
	}
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
