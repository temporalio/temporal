//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_mock.go

package ndc

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type (
	Workflow interface {
		GetContext() historyi.WorkflowContext
		GetMutableState() historyi.MutableState
		GetReleaseFn() historyi.ReleaseWorkflowContextFunc
		GetVectorClock() (int64, int64, error)

		HappensAfter(that Workflow) (bool, error)
		Revive() error
		SuppressBy(incomingWorkflow Workflow) (historyi.TransactionPolicy, error)
		FlushBufferedEvents() error
	}

	WorkflowImpl struct {
		clusterMetadata cluster.Metadata

		context      historyi.WorkflowContext
		mutableState historyi.MutableState
		releaseFn    historyi.ReleaseWorkflowContextFunc
	}
)

func NewWorkflow(
	clusterMetadata cluster.Metadata,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	releaseFn historyi.ReleaseWorkflowContextFunc,
) *WorkflowImpl {

	return &WorkflowImpl{
		clusterMetadata: clusterMetadata,

		context:      wfContext,
		mutableState: mutableState,
		releaseFn:    releaseFn,
	}
}

func (r *WorkflowImpl) GetContext() historyi.WorkflowContext {
	return r.context
}

func (r *WorkflowImpl) GetMutableState() historyi.MutableState {
	return r.mutableState
}

func (r *WorkflowImpl) GetReleaseFn() historyi.ReleaseWorkflowContextFunc {
	return r.releaseFn
}

func (r *WorkflowImpl) GetVectorClock() (int64, int64, error) {

	var version int64
	var err error
	if r.mutableState.IsWorkflowExecutionRunning() {
		version, err = r.mutableState.GetLastWriteVersion()
		if err != nil {
			return 0, 0, err
		}
	} else {
		version, err = r.mutableState.GetCloseVersion()
		if err != nil {
			return 0, 0, err
		}
	}

	lastEventTaskID := r.mutableState.GetExecutionInfo().LastEventTaskId
	return version, lastEventTaskID, nil
}

func (r *WorkflowImpl) HappensAfter(
	that Workflow,
) (bool, error) {

	thisLastWriteVersion, thisLastEventTaskID, err := r.GetVectorClock()
	if err != nil {
		return false, err
	}
	thatLastWriteVersion, thatLastEventTaskID, err := that.GetVectorClock()
	if err != nil {
		return false, err
	}

	return WorkflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	), nil
}

func (r *WorkflowImpl) Revive() error {

	state, _ := r.mutableState.GetWorkflowStateStatus()
	if state != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return nil
	} else if state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// workflow already finished
		return nil
	}

	// workflow is in zombie state, need to set the state correctly accordingly
	state = enumsspb.WORKFLOW_EXECUTION_STATE_CREATED
	if r.mutableState.HadOrHasWorkflowTask() {
		state = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
	}
	return r.mutableState.UpdateWorkflowStateStatus(
		state,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)
}

func (r *WorkflowImpl) SuppressBy(
	incomingWorkflow Workflow,
) (historyi.TransactionPolicy, error) {

	// NOTE: READ BEFORE MODIFICATION
	//
	// if the workflow to be suppressed has last write version being local active
	//  then use active logic to terminate this workflow
	// if the workflow to be suppressed has last write version being remote active
	//  then turn this workflow into a zombie

	lastWriteVersion, lastEventTaskID, err := r.GetVectorClock()
	if err != nil {
		return historyi.TransactionPolicyActive, err
	}
	incomingLastWriteVersion, incomingLastEventTaskID, err := incomingWorkflow.GetVectorClock()
	if err != nil {
		return historyi.TransactionPolicyActive, err
	}

	if WorkflowHappensAfter(
		lastWriteVersion,
		lastEventTaskID,
		incomingLastWriteVersion,
		incomingLastEventTaskID,
	) {
		return historyi.TransactionPolicyActive, serviceerror.NewInternal("Workflow cannot suppress workflow by older workflow")
	}

	// if workflow is in zombie or finished state, keep as is
	if !r.mutableState.IsWorkflowExecutionRunning() {
		return historyi.TransactionPolicyPassive, nil
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(true, lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if currentCluster == lastWriteCluster {
		return historyi.TransactionPolicyActive, r.terminateWorkflow(lastWriteVersion, incomingLastWriteVersion)
	}
	return historyi.TransactionPolicyPassive, r.mutableState.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)
}

func (r *WorkflowImpl) FlushBufferedEvents() error {

	if !r.mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	if !r.mutableState.HasBufferedEvents() {
		return nil
	}

	// TODO: Same as the reasoning in mutableState.startTransactionHandleWorkflowTaskFailover()
	// LastWriteVersion is only correct when replication task processing logic flush buffered
	// events for state only changes as well.
	// Transition history is not enabled today so LastWriteVersion == LastEventVersion
	lastWriteVersion, err := r.mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(true, lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if lastWriteCluster != currentCluster {
		return serviceerror.NewInternal("Workflow encountered workflow with buffered events but last write not from current cluster")
	}

	if _, err = r.failWorkflowTask(lastWriteVersion); err != nil {
		return err
	}
	if _, err := r.mutableState.AddWorkflowTaskScheduledEvent(
		false,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	); err != nil {
		return err
	}
	return nil
}

func (r *WorkflowImpl) failWorkflowTask(
	lastWriteVersion int64,
) (*historypb.HistoryEvent, error) {

	// do not persist the change right now, Workflow requires transaction
	if err := r.mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
		return nil, err
	}

	workflowTask := r.mutableState.GetStartedWorkflowTask()
	if workflowTask == nil {
		return nil, nil
	}

	wtFailedEvent, err := r.mutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
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

	r.mutableState.FlushBufferedEvents()
	return wtFailedEvent, nil
}

func (r *WorkflowImpl) terminateWorkflow(
	lastWriteVersion int64,
	incomingLastWriteVersion int64,
) error {

	eventBatchFirstEventID := r.GetMutableState().GetNextEventID()
	wtFailedEvent, err := r.failWorkflowTask(lastWriteVersion)
	if err != nil {
		return err
	}

	if wtFailedEvent != nil {
		eventBatchFirstEventID = wtFailedEvent.GetEventId()
	}

	// do not persist the change right now, Workflow requires transaction
	if err = r.mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
		return err
	}

	_, err = r.mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		common.FailureReasonWorkflowTerminationDueToVersionConflict,
		payloads.EncodeString(fmt.Sprintf("terminated by version: %v", incomingLastWriteVersion)),
		consts.IdentityHistoryService,
		false,
		nil, // No links necessary.
	)

	// Don't abort updates here for a few reasons:
	//   1. There probably no update waiters for Wf which is about to be terminated,
	//   2. MS is not persisted yet, and updates should be aborted after MS is persisted, which is not trivial in this case,
	//   3. New replication version will force update registry reload and waiters will get errors.
	// r.GetContext().UpdateRegistry(context.Background(), nil).Abort(update.AbortReasonWorkflowTerminated)

	return err
}

func WorkflowHappensAfter(
	thisLastWriteVersion int64,
	thisLastEventTaskID int64,
	thatLastWriteVersion int64,
	thatLastEventTaskID int64,
) bool {

	if thisLastWriteVersion != thatLastWriteVersion {
		return thisLastWriteVersion > thatLastWriteVersion
	}

	// thisLastWriteVersion == thatLastWriteVersion
	return thisLastEventTaskID > thatLastEventTaskID
}
