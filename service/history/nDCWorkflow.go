//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCWorkflow_mock.go

package history

import (
	"context"
	"fmt"

	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCWorkflow interface {
		getContext() workflowExecutionContext
		getMutableState() mutableState
		getReleaseFn() releaseWorkflowExecutionFunc
		getVectorClock() (int64, int64, error)
		happensAfter(that nDCWorkflow) (bool, error)
		revive() error
		suppressBy(incomingWorkflow nDCWorkflow) (transactionPolicy, error)
		flushBufferedEvents() error
	}

	nDCWorkflowImpl struct {
		namespaceCache  cache.NamespaceCache
		clusterMetadata cluster.Metadata

		ctx          context.Context
		context      workflowExecutionContext
		mutableState mutableState
		releaseFn    releaseWorkflowExecutionFunc
	}
)

func newNDCWorkflow(
	ctx context.Context,
	namespaceCache cache.NamespaceCache,
	clusterMetadata cluster.Metadata,
	context workflowExecutionContext,
	mutableState mutableState,
	releaseFn releaseWorkflowExecutionFunc,
) *nDCWorkflowImpl {

	return &nDCWorkflowImpl{
		ctx:             ctx,
		namespaceCache:  namespaceCache,
		clusterMetadata: clusterMetadata,

		context:      context,
		mutableState: mutableState,
		releaseFn:    releaseFn,
	}
}

func (r *nDCWorkflowImpl) getContext() workflowExecutionContext {
	return r.context
}

func (r *nDCWorkflowImpl) getMutableState() mutableState {
	return r.mutableState
}

func (r *nDCWorkflowImpl) getReleaseFn() releaseWorkflowExecutionFunc {
	return r.releaseFn
}

func (r *nDCWorkflowImpl) getVectorClock() (int64, int64, error) {

	lastWriteVersion, err := r.mutableState.GetLastWriteVersion()
	if err != nil {
		return 0, 0, err
	}

	lastEventTaskID := r.mutableState.GetExecutionInfo().LastEventTaskID
	return lastWriteVersion, lastEventTaskID, nil
}

func (r *nDCWorkflowImpl) happensAfter(
	that nDCWorkflow,
) (bool, error) {

	thisLastWriteVersion, thisLastEventTaskID, err := r.getVectorClock()
	if err != nil {
		return false, err
	}
	thatLastWriteVersion, thatLastEventTaskID, err := that.getVectorClock()
	if err != nil {
		return false, err
	}

	return workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	), nil
}

func (r *nDCWorkflowImpl) revive() error {

	state, _ := r.mutableState.GetWorkflowStateStatus()
	if state != persistence.WorkflowStateZombie {
		return nil
	} else if state == persistence.WorkflowStateCompleted {
		// workflow already finished
		return nil
	}

	// workflow is in zombie state, need to set the state correctly accordingly
	state = persistence.WorkflowStateCreated
	if r.mutableState.HasProcessedOrPendingDecision() {
		state = persistence.WorkflowStateRunning
	}
	return r.mutableState.UpdateWorkflowStateStatus(
		state,
		executionpb.WorkflowExecutionStatus_Running,
	)
}

func (r *nDCWorkflowImpl) suppressBy(
	incomingWorkflow nDCWorkflow,
) (transactionPolicy, error) {

	// NOTE: READ BEFORE MODIFICATION
	//
	// if the workflow to be suppressed has last write version being local active
	//  then use active logic to terminate this workflow
	// if the workflow to be suppressed has last write version being remote active
	//  then turn this workflow into a zombie

	lastWriteVersion, lastEventTaskID, err := r.getVectorClock()
	if err != nil {
		return transactionPolicyActive, err
	}
	incomingLastWriteVersion, incomingLastEventTaskID, err := incomingWorkflow.getVectorClock()
	if err != nil {
		return transactionPolicyActive, err
	}

	if workflowHappensAfter(
		lastWriteVersion,
		lastEventTaskID,
		incomingLastWriteVersion,
		incomingLastEventTaskID) {
		return transactionPolicyActive, serviceerror.NewInternal("nDCWorkflow cannot suppress workflow by older workflow")
	}

	// if workflow is in zombie or finished state, keep as is
	if !r.mutableState.IsWorkflowExecutionRunning() {
		return transactionPolicyPassive, nil
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if currentCluster == lastWriteCluster {
		return transactionPolicyActive, r.terminateWorkflow(lastWriteVersion, incomingLastWriteVersion)
	}
	return transactionPolicyPassive, r.zombiefyWorkflow()
}

func (r *nDCWorkflowImpl) flushBufferedEvents() error {

	if !r.mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	if !r.mutableState.HasBufferedEvents() {
		return nil
	}

	lastWriteVersion, _, err := r.getVectorClock()
	if err != nil {
		return err
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if lastWriteCluster != currentCluster {
		return serviceerror.NewInternal("nDCWorkflow encounter workflow with buffered events but last write not from current cluster")
	}

	return r.failDecision(lastWriteVersion)
}

func (r *nDCWorkflowImpl) failDecision(
	lastWriteVersion int64,
) error {

	// do not persist the change right now, NDC requires transaction
	if err := r.mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
		return err
	}

	decision, ok := r.mutableState.GetInFlightDecision()
	if !ok {
		return nil
	}

	if _, err := r.mutableState.AddDecisionTaskFailedEvent(
		decision.ScheduleID,
		decision.StartedID,
		eventpb.DecisionTaskFailedCause_FailoverCloseDecision,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}

	return r.mutableState.FlushBufferedEvents()
}

func (r *nDCWorkflowImpl) terminateWorkflow(
	lastWriteVersion int64,
	incomingLastWriteVersion int64,
) error {

	eventBatchFirstEventID := r.getMutableState().GetNextEventID()
	if err := r.failDecision(lastWriteVersion); err != nil {
		return err
	}

	// do not persist the change right now, NDC requires transaction
	if err := r.mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
		return err
	}

	_, err := r.mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		workflowTerminationReason,
		[]byte(fmt.Sprintf("terminated by version: %v", incomingLastWriteVersion)),
		workflowTerminationIdentity,
	)

	return err
}

func (r *nDCWorkflowImpl) zombiefyWorkflow() error {

	return r.mutableState.GetExecutionInfo().UpdateWorkflowStateStatus(
		persistence.WorkflowStateZombie,
		executionpb.WorkflowExecutionStatus_Running,
	)
}

func workflowHappensAfter(
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
