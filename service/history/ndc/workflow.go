// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_mock.go

package ndc

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

var (
	WorkflowTerminationReason   = "Terminate Workflow Due To Version Conflict."
	WorkflowTerminationIdentity = "history-service"
)

type (
	Workflow interface {
		GetContext() workflow.Context
		GetMutableState() workflow.MutableState
		GetReleaseFn() wcache.ReleaseCacheFunc
		GetVectorClock() (int64, int64, error)

		HappensAfter(that Workflow) (bool, error)
		Revive() error
		SuppressBy(incomingWorkflow Workflow) (workflow.TransactionPolicy, error)
		FlushBufferedEvents() error
	}

	WorkflowImpl struct {
		clusterMetadata cluster.Metadata

		context      workflow.Context
		mutableState workflow.MutableState
		releaseFn    wcache.ReleaseCacheFunc
	}
)

func NewWorkflow(
	clusterMetadata cluster.Metadata,
	context workflow.Context,
	mutableState workflow.MutableState,
	releaseFn wcache.ReleaseCacheFunc,
) *WorkflowImpl {

	return &WorkflowImpl{
		clusterMetadata: clusterMetadata,

		context:      context,
		mutableState: mutableState,
		releaseFn:    releaseFn,
	}
}

func (r *WorkflowImpl) GetContext() workflow.Context {
	return r.context
}

func (r *WorkflowImpl) GetMutableState() workflow.MutableState {
	return r.mutableState
}

func (r *WorkflowImpl) GetReleaseFn() wcache.ReleaseCacheFunc {
	return r.releaseFn
}

func (r *WorkflowImpl) GetVectorClock() (int64, int64, error) {

	lastWriteVersion, err := r.mutableState.GetLastWriteVersion()
	if err != nil {
		return 0, 0, err
	}

	lastEventTaskID := r.mutableState.GetExecutionInfo().LastEventTaskId
	return lastWriteVersion, lastEventTaskID, nil
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
) (workflow.TransactionPolicy, error) {

	// NOTE: READ BEFORE MODIFICATION
	//
	// if the workflow to be suppressed has last write version being local active
	//  then use active logic to terminate this workflow
	// if the workflow to be suppressed has last write version being remote active
	//  then turn this workflow into a zombie

	lastWriteVersion, lastEventTaskID, err := r.GetVectorClock()
	if err != nil {
		return workflow.TransactionPolicyActive, err
	}
	incomingLastWriteVersion, incomingLastEventTaskID, err := incomingWorkflow.GetVectorClock()
	if err != nil {
		return workflow.TransactionPolicyActive, err
	}

	if WorkflowHappensAfter(
		lastWriteVersion,
		lastEventTaskID,
		incomingLastWriteVersion,
		incomingLastEventTaskID,
	) {
		return workflow.TransactionPolicyActive, serviceerror.NewInternal("Workflow cannot suppress workflow by older workflow")
	}

	// if workflow is in zombie or finished state, keep as is
	if !r.mutableState.IsWorkflowExecutionRunning() {
		return workflow.TransactionPolicyPassive, nil
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(true, lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if currentCluster == lastWriteCluster {
		return workflow.TransactionPolicyActive, r.terminateWorkflow(lastWriteVersion, incomingLastWriteVersion)
	}
	return workflow.TransactionPolicyPassive, r.mutableState.UpdateWorkflowStateStatus(
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

	lastWriteVersion, _, err := r.GetVectorClock()
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
		WorkflowTerminationReason,
		payloads.EncodeString(fmt.Sprintf("terminated by version: %v", incomingLastWriteVersion)),
		WorkflowTerminationIdentity,
		false,
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
