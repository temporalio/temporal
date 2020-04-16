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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_mock.go -self_package github.com/uber/cadence/service/history/ndc

package ndc

import (
	"context"
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/execution"
)

const (
	// WorkflowTerminationReason is the reason for terminating workflow due to version conflit
	WorkflowTerminationReason = "Terminate Workflow Due To Version Conflict."
	// WorkflowTerminationIdentity is the component which decides to terminate the workflow
	WorkflowTerminationIdentity = "worker-service"

	identityHistoryService = "history-service"
)

type (
	// Workflow is the interface for NDC workflow
	Workflow interface {
		GetContext() execution.Context
		GetMutableState() execution.MutableState
		GetReleaseFn() execution.ReleaseFunc
		GetVectorClock() (int64, int64, error)
		HappensAfter(that Workflow) (bool, error)
		Revive() error
		SuppressBy(incomingWorkflow Workflow) (execution.TransactionPolicy, error)
		FlushBufferedEvents() error
	}

	workflowImpl struct {
		domainCache     cache.DomainCache
		clusterMetadata cluster.Metadata

		ctx          context.Context
		context      execution.Context
		mutableState execution.MutableState
		releaseFn    execution.ReleaseFunc
	}
)

// NewWorkflow creates a new NDC workflow
func NewWorkflow(
	ctx context.Context,
	domainCache cache.DomainCache,
	clusterMetadata cluster.Metadata,
	context execution.Context,
	mutableState execution.MutableState,
	releaseFn execution.ReleaseFunc,
) Workflow {

	return &workflowImpl{
		ctx:             ctx,
		domainCache:     domainCache,
		clusterMetadata: clusterMetadata,

		context:      context,
		mutableState: mutableState,
		releaseFn:    releaseFn,
	}
}

func (r *workflowImpl) GetContext() execution.Context {
	return r.context
}

func (r *workflowImpl) GetMutableState() execution.MutableState {
	return r.mutableState
}

func (r *workflowImpl) GetReleaseFn() execution.ReleaseFunc {
	return r.releaseFn
}

func (r *workflowImpl) GetVectorClock() (int64, int64, error) {

	lastWriteVersion, err := r.mutableState.GetLastWriteVersion()
	if err != nil {
		return 0, 0, err
	}

	lastEventTaskID := r.mutableState.GetExecutionInfo().LastEventTaskID
	return lastWriteVersion, lastEventTaskID, nil
}

func (r *workflowImpl) HappensAfter(
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

	return workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	), nil
}

func (r *workflowImpl) Revive() error {

	state, _ := r.mutableState.GetWorkflowStateCloseStatus()
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
	return r.mutableState.UpdateWorkflowStateCloseStatus(
		state,
		persistence.WorkflowCloseStatusNone,
	)
}

func (r *workflowImpl) SuppressBy(
	incomingWorkflow Workflow,
) (execution.TransactionPolicy, error) {

	// NOTE: READ BEFORE MODIFICATION
	//
	// if the workflow to be suppressed has last write version being local active
	//  then use active logic to terminate this workflow
	// if the workflow to be suppressed has last write version being remote active
	//  then turn this workflow into a zombie

	lastWriteVersion, lastEventTaskID, err := r.GetVectorClock()
	if err != nil {
		return execution.TransactionPolicyActive, err
	}
	incomingLastWriteVersion, incomingLastEventTaskID, err := incomingWorkflow.GetVectorClock()
	if err != nil {
		return execution.TransactionPolicyActive, err
	}

	if workflowHappensAfter(
		lastWriteVersion,
		lastEventTaskID,
		incomingLastWriteVersion,
		incomingLastEventTaskID) {
		return execution.TransactionPolicyActive, &shared.InternalServiceError{
			Message: "nDCWorkflow cannot suppress workflow by older workflow",
		}
	}

	// if workflow is in zombie or finished state, keep as is
	if !r.mutableState.IsWorkflowExecutionRunning() {
		return execution.TransactionPolicyPassive, nil
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if currentCluster == lastWriteCluster {
		return execution.TransactionPolicyActive, r.terminateWorkflow(lastWriteVersion, incomingLastWriteVersion)
	}
	return execution.TransactionPolicyPassive, r.zombiefyWorkflow()
}

func (r *workflowImpl) FlushBufferedEvents() error {

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

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if lastWriteCluster != currentCluster {
		return &shared.InternalServiceError{
			Message: "nDCWorkflow encounter workflow with buffered events but last write not from current cluster",
		}
	}

	return r.failDecision(lastWriteVersion)
}

func (r *workflowImpl) failDecision(
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
		workflow.DecisionTaskFailedCauseFailoverCloseDecision,
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

func (r *workflowImpl) terminateWorkflow(
	lastWriteVersion int64,
	incomingLastWriteVersion int64,
) error {

	eventBatchFirstEventID := r.GetMutableState().GetNextEventID()
	if err := r.failDecision(lastWriteVersion); err != nil {
		return err
	}

	// do not persist the change right now, NDC requires transaction
	if err := r.mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
		return err
	}

	_, err := r.mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		WorkflowTerminationReason,
		[]byte(fmt.Sprintf("terminated by version: %v", incomingLastWriteVersion)),
		WorkflowTerminationIdentity,
	)

	return err
}

func (r *workflowImpl) zombiefyWorkflow() error {

	return r.mutableState.GetExecutionInfo().UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateZombie,
		persistence.WorkflowCloseStatusNone,
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
