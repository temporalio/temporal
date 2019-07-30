// Copyright (c) 2019 Uber Technologies, Inc.
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

package history

import (
	ctx "context"
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCWorkflow interface {
		getContext() workflowExecutionContext
		getMutableState() mutableState
		getReleaseFn() releaseWorkflowExecutionFunc
		getVectorClock() (int64, int64, error)
		happensAfter(that nDCWorkflow) (bool, error)
		suppressWorkflowBy(incomingWorkflow nDCWorkflow) error
	}

	nDCWorkflowImpl struct {
		shard           ShardContext
		domainCache     cache.DomainCache
		clusterMetadata cluster.Metadata

		ctx          ctx.Context
		context      workflowExecutionContext
		mutableState mutableState
		releaseFn    releaseWorkflowExecutionFunc
	}
)

func newNDCWorkflow(
	ctx ctx.Context,
	domainCache cache.DomainCache,
	clusterMetadata cluster.Metadata,

	context workflowExecutionContext,
	mutableState mutableState,
	releaseFn releaseWorkflowExecutionFunc,
) *nDCWorkflowImpl {

	return &nDCWorkflowImpl{
		ctx:             ctx,
		domainCache:     domainCache,
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

	currentVersionHistory, err := r.mutableState.GetVersionHistories().GetCurrentVersionHistory()
	if err != nil {
		return 0, 0, err
	}

	lastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return 0, 0, err
	}

	lastEventTaskID := r.mutableState.GetExecutionInfo().LastEventTaskID

	return lastItem.GetVersion(), lastEventTaskID, nil
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

func (r *nDCWorkflowImpl) suppressWorkflowBy(
	incomingWorkflow nDCWorkflow,
) error {

	// NOTE: READ BEFORE MODIFICATION
	//
	// if the workflow to be suppressed has last write version being local active
	//  then use active logic to terminate this workflow
	// if the workflow to be suppressed has last write version being remote active
	//  then turn this workflow into a zombie

	lastWriteVersion, lastEventTaskID, err := r.getVectorClock()
	if err != nil {
		return err
	}
	incomingLastWriteVersion, incomingLastEventTaskID, err := incomingWorkflow.getVectorClock()
	if err != nil {
		return err
	}

	if workflowHappensAfter(
		lastWriteVersion,
		lastEventTaskID,
		incomingLastWriteVersion,
		incomingLastEventTaskID) {
		return &shared.InternalServiceError{
			Message: "nDCWorkflow cannot suppress workflow by older workflow",
		}
	}

	// if workflow is in zombie or finished state, keep as is
	if !r.mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	lastWriteCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()

	if currentCluster == lastWriteCluster {
		return r.terminateWorkflow(lastWriteVersion, incomingLastWriteVersion)
	}
	return r.zombiefyWorkflow()
}

func (r *nDCWorkflowImpl) terminateWorkflow(
	lastWriteVersion int64,
	incomingLastWriteVersion int64,
) error {

	// do not persist the change right now, NDC requires transaction
	r.mutableState.UpdateCurrentVersion(lastWriteVersion, true)
	_, err := r.mutableState.AddWorkflowExecutionTerminatedEvent(
		workflowTerminationReason,
		[]byte(fmt.Sprintf("terminated by version: %v", incomingLastWriteVersion)),
		workflowTerminationIdentity,
	)
	if err != nil {
		return err
	}

	// add close workflow tasks
	executionInfo := r.mutableState.GetExecutionInfo()
	transferTask, timerTask, err := getWorkflowCleanupTasks(
		r.domainCache,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		newTimerBuilder(clock.NewRealTimeSource()), // terminate workflow as active
	)
	if err != nil {
		return err
	}
	r.mutableState.AddTransferTasks(transferTask)
	r.mutableState.AddTimerTasks(timerTask)
	return err
}

func (r *nDCWorkflowImpl) zombiefyWorkflow() error {

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
