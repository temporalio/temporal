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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCActivityReplicator_mock.go

package history

import (
	"context"
	"time"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

const (
	resendMissingEventMessage  = "Resend missed sync activity events"
	resendHigherVersionMessage = "Resend sync activity events due to a higher version received"
)

type (
	nDCActivityReplicator interface {
		SyncActivity(
			ctx context.Context,
			request *historyservice.SyncActivityRequest,
		) error
	}

	nDCActivityReplicatorImpl struct {
		historyCache    workflow.Cache
		clusterMetadata cluster.Metadata
		logger          log.Logger
	}
)

func newNDCActivityReplicator(
	shard shard.Context,
	historyCache workflow.Cache,
	logger log.Logger,
) *nDCActivityReplicatorImpl {

	return &nDCActivityReplicatorImpl{
		historyCache:    historyCache,
		clusterMetadata: shard.GetClusterMetadata(),
		logger:          log.With(logger, tag.ComponentHistoryReplicator),
	}
}

func (r *nDCActivityReplicatorImpl) SyncActivity(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
) (retError error) {

	// sync activity info will only be sent from active side, when
	// 1. activity retry
	// 2. activity start
	// 3. activity heart beat
	// no sync activity task will be sent when active side fail / timeout activity,
	namespaceID := namespace.ID(request.GetNamespaceId())
	execution := commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}

	executionContext, release, err := r.historyCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeAPI,
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := executionContext.LoadWorkflowExecution(ctx)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// this can happen if the workflow start event and this sync activity task are out of order
			// or the target workflow is long gone
			// the safe solution to this is to throw away the sync activity task
			// or otherwise, worker attempt will exceed limit and put this message to DLQ
			return nil
		}
		return err
	}

	scheduledEventID := request.GetScheduledEventId()
	shouldApply, err := r.testVersionHistory(
		namespaceID,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		scheduledEventID,
		mutableState,
		request.GetVersionHistory(),
	)
	if err != nil || !shouldApply {
		return err
	}

	activityInfo, ok := mutableState.GetActivityInfo(scheduledEventID)
	if !ok {
		// this should not retry, can be caused by out of order delivery
		// since the activity is already finished
		return nil
	}
	if shouldApply := r.testActivity(
		request.GetVersion(),
		request.GetAttempt(),
		timestamp.TimeValue(request.GetLastHeartbeatTime()),
		activityInfo,
	); !shouldApply {
		return nil
	}

	// sync activity with empty started ID means activity retry
	eventTime := timestamp.TimeValue(request.GetScheduledTime())
	if request.StartedEventId == common.EmptyEventID && request.Attempt > activityInfo.GetAttempt() {
		mutableState.AddTasks(&tasks.ActivityRetryTimerTask{
			WorkflowKey: definition.WorkflowKey{
				NamespaceID: request.GetNamespaceId(),
				WorkflowID:  request.GetWorkflowId(),
				RunID:       request.GetRunId(),
			},
			VisibilityTimestamp: eventTime,
			EventID:             request.GetScheduledEventId(),
			Version:             request.GetVersion(),
			Attempt:             request.GetAttempt(),
		})
	}

	refreshTask := r.testRefreshActivityTimerTaskMask(
		request.GetVersion(),
		request.GetAttempt(),
		activityInfo,
	)
	err = mutableState.ReplicateActivityInfo(request, refreshTask)
	if err != nil {
		return err
	}

	// see whether we need to refresh the activity timer
	startedTime := timestamp.TimeValue(request.GetStartedTime())
	lastHeartbeatTime := timestamp.TimeValue(request.GetLastHeartbeatTime())
	if eventTime.Before(startedTime) {
		eventTime = startedTime
	}
	if eventTime.Before(lastHeartbeatTime) {
		eventTime = lastHeartbeatTime
	}

	// passive logic need to explicitly call create timer
	now := eventTime
	if _, err := workflow.NewTimerSequence(
		mutableState,
	).CreateNextActivityTimer(); err != nil {
		return err
	}

	updateMode := persistence.UpdateWorkflowModeUpdateCurrent
	if state, _ := mutableState.GetWorkflowStateStatus(); state == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		updateMode = persistence.UpdateWorkflowModeBypassCurrent
	}

	return executionContext.UpdateWorkflowExecutionWithNew(
		ctx,
		now,
		updateMode,
		nil, // no new workflow
		nil, // no new workflow
		workflow.TransactionPolicyPassive,
		nil,
	)
}

func (r *nDCActivityReplicatorImpl) testRefreshActivityTimerTaskMask(
	version int64,
	attempt int32,
	activityInfo *persistencespb.ActivityInfo,
) bool {

	// calculate whether to reset the activity timer task status bits
	// reset timer task status bits if
	// 1. same source cluster & attempt changes
	// 2. different source cluster
	if !r.clusterMetadata.IsVersionFromSameCluster(version, activityInfo.Version) {
		return true
	} else if activityInfo.Attempt != attempt {
		return true
	}
	return false
}

func (r *nDCActivityReplicatorImpl) testActivity(
	version int64,
	attempt int32,
	lastHeartbeatTime time.Time,
	activityInfo *persistencespb.ActivityInfo,
) bool {

	if activityInfo.Version > version {
		// this should not retry, can be caused by failover or reset
		return false
	}

	if activityInfo.Version < version {
		// incoming version larger then local version, should update activity
		return true
	}

	// activityInfo.Version == version
	if activityInfo.Attempt > attempt {
		// this should not retry, can be caused by failover or reset
		return false
	}

	// activityInfo.Version == version
	if activityInfo.Attempt < attempt {
		// version equal & attempt larger then existing, should update activity
		return true
	}

	// activityInfo.Version == version & activityInfo.Attempt == attempt

	// last heartbeat after existing heartbeat & should update activity
	if activityInfo.LastHeartbeatUpdateTime != nil && activityInfo.LastHeartbeatUpdateTime.After(lastHeartbeatTime) {
		// this should not retry, can be caused by out of order delivery
		return false
	}
	return true
}

func (r *nDCActivityReplicatorImpl) testVersionHistory(
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	scheduledEventID int64,
	mutableState workflow.MutableState,
	incomingVersionHistory *historyspb.VersionHistory,
) (bool, error) {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return false, err
	}

	lastLocalItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return false, err
	}

	lastIncomingItem, err := versionhistory.GetLastVersionHistoryItem(incomingVersionHistory)
	if err != nil {
		return false, err
	}

	lcaItem, err := versionhistory.FindLCAVersionHistoryItem(currentVersionHistory, incomingVersionHistory)
	if err != nil {
		return false, err
	}

	// case 1: local version history is superset of incoming version history
	//  or incoming version history is superset of local version history
	//  resend the missing event if local version history doesn't have the schedule event

	// case 2: local version history and incoming version history diverged
	//  case 2-1: local version history has the higher version and discard the incoming event
	//  case 2-2: incoming version history has the higher version and resend the missing incoming events
	if versionhistory.IsLCAVersionHistoryItemAppendable(currentVersionHistory, lcaItem) ||
		versionhistory.IsLCAVersionHistoryItemAppendable(incomingVersionHistory, lcaItem) {
		// case 1
		if scheduledEventID > lcaItem.GetEventId() {
			return false, serviceerrors.NewRetryReplication(
				resendMissingEventMessage,
				namespaceID.String(),
				workflowID,
				runID,
				lcaItem.GetEventId(),
				lcaItem.GetVersion(),
				common.EmptyEventID,
				common.EmptyVersion,
			)
		}
	} else {
		// case 2
		if lastIncomingItem.GetVersion() < lastLocalItem.GetVersion() {
			// case 2-1
			return false, nil
		} else if lastIncomingItem.GetVersion() > lastLocalItem.GetVersion() {
			// case 2-2
			return false, serviceerrors.NewRetryReplication(
				resendHigherVersionMessage,
				namespaceID.String(),
				workflowID,
				runID,
				lcaItem.GetEventId(),
				lcaItem.GetVersion(),
				common.EmptyEventID,
				common.EmptyVersion,
			)
		}
	}

	state, _ := mutableState.GetWorkflowStateStatus()
	return state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, nil
}
