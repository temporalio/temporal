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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCActivityReplicator_mock.go

package history

import (
	ctx "context"
	"time"

	"go.temporal.io/temporal/.gen/go/shared"

	h "github.com/temporalio/temporal/.gen/go/history"
	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCActivityReplicator interface {
		SyncActivity(
			ctx ctx.Context,
			request *h.SyncActivityRequest,
		) error
	}

	nDCActivityReplicatorImpl struct {
		historyCache    *historyCache
		clusterMetadata cluster.Metadata
		logger          log.Logger
	}
)

func newNDCActivityReplicator(
	shard ShardContext,
	historyCache *historyCache,
	logger log.Logger,
) *nDCActivityReplicatorImpl {

	return &nDCActivityReplicatorImpl{
		historyCache:    historyCache,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		logger:          logger.WithTags(tag.ComponentHistoryReplicator),
	}
}

func (r *nDCActivityReplicatorImpl) SyncActivity(
	ctx ctx.Context,
	request *h.SyncActivityRequest,
) (retError error) {

	// sync activity info will only be sent from active side, when
	// 1. activity has retry policy and activity got started
	// 2. activity heart beat
	// no sync activity task will be sent when active side fail / timeout activity,
	// since standby side does not have activity retry timer

	domainID := request.GetDomainId()
	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}

	context, release, err := r.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}

		// this can happen if the workflow start event and this sync activity task are out of order
		// or the target workflow is long gone
		// the safe solution to this is to throw away the sync activity task
		// or otherwise, worker attempt will exceeds limit and put this message to DLQ
		return nil
	}

	version := request.GetVersion()
	scheduleID := request.GetScheduledId()
	shouldApply, err := r.shouldApplySyncActivity(
		domainID,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		scheduleID,
		version,
		msBuilder,
		request.GetVersionHistory(),
	)
	if err != nil {
		return err
	}
	if !shouldApply {
		return nil
	}

	ai, ok := msBuilder.GetActivityInfo(scheduleID)
	if !ok {
		// this should not retry, can be caused by out of order delivery
		// since the activity is already finished
		return nil
	}

	if ai.Version > request.GetVersion() {
		// this should not retry, can be caused by failover or reset
		return nil
	}

	if ai.Version == request.GetVersion() {
		if ai.Attempt > request.GetAttempt() {
			// this should not retry, can be caused by failover or reset
			return nil
		}
		if ai.Attempt == request.GetAttempt() {
			lastHeartbeatTime := time.Unix(0, request.GetLastHeartbeatTime())
			if ai.LastHeartBeatUpdatedTime.After(lastHeartbeatTime) {
				// this should not retry, can be caused by out of order delivery
				return nil
			}
			// version equal & attempt equal & last heartbeat after existing heartbeat
			// should update activity
		}
		// version equal & attempt larger then existing, should update activity
	}
	// version larger then existing, should update activity

	// calculate whether to reset the activity timer task status bits
	// reset timer task status bits if
	// 1. same source cluster & attempt changes
	// 2. different source cluster
	resetActivityTimerTaskStatus := false
	if !r.clusterMetadata.IsVersionFromSameCluster(request.GetVersion(), ai.Version) {
		resetActivityTimerTaskStatus = true
	} else if ai.Attempt < request.GetAttempt() {
		resetActivityTimerTaskStatus = true
	}
	err = msBuilder.ReplicateActivityInfo(request, resetActivityTimerTaskStatus)
	if err != nil {
		return err
	}

	// see whether we need to refresh the activity timer
	eventTime := request.GetScheduledTime()
	if eventTime < request.GetStartedTime() {
		eventTime = request.GetStartedTime()
	}
	if eventTime < request.GetLastHeartbeatTime() {
		eventTime = request.GetLastHeartbeatTime()
	}
	now := time.Unix(0, eventTime)
	timerTasks := []persistence.Task{}
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	timerBuilder := newTimerBuilder(timeSource)
	tt, err := timerBuilder.GetActivityTimerTaskIfNeeded(msBuilder)
	if err != nil {
		timerTasks = append(timerTasks, tt)
	}

	msBuilder.AddTimerTasks(timerTasks...)
	return context.updateWorkflowExecutionAsPassive(now)
}

func (r *nDCActivityReplicatorImpl) shouldApplySyncActivity(
	domainID string,
	workflowID string,
	runID string,
	scheduleID int64,
	activityVersion int64,
	msBuilder mutableState,
	incomingRawVersionHistory *workflow.VersionHistory,
) (bool, error) {

	if msBuilder.GetVersionHistories() != nil {
		currentVersionHistory, err := msBuilder.GetVersionHistories().GetCurrentVersionHistory()
		if err != nil {
			return false, err
		}
		lastLocalItem, err := currentVersionHistory.GetLastItem()
		if err != nil {
			return false, err
		}
		incomingVersionHistory := persistence.NewVersionHistoryFromThrift(incomingRawVersionHistory)
		lastIncomingItem, err := incomingVersionHistory.GetLastItem()
		if err != nil {
			return false, err
		}
		if lastIncomingItem.GetVersion() < lastLocalItem.GetVersion() {
			// the incoming branch will lose to the local branch
			// discard this task
			return false, nil
		}

		lcaItem, err := currentVersionHistory.FindLCAItem(incomingVersionHistory)
		if err != nil {
			return false, err
		}

		// version history matches and activity schedule ID appears in local version history
		if currentVersionHistory.IsLCAAppendable(lcaItem) && scheduleID <= lastLocalItem.GetEventID() {
			return true, nil
		}

		// incoming branch will win the local branch
		// resend the events with higher version
		if scheduleID+1 <= lastIncomingItem.GetEventID() {
			// according to the incoming version history, we can calculate the
			// the end event ID & version to fetch from remote
			endEventID := scheduleID + 1
			endEventVersion, err := incomingVersionHistory.GetEventVersion(scheduleID + 1)
			if err != nil {
				return false, err
			}
			return false, newNDCRetryTaskErrorWithHint(
				domainID,
				workflowID,
				runID,
				common.Int64Ptr(lcaItem.GetEventID()),
				common.Int64Ptr(lcaItem.GetVersion()),
				common.Int64Ptr(endEventID),
				common.Int64Ptr(endEventVersion),
			)
		}

		// activity schedule event is the last event
		// use nil event ID & version indicating re-send to end
		return false, newNDCRetryTaskErrorWithHint(
			domainID,
			workflowID,
			runID,
			common.Int64Ptr(lcaItem.GetEventID()),
			common.Int64Ptr(lcaItem.GetVersion()),
			nil,
			nil,
		)
	} else if msBuilder.GetReplicationState() != nil {
		// TODO when 2DC is deprecated, remove this block
		if !msBuilder.IsWorkflowExecutionRunning() {
			// perhaps conflict resolution force termination
			return false, nil
		}

		if scheduleID >= msBuilder.GetNextEventID() {
			lastWriteVersion, err := msBuilder.GetLastWriteVersion()
			if err != nil {
				return false, err
			}
			if activityVersion < lastWriteVersion {
				// this can happen if target workflow has different history branch
				return false, nil
			}
			// version >= last write version
			// this can happen if out of order delivery happens
			return false, newRetryTaskErrorWithHint(
				ErrRetrySyncActivityMsg,
				domainID,
				workflowID,
				runID,
				msBuilder.GetNextEventID(),
			)
		}
	} else {
		return false, &shared.InternalServiceError{Message: "The workflow is neither 2DC or 3DC enabled."}
	}
	return true, nil
}
