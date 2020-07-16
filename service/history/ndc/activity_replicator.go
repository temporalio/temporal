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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination activity_replicator_mock.go

package ndc

import (
	ctx "context"
	"time"

	"go.uber.org/cadence/.gen/go/shared"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
)

const (
	resendMissingEventMessage  = "Resend missed sync activity events"
	resendHigherVersionMessage = "Resend sync activity events due to a higher version received"
	errRetrySyncActivityMsg    = "retry on applying sync activity"
)

type (
	// ActivityReplicator handles sync activity process
	ActivityReplicator interface {
		SyncActivity(
			ctx ctx.Context,
			request *h.SyncActivityRequest,
		) error
	}

	activityReplicatorImpl struct {
		executionCache  *execution.Cache
		clusterMetadata cluster.Metadata
		logger          log.Logger
	}
)

var _ ActivityReplicator = (*activityReplicatorImpl)(nil)

// NewActivityReplicator creates activity replicator
func NewActivityReplicator(
	shard shard.Context,
	executionCache *execution.Cache,
	logger log.Logger,
) ActivityReplicator {

	return &activityReplicatorImpl{
		executionCache:  executionCache,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		logger:          logger.WithTags(tag.ComponentHistoryReplicator),
	}
}

func (r *activityReplicatorImpl) SyncActivity(
	ctx ctx.Context,
	request *h.SyncActivityRequest,
) (retError error) {

	// sync activity info will only be sent from active side, when
	// 1. activity has retry policy and activity got started
	// 2. activity heart beat
	// no sync activity task will be sent when active side fail / timeout activity,
	// since standby side does not have activity retry timer
	domainID := request.GetDomainId()
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}

	context, release, err := r.executionCache.GetOrCreateWorkflowExecution(ctx, domainID, workflowExecution)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := context.LoadWorkflowExecution()
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
		workflowExecution.GetWorkflowId(),
		workflowExecution.GetRunId(),
		scheduleID,
		version,
		mutableState,
		request.GetVersionHistory(),
	)
	if err != nil {
		return err
	}
	if !shouldApply {
		return nil
	}

	ai, ok := mutableState.GetActivityInfo(scheduleID)
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
	err = mutableState.ReplicateActivityInfo(request, resetActivityTimerTaskStatus)
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

	// passive logic need to explicitly call create timer
	now := time.Unix(0, eventTime)
	if _, err := execution.NewTimerSequence(
		clock.NewEventTimeSource().Update(now),
		mutableState,
	).CreateNextActivityTimer(); err != nil {
		return err
	}

	updateMode := persistence.UpdateWorkflowModeUpdateCurrent
	if state, _ := mutableState.GetWorkflowStateCloseStatus(); state == persistence.WorkflowStateZombie {
		updateMode = persistence.UpdateWorkflowModeBypassCurrent
	}

	return context.UpdateWorkflowExecutionWithNew(
		now,
		updateMode,
		nil, // no new workflow
		nil, // no new workflow
		execution.TransactionPolicyPassive,
		nil,
	)
}

func (r *activityReplicatorImpl) shouldApplySyncActivity(
	domainID string,
	workflowID string,
	runID string,
	scheduleID int64,
	activityVersion int64,
	mutableState execution.MutableState,
	incomingRawVersionHistory *workflow.VersionHistory,
) (bool, error) {

	if mutableState.GetVersionHistories() != nil {
		currentVersionHistory, err := mutableState.GetVersionHistories().GetCurrentVersionHistory()
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

		lcaItem, err := currentVersionHistory.FindLCAItem(incomingVersionHistory)
		if err != nil {
			return false, err
		}

		// case 1: local version history is superset of incoming version history
		// or incoming version history is superset of local version history
		// resend the missing event if local version history doesn't have the schedule event

		// case 2: local version history and incoming version history diverged
		// case 2-1: local version history has the higher version and discard the incoming event
		// case 2-2: incoming version history has the higher version and resend the missing incoming events
		if currentVersionHistory.IsLCAAppendable(lcaItem) || incomingVersionHistory.IsLCAAppendable(lcaItem) {
			// case 1
			if scheduleID > lcaItem.GetEventID() {
				return false, newNDCRetryTaskErrorWithHint(
					resendMissingEventMessage,
					domainID,
					workflowID,
					runID,
					common.Int64Ptr(lcaItem.GetEventID()),
					common.Int64Ptr(lcaItem.GetVersion()),
					nil,
					nil,
				)
			}
		} else {
			// case 2
			if lastIncomingItem.GetVersion() < lastLocalItem.GetVersion() {
				// case 2-1
				return false, nil
			} else if lastIncomingItem.GetVersion() > lastLocalItem.GetVersion() {
				// case 2-2
				return false, newNDCRetryTaskErrorWithHint(
					resendHigherVersionMessage,
					domainID,
					workflowID,
					runID,
					common.Int64Ptr(lcaItem.GetEventID()),
					common.Int64Ptr(lcaItem.GetVersion()),
					nil,
					nil,
				)
			}
		}

		if state, _ := mutableState.GetWorkflowStateCloseStatus(); state == persistence.WorkflowStateCompleted {
			return false, nil
		}
	} else if mutableState.GetReplicationState() != nil {
		// TODO when 2DC is deprecated, remove this block
		if !mutableState.IsWorkflowExecutionRunning() {
			// perhaps conflict resolution force termination
			return false, nil
		}

		if scheduleID >= mutableState.GetNextEventID() {
			lastWriteVersion, err := mutableState.GetLastWriteVersion()
			if err != nil {
				return false, err
			}
			if activityVersion < lastWriteVersion {
				// this can happen if target workflow has different history branch
				return false, nil
			}
			// version >= last write version
			// this can happen if out of order delivery happens
			return false, NewRetryTaskErrorWithHint(
				errRetrySyncActivityMsg,
				domainID,
				workflowID,
				runID,
				mutableState.GetNextEventID(),
			)
		}
	} else {
		return false, &shared.InternalServiceError{Message: "The workflow is neither 2DC or 3DC enabled."}
	}

	return true, nil
}

// NewRetryTaskErrorWithHint returns a 2DC resend error
// TODO: remove it after remove 2DC code
func NewRetryTaskErrorWithHint(
	msg string,
	domainID string,
	workflowID string,
	runID string,
	nextEventID int64,
) *workflow.RetryTaskError {

	return &workflow.RetryTaskError{
		Message:     msg,
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		NextEventId: common.Int64Ptr(nextEventID),
	}
}
