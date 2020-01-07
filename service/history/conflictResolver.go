// Copyright (c) 2017 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination conflictResolver_mock.go -self_package github.com/uber/cadence/service/history

package history

import (
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	conflictResolver interface {
		reset(
			prevRunID string,
			prevLastWriteVersion int64,
			prevState int,
			requestID string,
			replayEventID int64,
			info *persistence.WorkflowExecutionInfo,
			updateCondition int64,
		) (mutableState, error)
	}

	conflictResolverImpl struct {
		shard           ShardContext
		clusterMetadata cluster.Metadata
		context         workflowExecutionContext
		historyV2Mgr    persistence.HistoryManager
		logger          log.Logger
	}
)

func newConflictResolver(shard ShardContext, context workflowExecutionContext, historyV2Mgr persistence.HistoryManager,
	logger log.Logger) *conflictResolverImpl {

	return &conflictResolverImpl{
		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		context:         context,
		historyV2Mgr:    historyV2Mgr,
		logger:          logger,
	}
}

func (r *conflictResolverImpl) reset(
	prevRunID string,
	prevLastWriteVersion int64,
	prevState int,
	requestID string,
	replayEventID int64,
	info *persistence.WorkflowExecutionInfo,
	updateCondition int64,
) (mutableState, error) {

	domainID := r.context.getDomainID()
	execution := *r.context.getExecution()
	startTime := info.StartTimestamp
	branchToken := info.BranchToken // in 2DC world branch token is stored in execution info
	replayNextEventID := replayEventID + 1

	domainEntry, err := r.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, err
	}

	var nextPageToken []byte
	var resetMutableStateBuilder *mutableStateBuilder
	var sBuilder stateBuilder
	var history []*shared.HistoryEvent
	var totalSize int64

	eventsToApply := replayNextEventID - common.FirstEventID
	for hasMore := true; hasMore; hasMore = len(nextPageToken) > 0 {
		var size int
		history, size, _, nextPageToken, err = r.getHistory(domainID, execution, common.FirstEventID, replayNextEventID, nextPageToken, branchToken)
		if err != nil {
			r.logError("Conflict resolution err getting history.", err)
			return nil, err
		}

		batchSize := int64(len(history))
		// NextEventID could be in the middle of the batch.  Trim the history events to not have more events then what
		// need to be applied
		if batchSize > eventsToApply {
			history = history[0:eventsToApply]
		}

		eventsToApply -= int64(len(history))

		if len(history) == 0 {
			break
		}

		firstEvent := history[0]
		if firstEvent.GetEventId() == common.FirstEventID {
			resetMutableStateBuilder = newMutableStateBuilderWithReplicationState(
				r.shard,
				r.shard.GetEventsCache(),
				r.logger,
				domainEntry,
			)

			sBuilder = newStateBuilder(
				r.shard,
				r.logger,
				resetMutableStateBuilder,
				func(mutableState mutableState) mutableStateTaskGenerator {
					return newMutableStateTaskGenerator(r.shard.GetDomainCache(), r.logger, mutableState)
				},
			)
		}

		_, err = sBuilder.applyEvents(domainID, requestID, execution, history, nil, false)
		if err != nil {
			r.logError("Conflict resolution err applying events.", err)
			return nil, err
		}
		totalSize += int64(size)
	}

	if resetMutableStateBuilder == nil {
		return nil, &shared.BadRequestError{
			Message: "unable to create reset mutable state",
		}
	}

	// reset branchToken to the original one(it has been set to a wrong branchToken in applyEvents for startEvent)
	resetMutableStateBuilder.executionInfo.BranchToken = branchToken // in 2DC world branch token is stored in execution info

	resetMutableStateBuilder.executionInfo.StartTimestamp = startTime
	// the last updated time is not important here, since this should be updated with event time afterwards
	resetMutableStateBuilder.executionInfo.LastUpdatedTimestamp = startTime

	// close the rebuild transaction on reset mutable state, since we do not want oo write the
	// events used in the replay to be persisted again
	_, _, err = resetMutableStateBuilder.CloseTransactionAsSnapshot(
		startTime,
		transactionPolicyPassive,
	)
	if err != nil {
		return nil, err
	}

	resetMutableStateBuilder.SetUpdateCondition(updateCondition)
	if r.shard.GetConfig().AdvancedVisibilityWritingMode() != common.AdvancedVisibilityWritingModeOff {
		// whenever a reset of mutable state is done, we need to sync the workflow search attribute
		resetMutableStateBuilder.AddTransferTasks(&persistence.UpsertWorkflowSearchAttributesTask{})
	}

	r.logger.Info("All events applied for execution.", tag.WorkflowResetNextEventID(resetMutableStateBuilder.GetNextEventID()))
	r.context.setHistorySize(totalSize)
	if err := r.context.conflictResolveWorkflowExecution(
		startTime,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		resetMutableStateBuilder,
		nil,
		nil,
		nil,
		nil,
		nil,
		&persistence.CurrentWorkflowCAS{
			PrevRunID:            prevRunID,
			PrevLastWriteVersion: prevLastWriteVersion,
			PrevState:            prevState,
		},
	); err != nil {
		r.logError("Conflict resolution err reset workflow.", err)
	}
	return r.context.loadWorkflowExecution()
}

func (r *conflictResolverImpl) getHistory(domainID string, execution shared.WorkflowExecution, firstEventID,
	nextEventID int64, nextPageToken []byte, branchToken []byte) ([]*shared.HistoryEvent, int, int64, []byte, error) {

	response, err := r.historyV2Mgr.ReadHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(r.shard.GetShardID()),
	})
	if err != nil {
		return nil, 0, 0, nil, err
	}
	return response.HistoryEvents, response.Size, response.LastFirstEventID, response.NextPageToken, nil
}

func (r *conflictResolverImpl) logError(msg string, err error) {
	r.logger.Error(msg, tag.Error(err))
}
