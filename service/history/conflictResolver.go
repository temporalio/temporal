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

package history

import (
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

type (
	conflictResolver interface {
		reset(prevRunID string, requestID string, replayEventID int64, startTime time.Time) (mutableState, error)
	}

	conflictResolverImpl struct {
		shard              ShardContext
		clusterMetadata    cluster.Metadata
		context            *workflowExecutionContext
		historyMgr         persistence.HistoryManager
		hSerializerFactory persistence.HistorySerializerFactory
		logger             bark.Logger
	}
)

func newConflictResolver(shard ShardContext, context *workflowExecutionContext, historyMgr persistence.HistoryManager,
	logger bark.Logger) *conflictResolverImpl {

	return &conflictResolverImpl{
		shard:              shard,
		clusterMetadata:    shard.GetService().GetClusterMetadata(),
		context:            context,
		historyMgr:         historyMgr,
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		logger:             logger,
	}
}

func (r *conflictResolverImpl) reset(prevRunID string, requestID string, replayEventID int64, startTime time.Time) (mutableState, error) {
	domainID := r.context.domainID
	execution := r.context.workflowExecution
	replayNextEventID := replayEventID + 1
	var nextPageToken []byte
	var history *shared.History
	var err error
	var resetMutableStateBuilder *mutableStateBuilder
	var sBuilder stateBuilder
	var lastFirstEventID int64
	var lastEvent *shared.HistoryEvent
	eventsToApply := replayNextEventID - common.FirstEventID
	for hasMore := true; hasMore; hasMore = len(nextPageToken) > 0 {
		history, nextPageToken, lastFirstEventID, err = r.getHistory(domainID, execution, common.FirstEventID,
			replayNextEventID, nextPageToken)
		if err != nil {
			r.logError("Conflict resolution err getting history.", err)
			return nil, err
		}

		batchSize := int64(len(history.Events))
		// NextEventID could be in the middle of the batch.  Trim the history events to not have more events then what
		// need to be applied
		if batchSize > eventsToApply {
			history.Events = history.Events[0:eventsToApply]
		}

		eventsToApply -= int64(len(history.Events))

		if len(history.Events) == 0 {
			break
		}

		firstEvent := history.Events[0]
		lastEvent = history.Events[len(history.Events)-1]
		if firstEvent.GetEventId() == common.FirstEventID {
			resetMutableStateBuilder = newMutableStateBuilderWithReplicationState(
				r.clusterMetadata.GetCurrentClusterName(),
				r.shard.GetConfig(),
				r.logger,
				firstEvent.GetVersion(),
			)

			sBuilder = newStateBuilder(r.shard, resetMutableStateBuilder, r.logger)
		}

		_, _, _, err = sBuilder.applyEvents(domainID, requestID, execution, history, nil)
		if err != nil {
			r.logError("Conflict resolution err applying events.", err)
			return nil, err
		}
		resetMutableStateBuilder.executionInfo.LastFirstEventID = lastFirstEventID
	}

	// Applying events to mutableState does not move the nextEventID.  Explicitly set nextEventID to new value
	resetMutableStateBuilder.executionInfo.NextEventID = replayNextEventID
	resetMutableStateBuilder.executionInfo.StartTimestamp = startTime
	// the last updated time is not important here, since this should be updated with event time afterwards
	resetMutableStateBuilder.executionInfo.LastUpdatedTimestamp = startTime

	sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastEvent.GetVersion())
	resetMutableStateBuilder.UpdateReplicationStateLastEventID(sourceCluster, lastEvent.GetVersion(), replayEventID)

	r.logger.WithField(logging.TagResetNextEventID, resetMutableStateBuilder.GetNextEventID()).Info("All events applied for execution.")
	msBuilder, err := r.context.resetWorkflowExecution(prevRunID, resetMutableStateBuilder)
	if err != nil {
		r.logError("Conflict resolution err reset workflow.", err)
	}
	return msBuilder, err
}

func (r *conflictResolverImpl) getHistory(domainID string, execution shared.WorkflowExecution, firstEventID,
	nextEventID int64, nextPageToken []byte) (*shared.History, []byte, int64, error) {

	response, err := r.historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
		DomainID:      domainID,
		Execution:     execution,
		FirstEventID:  firstEventID,
		NextEventID:   nextEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
	})

	if err != nil {
		return nil, nil, common.EmptyEventID, err
	}

	return response.History, response.NextPageToken, response.LastFirstEventID, nil
}

func (r *conflictResolverImpl) logError(msg string, err error) {
	r.logger.WithFields(bark.Fields{
		logging.TagErr: err,
	}).Error(msg)
}
