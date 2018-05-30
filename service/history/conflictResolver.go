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

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

type (
	conflictResolver struct {
		shard              ShardContext
		context            *workflowExecutionContext
		historyMgr         persistence.HistoryManager
		hSerializerFactory persistence.HistorySerializerFactory
		logger             bark.Logger
	}
)

func newConflictResolver(shard ShardContext, context *workflowExecutionContext, historyMgr persistence.HistoryManager,
	logger bark.Logger) *conflictResolver {

	return &conflictResolver{
		shard:              shard,
		context:            context,
		historyMgr:         historyMgr,
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		logger:             logger,
	}
}

func (r *conflictResolver) reset(replayEventID int64, startTime time.Time) (*mutableStateBuilder, error) {
	domainID := r.context.domainID
	execution := r.context.workflowExecution
	replayNextEventID := replayEventID + 1
	var nextPageToken []byte
	var history *shared.History
	var err error
	var resetMutableStateBuilder *mutableStateBuilder
	var sBuilder *stateBuilder
	var lastFirstEventID int64
	eventsToApply := replayNextEventID - common.FirstEventID
	requestID := uuid.New()
	for hasMore := true; hasMore; hasMore = len(nextPageToken) > 0 {
		history, nextPageToken, lastFirstEventID, err = r.getHistory(domainID, execution, common.FirstEventID,
			replayNextEventID, nextPageToken)
		r.logger.Debugf("Conflict Resolver GetHistory.  History Length: %v, token: %v, err: %v",
			len(history.Events), nextPageToken, err)
		if err != nil {
			return nil, err
		}

		batchSize := int64(len(history.Events))
		// NextEventID could be in the middle of the batch.  Trim the history events to not have more events then what
		// need to be applied
		if batchSize > eventsToApply {
			history.Events = history.Events[0:eventsToApply]
		}

		eventsToApply -= batchSize

		if len(history.Events) == 0 {
			break
		}

		firstEvent := history.Events[0]
		if firstEvent.GetEventId() == common.FirstEventID {
			resetMutableStateBuilder = newMutableStateBuilderWithReplicationState(r.shard.GetConfig(), r.logger,
				firstEvent.GetVersion())

			sBuilder = newStateBuilder(r.shard, resetMutableStateBuilder, r.logger)
		}

		_, _, _, err = sBuilder.applyEvents(common.EmptyVersion, "", domainID, requestID, execution, history, nil)
		if err != nil {
			return nil, err
		}
		resetMutableStateBuilder.executionInfo.LastFirstEventID = lastFirstEventID
	}

	// Applying events to mutableState does not move the nextEventID.  Explicitly set nextEventID to new value
	resetMutableStateBuilder.executionInfo.NextEventID = replayNextEventID
	resetMutableStateBuilder.executionInfo.StartTimestamp = startTime
	// the last updated time is not important here, since this should be updated with event time afterwards
	resetMutableStateBuilder.executionInfo.LastUpdatedTimestamp = startTime

	r.logger.Infof("All events applied for execution.  WorkflowID: %v, RunID: %v, NextEventID: %v",
		execution.GetWorkflowId(), execution.GetRunId(), resetMutableStateBuilder.GetNextEventID())

	return r.context.resetWorkflowExecution(resetMutableStateBuilder)
}

func (r *conflictResolver) getHistory(domainID string, execution shared.WorkflowExecution, firstEventID,
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

	lastFirstEventID := common.EmptyEventID
	historyEvents := []*shared.HistoryEvent{}
	for _, e := range response.Events {
		persistence.SetSerializedHistoryDefaults(&e)
		s, _ := r.hSerializerFactory.Get(e.EncodingType)
		history, err1 := s.Deserialize(&e)
		if err1 != nil {
			return nil, nil, common.EmptyEventID, err1
		}
		if len(history.Events) > 0 {
			lastFirstEventID = history.Events[0].GetEventId()
		}
		historyEvents = append(historyEvents, history.Events...)
	}

	executionHistory := &shared.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, lastFirstEventID, nil
}
