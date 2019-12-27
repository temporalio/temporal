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

package ndc

import (
	"math"
	"reflect"
	"time"

	"github.com/uber/cadence/common/persistence"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	test "github.com/uber/cadence/common/testing"
)

func (s *nDCIntegrationTestSuite) TestReplicationMessageApplication() {

	workflowID := "replication-message-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	tasklist := "event-generator-taskList"

	var historyBatch []*shared.History
	s.generator = test.InitializeHistoryEventGenerator(s.domainName, 1)

	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		historyEvents := &shared.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*shared.HistoryEvent))
		}
		historyBatch = append(historyBatch, historyEvents)
	}

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)

	s.applyEventsThroughFetcher(
		workflowID,
		runID,
		workflowType,
		tasklist,
		versionHistory,
		historyBatch,
	)

	// Applying replication messages through fetcher is Async.
	// So we need to retry a couple of times.
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		err := s.verifyEventHistory(workflowID, runID, historyBatch)
		if err == nil {
			return
		}
	}

	s.Fail("Verification of replicated messages failed")
}

func (s *nDCIntegrationTestSuite) TestReplicationMessageDLQ() {

	workflowID := "replication-message-dlq-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	tasklist := "event-generator-taskList"

	var historyBatch []*shared.History
	s.generator = test.InitializeHistoryEventGenerator(s.domainName, 1)

	events := s.generator.GetNextVertices()
	historyEvents := &shared.History{}
	for _, event := range events {
		historyEvents.Events = append(historyEvents.Events, event.GetData().(*shared.HistoryEvent))
	}
	historyBatch = append(historyBatch, historyEvents)

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)

	s.NotNil(historyBatch)
	historyBatch[0].Events[1].Version = common.Int64Ptr(2)

	s.applyEventsThroughFetcher(
		workflowID,
		runID,
		workflowType,
		tasklist,
		versionHistory,
		historyBatch,
	)

	execMgrFactory := s.active.GetExecutionManagerFactory()
	executionManager, err := execMgrFactory.NewExecutionManager(0)
	s.NoError(err)

	expectedDLQMsgs := map[int64]bool{}
	for _, batch := range historyBatch {
		firstEventID := batch.Events[0].GetEventId()
		expectedDLQMsgs[firstEventID] = true
	}

	// Applying replication messages through fetcher is Async.
	// So we need to retry a couple of times.
Loop:
	for i := 0; i < 60; i++ {
		time.Sleep(time.Second)

		actualDLQMsgs := map[int64]bool{}
		request := persistence.NewGetReplicationTasksFromDLQRequest(
			"standby", -1, math.MaxInt64, math.MaxInt64, nil,
		)
		var token []byte
		for doPaging := true; doPaging; doPaging = len(token) > 0 {
			request.NextPageToken = token
			response, err := executionManager.GetReplicationTasksFromDLQ(request)
			if err != nil {
				continue Loop
			}
			token = response.NextPageToken

			for _, task := range response.Tasks {
				firstEventID := task.FirstEventID
				actualDLQMsgs[firstEventID] = true
			}
		}
		if reflect.DeepEqual(expectedDLQMsgs, actualDLQMsgs) {
			return
		}
	}

	s.Fail("Failed to get messages from DLQ.")
}
