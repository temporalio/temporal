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

package ndc

import (
	"math"
	"reflect"
	"time"

	"github.com/pborman/uuid"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common/persistence"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/host"
	"go.temporal.io/server/service/history/tasks"
)

func (s *nDCIntegrationTestSuite) TestReplicationMessageApplication() {

	workflowID := "replication-message-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	var historyBatch []*historypb.History
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, 2)

	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		historyEvents := &historypb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
		}
		historyBatch = append(historyBatch, historyEvents)
	}

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)

	s.applyEventsThroughFetcher(
		workflowID,
		runID,
		workflowType,
		taskqueue,
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
	var shardID int32 = 1
	workflowID := "replication-message-dlq-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	taskqueue := "event-generator-taskQueue"

	var historyBatch []*historypb.History
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, s.namespaceID, 1)

	events := s.generator.GetNextVertices()
	historyEvents := &historypb.History{}
	for _, event := range events {
		historyEvents.Events = append(historyEvents.Events, event.GetData().(*historypb.HistoryEvent))
	}
	historyBatch = append(historyBatch, historyEvents)

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)

	s.NotNil(historyBatch)
	historyBatch[0].Events[1].Version = 2

	s.applyEventsThroughFetcher(
		workflowID,
		runID,
		workflowType,
		taskqueue,
		versionHistory,
		historyBatch,
	)

	executionManager := s.active.GetExecutionManager()
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
		request := &persistence.GetReplicationTasksFromDLQRequest{
			GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
				ShardID:             shardID,
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(0),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(math.MaxInt64),
				BatchSize:           math.MaxInt64,
				NextPageToken:       nil,
			},
			SourceClusterName: "standby",
		}
		var token []byte
		for doPaging := true; doPaging; doPaging = len(token) > 0 {
			request.NextPageToken = token
			response, err := executionManager.GetReplicationTasksFromDLQ(host.NewContext(), request)
			if err != nil {
				continue Loop
			}
			token = response.NextPageToken

			for _, task := range response.Tasks {
				firstEventID := task.(*tasks.HistoryReplicationTask).FirstEventID
				actualDLQMsgs[firstEventID] = true
			}
		}
		if reflect.DeepEqual(expectedDLQMsgs, actualDLQMsgs) {
			return
		}
	}

	s.Fail("Failed to get messages from DLQ")
}
