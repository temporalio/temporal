package ndc

import (
	"math"
	"reflect"
	"time"

	"github.com/google/uuid"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/persistence"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
)

func (s *NDCFunctionalTestSuite) TestReplicationMessageDLQ() {
	s.T().SkipNow()

	var shardID int32 = 1
	workflowID := "replication-message-dlq-test" + uuid.NewString()
	runID := uuid.NewString()
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

	executionManager := s.cluster.ExecutionManager()
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
			response, err := executionManager.GetReplicationTasksFromDLQ(testcore.NewContext(), request)
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
