package ndc

import (
	"math"
	"reflect"
	"time"

	"github.com/pborman/uuid"
	eventpb "go.temporal.io/temporal-proto/event"

	"github.com/temporalio/temporal/common/persistence"
	test "github.com/temporalio/temporal/common/testing"
)

func (s *nDCIntegrationTestSuite) TestReplicationMessageApplication() {

	workflowID := "replication-message-test" + uuid.New()
	runID := uuid.New()
	workflowType := "event-generator-workflow-type"
	tasklist := "event-generator-taskList"

	var historyBatch []*eventpb.History
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, 1)

	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		historyEvents := &eventpb.History{}
		for _, event := range events {
			historyEvents.Events = append(historyEvents.Events, event.GetData().(*eventpb.HistoryEvent))
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

	var historyBatch []*eventpb.History
	s.generator = test.InitializeHistoryEventGenerator(s.namespace, 1)

	events := s.generator.GetNextVertices()
	historyEvents := &eventpb.History{}
	for _, event := range events {
		historyEvents.Events = append(historyEvents.Events, event.GetData().(*eventpb.HistoryEvent))
	}
	historyBatch = append(historyBatch, historyEvents)

	versionHistory := s.eventBatchesToVersionHistory(nil, historyBatch)

	s.NotNil(historyBatch)
	historyBatch[0].Events[1].Version = 2

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
				firstEventID := task.GetFirstEventId()
				actualDLQMsgs[firstEventID] = true
			}
		}
		if reflect.DeepEqual(expectedDLQMsgs, actualDLQMsgs) {
			return
		}
	}

	s.Fail("Failed to get messages from DLQ")
}
