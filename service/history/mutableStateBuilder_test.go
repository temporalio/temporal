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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/persistence"
)

type (
	mutableStateSuite struct {
		suite.Suite
		msBuilder       *mutableStateBuilder
		mockShard       *shardContextImpl
		mockEventsCache *MockEventsCache
		logger          log.Logger
	}
)

func TestMutableStateSuite(t *testing.T) {
	s := new(mutableStateSuite)
	suite.Run(t, s)
}

func (s *mutableStateSuite) SetupSuite() {

}

func (s *mutableStateSuite) TearDownSuite() {

}

func (s *mutableStateSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockShard = &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockEventsCache = &MockEventsCache{}
	s.msBuilder = newMutableStateBuilder(s.mockShard, s.mockEventsCache,
		s.logger, testLocalDomainEntry)
}

func (s *mutableStateSuite) TearDownTest() {

}

func (s *mutableStateSuite) TestTransientDecisionCompletionFirstBatchReplicated_ReplicateDecisionCompleted() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newDecisionScheduleEvent, newDecisionStartedEvent := s.prepareTransientDecisionCompletionFirstBatchReplicated(version, runID)

	newDecisionCompletedEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(newDecisionStartedEvent.GetEventId() + 1),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		EventType: shared.EventTypeDecisionTaskCompleted.Ptr(),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: common.Int64Ptr(newDecisionScheduleEvent.GetEventId()),
			StartedEventId:   common.Int64Ptr(newDecisionStartedEvent.GetEventId()),
			Identity:         common.StringPtr("some random identity"),
		},
	}
	s.msBuilder.ReplicateDecisionTaskCompletedEvent(newDecisionCompletedEvent)
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestTransientDecisionCompletionFirstBatchReplicated_FailoverDecisionTimeout() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newDecisionScheduleEvent, newDecisionStartedEvent := s.prepareTransientDecisionCompletionFirstBatchReplicated(version, runID)

	s.msBuilder.UpdateReplicationStateVersion(version+1, true)
	s.NotNil(s.msBuilder.AddDecisionTaskTimedOutEvent(newDecisionScheduleEvent.GetEventId(), newDecisionStartedEvent.GetEventId()))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(1, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestInMemDecisionTask() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)
	eventTimeSource := clock.NewEventTimeSource()
	s.msBuilder.timeSource = eventTimeSource

	s.assertMemDecisionTaskState(false, false, false, false, false)

	// error on start mem decision task before schedule
	s.Error(s.msBuilder.AddInMemoryDecisionTaskStarted())
	s.assertMemDecisionTaskState(false, false, false, false, false)

	// no error on schedule in mem decision task
	s.NoError(s.msBuilder.AddInMemoryDecisionTaskScheduled(time.Second))
	s.assertMemDecisionTaskState(true, true, false, false, false)

	// attempt to schedule mem decision task again is an error
	s.Error(s.msBuilder.AddInMemoryDecisionTaskScheduled(time.Second))
	s.assertMemDecisionTaskState(true, true, false, false, false)

	// no error on start decision task
	s.NoError(s.msBuilder.AddInMemoryDecisionTaskStarted())
	s.assertMemDecisionTaskState(true, false, true, false, false)

	// attempting to schedule decision task with started mem decision task is an error
	di, err := s.msBuilder.AddDecisionTaskScheduledEvent(false)
	s.Nil(di)
	s.Error(err)
	s.assertMemDecisionTaskState(true, false, true, false, false)

	// expire started mem decision task results in no mem decision task
	eventTimeSource.Update(eventTimeSource.Now().Add(500 * time.Millisecond))
	s.assertMemDecisionTaskState(true, false, true, false, false)
	eventTimeSource.Update(eventTimeSource.Now().Add(500 * time.Millisecond))
	s.assertMemDecisionTaskState(false, false, false, false, false)

	// no error on schedule decision task
	di, err = s.msBuilder.AddDecisionTaskScheduledEvent(false)
	s.NotNil(di)
	s.NoError(err)
	s.assertMemDecisionTaskState(false, false, false, true, false)

	// it is an error to try to schedule a mem decision task while there is a scheduled decision task
	s.Error(s.msBuilder.AddInMemoryDecisionTaskScheduled(time.Second))
	s.assertMemDecisionTaskState(false, false, false, true, false)

	// start the decision task
	he, di, err := s.msBuilder.AddDecisionTaskStartedEvent(di.ScheduleID, di.RequestID, &workflow.PollForDecisionTaskRequest{
		Domain: common.StringPtr("test-domain"),
		TaskList: &workflow.TaskList{
			Name: common.StringPtr("test-task-list-name"),
			Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
		},
		Identity:       common.StringPtr("test-identity"),
		BinaryChecksum: common.StringPtr("binary-checksum"),
	})
	s.NotNil(he)
	s.NotNil(di)
	s.NoError(err)
	s.assertMemDecisionTaskState(false, false, false, true, true)

	// error to schedule mem decision task while there is started decision task
	s.Error(s.msBuilder.AddInMemoryDecisionTaskScheduled(time.Second))
	s.assertMemDecisionTaskState(false, false, false, true, true)

	// delete decision task
	s.msBuilder.DeleteDecision()
	s.assertMemDecisionTaskState(false, false, false, false, false)

	// no error on schedule mem decision task
	s.NoError(s.msBuilder.AddInMemoryDecisionTaskScheduled(time.Second))
	s.assertMemDecisionTaskState(true, true, false, false, false)

	// delete mem decision task
	s.msBuilder.DeleteInMemoryDecisionTask()
	s.assertMemDecisionTaskState(false, false, false, false, false)

	// no error on schedule mem decision task
	s.NoError(s.msBuilder.AddInMemoryDecisionTaskScheduled(time.Second))
	s.assertMemDecisionTaskState(true, true, false, false, false)

	// schedule decision task to overwrite mem decision task
	di, err = s.msBuilder.AddDecisionTaskScheduledEvent(false)
	s.NoError(err)
	s.NotNil(di)
	s.assertMemDecisionTaskState(false, false, false, true, false)
}

func (s *mutableStateSuite) assertMemDecisionTaskState(hasMemDecisionTask, hasMemScheduledDecisionTask, hasMemStartedDecisionTask, hasPending, hasInflight bool) {
	s.Equal(hasMemDecisionTask, s.msBuilder.HasInMemoryDecisionTask())
	s.Equal(hasMemScheduledDecisionTask, s.msBuilder.HasScheduledInMemoryDecisionTask())
	s.Equal(hasMemStartedDecisionTask, s.msBuilder.HasStartedInMemoryDecisionTask())
	s.Equal(hasPending, s.msBuilder.HasPendingDecision())
	s.Equal(hasInflight, s.msBuilder.HasInFlightDecision())
}

func (s *mutableStateSuite) TestTransientDecisionCompletionFirstBatchReplicated_FailoverDecisionFailed() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithReplicationStateWithEventV2(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newDecisionScheduleEvent, newDecisionStartedEvent := s.prepareTransientDecisionCompletionFirstBatchReplicated(version, runID)

	s.msBuilder.UpdateReplicationStateVersion(version+1, true)
	s.NotNil(s.msBuilder.AddDecisionTaskFailedEvent(
		newDecisionScheduleEvent.GetEventId(),
		newDecisionStartedEvent.GetEventId(),
		workflow.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure,
		[]byte("some random decision failure details"),
		"some random decision failure identity",
		"", "", "", 0,
	))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(1, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestShouldBufferEvent() {
	// workflow status events will be assign event ID immediately
	workflowEvents := map[workflow.EventType]bool{
		workflow.EventTypeWorkflowExecutionStarted:        true,
		workflow.EventTypeWorkflowExecutionCompleted:      true,
		workflow.EventTypeWorkflowExecutionFailed:         true,
		workflow.EventTypeWorkflowExecutionTimedOut:       true,
		workflow.EventTypeWorkflowExecutionTerminated:     true,
		workflow.EventTypeWorkflowExecutionContinuedAsNew: true,
		workflow.EventTypeWorkflowExecutionCanceled:       true,
	}

	// decision events will be assign event ID immediately
	decisionTaskEvents := map[workflow.EventType]bool{
		workflow.EventTypeDecisionTaskScheduled: true,
		workflow.EventTypeDecisionTaskStarted:   true,
		workflow.EventTypeDecisionTaskCompleted: true,
		workflow.EventTypeDecisionTaskFailed:    true,
		workflow.EventTypeDecisionTaskTimedOut:  true,
	}

	// events corresponding to decisions from client will be assign event ID immediately
	decisionEvents := map[workflow.EventType]bool{
		workflow.EventTypeWorkflowExecutionCompleted:                      true,
		workflow.EventTypeWorkflowExecutionFailed:                         true,
		workflow.EventTypeWorkflowExecutionCanceled:                       true,
		workflow.EventTypeWorkflowExecutionContinuedAsNew:                 true,
		workflow.EventTypeActivityTaskScheduled:                           true,
		workflow.EventTypeActivityTaskCancelRequested:                     true,
		workflow.EventTypeTimerStarted:                                    true,
		workflow.EventTypeTimerCanceled:                                   true,
		workflow.EventTypeCancelTimerFailed:                               true,
		workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated: true,
		workflow.EventTypeMarkerRecorded:                                  true,
		workflow.EventTypeStartChildWorkflowExecutionInitiated:            true,
		workflow.EventTypeSignalExternalWorkflowExecutionInitiated:        true,
		workflow.EventTypeUpsertWorkflowSearchAttributes:                  true,
	}

	// other events will not be assign event ID immediately
	otherEvents := map[workflow.EventType]bool{}
OtherEventsLoop:
	for _, eventType := range workflow.EventType_Values() {
		if _, ok := workflowEvents[eventType]; ok {
			continue OtherEventsLoop
		}
		if _, ok := decisionTaskEvents[eventType]; ok {
			continue OtherEventsLoop
		}
		if _, ok := decisionEvents[eventType]; ok {
			continue OtherEventsLoop
		}
		otherEvents[eventType] = true
	}

	// test workflowEvents, decisionTaskEvents, decisionEvents will return true
	for eventType := range workflowEvents {
		s.False(s.msBuilder.shouldBufferEvent(eventType))
	}
	for eventType := range decisionTaskEvents {
		s.False(s.msBuilder.shouldBufferEvent(eventType))
	}
	for eventType := range decisionEvents {
		s.False(s.msBuilder.shouldBufferEvent(eventType))
	}
	// other events will return false
	for eventType := range otherEvents {
		s.True(s.msBuilder.shouldBufferEvent(eventType))
	}

	// +1 is because DecisionTypeCancelTimer will be mapped
	// to either workflow.EventTypeTimerCanceled, or workflow.EventTypeCancelTimerFailed.
	s.Equal(len(workflow.DecisionType_Values())+1, len(decisionEvents),
		"This assertaion will be broken a new decision is added and no corresponding logic added to shouldBufferEvent()")
}

func (s *mutableStateSuite) TestReorderEvents() {
	domainID := testDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	activityID := "activity_id"
	activityResult := []byte("activity_result")

	info := &persistence.WorkflowExecutionInfo{
		DomainID:             domainID,
		WorkflowID:           we.GetWorkflowId(),
		RunID:                we.GetRunId(),
		TaskList:             tl,
		WorkflowTypeName:     "wType",
		WorkflowTimeout:      200,
		DecisionTimeoutValue: 100,
		State:                persistence.WorkflowStateRunning,
		CloseStatus:          persistence.WorkflowCloseStatusNone,
		NextEventID:          int64(8),
		LastProcessedEvent:   int64(3),
		LastUpdatedTimestamp: time.Now(),
		DecisionVersion:      common.EmptyVersion,
		DecisionScheduleID:   common.EmptyEventID,
		DecisionStartedID:    common.EmptyEventID,
		DecisionTimeout:      100,
	}

	activityInfos := map[int64]*persistence.ActivityInfo{
		5: &persistence.ActivityInfo{
			Version:                int64(1),
			ScheduleID:             int64(5),
			ScheduledTime:          time.Now(),
			StartedID:              common.EmptyEventID,
			StartedTime:            time.Now(),
			ActivityID:             activityID,
			ScheduleToStartTimeout: 100,
			ScheduleToCloseTimeout: 200,
			StartToCloseTimeout:    300,
			HeartbeatTimeout:       50,
		},
	}

	bufferedEvents := []*workflow.HistoryEvent{
		&workflow.HistoryEvent{
			EventId:   common.Int64Ptr(common.BufferedEventID),
			EventType: workflow.EventTypeActivityTaskCompleted.Ptr(),
			Version:   common.Int64Ptr(1),
			ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
				Result:           []byte(activityResult),
				ScheduledEventId: common.Int64Ptr(5),
				StartedEventId:   common.Int64Ptr(common.BufferedEventID),
			},
		},

		&workflow.HistoryEvent{
			EventId:   common.Int64Ptr(common.BufferedEventID),
			EventType: workflow.EventTypeActivityTaskStarted.Ptr(),
			Version:   common.Int64Ptr(1),
			ActivityTaskStartedEventAttributes: &workflow.ActivityTaskStartedEventAttributes{
				ScheduledEventId: common.Int64Ptr(5),
			},
		},
	}

	replicationState := &persistence.ReplicationState{
		StartVersion:        int64(1),
		CurrentVersion:      int64(1),
		LastWriteVersion:    common.EmptyVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastReplicationInfo: make(map[string]*persistence.ReplicationInfo),
	}

	dbState := &persistence.WorkflowMutableState{
		ExecutionInfo:    info,
		ActivityInfos:    activityInfos,
		BufferedEvents:   bufferedEvents,
		ReplicationState: replicationState,
	}

	s.msBuilder.Load(dbState)
	s.Equal(workflow.EventTypeActivityTaskCompleted, s.msBuilder.bufferedEvents[0].GetEventType())
	s.Equal(workflow.EventTypeActivityTaskStarted, s.msBuilder.bufferedEvents[1].GetEventType())

	err := s.msBuilder.FlushBufferedEvents()
	s.Nil(err)
	s.Equal(workflow.EventTypeActivityTaskStarted, s.msBuilder.hBuilder.history[0].GetEventType())
	s.Equal(int64(8), s.msBuilder.hBuilder.history[0].GetEventId())
	s.Equal(int64(5), s.msBuilder.hBuilder.history[0].ActivityTaskStartedEventAttributes.GetScheduledEventId())
	s.Equal(workflow.EventTypeActivityTaskCompleted, s.msBuilder.hBuilder.history[1].GetEventType())
	s.Equal(int64(9), s.msBuilder.hBuilder.history[1].GetEventId())
	s.Equal(int64(8), s.msBuilder.hBuilder.history[1].ActivityTaskCompletedEventAttributes.GetStartedEventId())
	s.Equal(int64(5), s.msBuilder.hBuilder.history[1].ActivityTaskCompletedEventAttributes.GetScheduledEventId())
}

func (s *mutableStateSuite) TestTrimEvents() {
	var input []*workflow.HistoryEvent
	output := s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*workflow.HistoryEvent{}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*workflow.HistoryEvent{
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeActivityTaskCanceled.Ptr(),
		},
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeWorkflowExecutionSignaled.Ptr(),
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*workflow.HistoryEvent{
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeActivityTaskCanceled.Ptr(),
		},
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeWorkflowExecutionCompleted.Ptr(),
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*workflow.HistoryEvent{
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeWorkflowExecutionCompleted.Ptr(),
		},
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeActivityTaskCanceled.Ptr(),
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal([]*workflow.HistoryEvent{
		&workflow.HistoryEvent{
			EventType: workflow.EventTypeWorkflowExecutionCompleted.Ptr(),
		},
	}, output)
}

func (s *mutableStateSuite) TestMergeMapOfByteArray() {
	var currentMap map[string][]byte
	var newMap map[string][]byte
	resultMap := mergeMapOfByteArray(currentMap, newMap)
	s.Equal(make(map[string][]byte), resultMap)

	newMap = map[string][]byte{"key": []byte("val")}
	resultMap = mergeMapOfByteArray(currentMap, newMap)
	s.Equal(newMap, resultMap)

	currentMap = map[string][]byte{"number": []byte("1")}
	resultMap = mergeMapOfByteArray(currentMap, newMap)
	s.Equal(2, len(resultMap))
}

func (s *mutableStateSuite) prepareTransientDecisionCompletionFirstBatchReplicated(version int64, runID string) (*shared.HistoryEvent, *shared.HistoryEvent) {
	domainID := testDomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(runID),
	}

	now := time.Now()
	workflowType := "some random workflow type"
	tasklist := "some random tasklist"
	workflowTimeoutSecond := int32(222)
	decisionTimeoutSecond := int32(11)
	decisionAttempt := int64(0)

	eventID := int64(1)
	workflowStartEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &shared.TaskList{Name: common.StringPtr(tasklist)},
			Input:                               nil,
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(workflowTimeoutSecond),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(decisionTimeoutSecond),
		},
	}
	eventID++

	decisionScheduleEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
			TaskList:                   &shared.TaskList{Name: common.StringPtr(tasklist)},
			StartToCloseTimeoutSeconds: common.Int32Ptr(decisionTimeoutSecond),
			Attempt:                    common.Int64Ptr(decisionAttempt),
		},
	}
	eventID++

	decisionStartedEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeDecisionTaskStarted.Ptr(),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{
			ScheduledEventId: common.Int64Ptr(decisionScheduleEvent.GetEventId()),
			RequestId:        common.StringPtr(uuid.New()),
		},
	}
	eventID++

	_ = &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeDecisionTaskFailed.Ptr(),
		DecisionTaskFailedEventAttributes: &shared.DecisionTaskFailedEventAttributes{
			ScheduledEventId: common.Int64Ptr(decisionScheduleEvent.GetEventId()),
			StartedEventId:   common.Int64Ptr(decisionStartedEvent.GetEventId()),
		},
	}
	eventID++

	s.mockEventsCache.On("putEvent", domainID, execution.GetWorkflowId(), execution.GetRunId(),
		workflowStartEvent.GetEventId(), workflowStartEvent).Return(nil).Once()
	err := s.msBuilder.ReplicateWorkflowExecutionStartedEvent(
		nil,
		execution,
		uuid.New(),
		workflowStartEvent,
	)
	s.Nil(err)

	// setup transient decision
	di, err := s.msBuilder.ReplicateDecisionTaskScheduledEvent(
		decisionScheduleEvent.GetVersion(),
		decisionScheduleEvent.GetEventId(),
		decisionScheduleEvent.DecisionTaskScheduledEventAttributes.TaskList.GetName(),
		decisionScheduleEvent.DecisionTaskScheduledEventAttributes.GetStartToCloseTimeoutSeconds(),
		decisionScheduleEvent.DecisionTaskScheduledEventAttributes.GetAttempt(),
		0,
		0,
	)
	s.Nil(err)
	s.NotNil(di)

	di, err = s.msBuilder.ReplicateDecisionTaskStartedEvent(nil,
		decisionStartedEvent.GetVersion(),
		decisionScheduleEvent.GetEventId(),
		decisionStartedEvent.GetEventId(),
		decisionStartedEvent.DecisionTaskStartedEventAttributes.GetRequestId(),
		decisionStartedEvent.GetTimestamp(),
	)
	s.Nil(err)
	s.NotNil(di)

	err = s.msBuilder.ReplicateDecisionTaskFailedEvent()
	s.Nil(err)

	decisionAttempt = int64(123)
	newDecisionScheduleEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeDecisionTaskScheduled.Ptr(),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{
			TaskList:                   &shared.TaskList{Name: common.StringPtr(tasklist)},
			StartToCloseTimeoutSeconds: common.Int32Ptr(decisionTimeoutSecond),
			Attempt:                    common.Int64Ptr(decisionAttempt),
		},
	}
	eventID++

	newDecisionStartedEvent := &shared.HistoryEvent{
		Version:   common.Int64Ptr(version),
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(now.UnixNano()),
		EventType: shared.EventTypeDecisionTaskStarted.Ptr(),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{
			ScheduledEventId: common.Int64Ptr(decisionScheduleEvent.GetEventId()),
			RequestId:        common.StringPtr(uuid.New()),
		},
	}
	eventID++

	di, err = s.msBuilder.ReplicateDecisionTaskScheduledEvent(
		newDecisionScheduleEvent.GetVersion(),
		newDecisionScheduleEvent.GetEventId(),
		newDecisionScheduleEvent.DecisionTaskScheduledEventAttributes.TaskList.GetName(),
		newDecisionScheduleEvent.DecisionTaskScheduledEventAttributes.GetStartToCloseTimeoutSeconds(),
		newDecisionScheduleEvent.DecisionTaskScheduledEventAttributes.GetAttempt(),
		0,
		0,
	)
	s.Nil(err)
	s.NotNil(di)

	di, err = s.msBuilder.ReplicateDecisionTaskStartedEvent(nil,
		newDecisionStartedEvent.GetVersion(),
		newDecisionScheduleEvent.GetEventId(),
		newDecisionStartedEvent.GetEventId(),
		newDecisionStartedEvent.DecisionTaskStartedEventAttributes.GetRequestId(),
		newDecisionStartedEvent.GetTimestamp(),
	)
	s.Nil(err)
	s.NotNil(di)

	return newDecisionScheduleEvent, newDecisionStartedEvent
}
