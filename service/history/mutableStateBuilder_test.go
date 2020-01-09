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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	mutableStateSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockShard       *shardContextTest
		mockEventsCache *MockeventsCache

		msBuilder *mutableStateBuilder
		logger    log.Logger
		testScope tally.TestScope
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
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockEventsCache = NewMockeventsCache(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          0,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		NewDynamicConfigForTest(),
	)
	// set the checksum probabilities to 100% for exercising during test
	s.mockShard.config.MutableStateChecksumGenProbability = func(domain string) int { return 100 }
	s.mockShard.config.MutableStateChecksumVerifyProbability = func(domain string) int { return 100 }
	s.mockShard.eventsCache = s.mockEventsCache

	s.testScope = s.mockShard.resource.MetricsScope.(tally.TestScope)
	s.logger = s.mockShard.GetLogger()

	s.msBuilder = newMutableStateBuilder(s.mockShard, s.mockEventsCache, s.logger, testLocalDomainEntry)
}

func (s *mutableStateSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
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
	err := s.msBuilder.ReplicateDecisionTaskCompletedEvent(newDecisionCompletedEvent)
	s.NoError(err)
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
		"", "", "", "", 0,
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
		DomainID:                    domainID,
		WorkflowID:                  we.GetWorkflowId(),
		RunID:                       we.GetRunId(),
		TaskList:                    tl,
		WorkflowTypeName:            "wType",
		WorkflowTimeout:             200,
		DecisionStartToCloseTimeout: 100,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
		NextEventID:                 int64(8),
		LastProcessedEvent:          int64(3),
		LastUpdatedTimestamp:        time.Now(),
		DecisionVersion:             common.EmptyVersion,
		DecisionScheduleID:          common.EmptyEventID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionTimeout:             100,
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

func (s *mutableStateSuite) TestChecksum() {
	testCases := []struct {
		name                 string
		enableBufferedEvents bool
		closeTxFunc          func(ms *mutableStateBuilder) (checksum.Checksum, error)
	}{
		{
			name: "closeTransactionAsSnapshot",
			closeTxFunc: func(ms *mutableStateBuilder) (checksum.Checksum, error) {
				snapshot, _, err := ms.CloseTransactionAsSnapshot(time.Now(), transactionPolicyPassive)
				if err != nil {
					return checksum.Checksum{}, err
				}
				return snapshot.Checksum, err
			},
		},
		{
			name:                 "closeTransactionAsMutation",
			enableBufferedEvents: true,
			closeTxFunc: func(ms *mutableStateBuilder) (checksum.Checksum, error) {
				mutation, _, err := ms.CloseTransactionAsMutation(time.Now(), transactionPolicyPassive)
				if err != nil {
					return checksum.Checksum{}, err
				}
				return mutation.Checksum, err
			},
		},
	}

	loadErrorsFunc := func() int64 {
		counter := s.testScope.Snapshot().Counters()["test.mutable_state_checksum_mismatch+operation=WorkflowContext"]
		if counter != nil {
			return counter.Value()
		}
		return 0
	}

	var loadErrors int64

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			if !tc.enableBufferedEvents {
				dbState.BufferedEvents = nil
			}

			// create mutable state and verify checksum is generated on close
			loadErrors = loadErrorsFunc()
			s.msBuilder.Load(dbState)
			s.Equal(loadErrors, loadErrorsFunc()) // no errors expected
			s.EqualValues(dbState.Checksum, s.msBuilder.checksum)
			s.msBuilder.domainEntry = s.newDomainCacheEntry()
			csum, err := tc.closeTxFunc(s.msBuilder)
			s.Nil(err)
			s.NotNil(csum.Value)
			s.Equal(checksum.FlavorIEEECRC32OverThriftBinary, csum.Flavor)
			s.Equal(mutableStateChecksumPayloadV1, csum.Version)
			s.EqualValues(csum, s.msBuilder.checksum)

			// verify checksum is verified on Load
			dbState.Checksum = csum
			s.msBuilder.Load(dbState)
			s.Equal(loadErrors, loadErrorsFunc())

			// generate checksum again and verify its the same
			csum, err = tc.closeTxFunc(s.msBuilder)
			s.Nil(err)
			s.NotNil(csum.Value)
			s.Equal(dbState.Checksum.Value, csum.Value)

			// modify checksum and verify Load fails
			dbState.Checksum.Value[0]++
			s.msBuilder.Load(dbState)
			s.Equal(loadErrors+1, loadErrorsFunc())
			s.EqualValues(dbState.Checksum, s.msBuilder.checksum)

			// test checksum is invalidated
			loadErrors = loadErrorsFunc()
			s.mockShard.config.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
				return float64((s.msBuilder.executionInfo.LastUpdatedTimestamp.UnixNano() / int64(time.Second)) + 1)
			}
			s.msBuilder.Load(dbState)
			s.Equal(loadErrors, loadErrorsFunc())
			s.EqualValues(checksum.Checksum{}, s.msBuilder.checksum)

			// revert the config value for the next test case
			s.mockShard.config.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
				return float64(0)
			}
		})
	}
}

func (s *mutableStateSuite) TestChecksumProbabilities() {
	for _, prob := range []int{0, 100} {
		s.mockShard.config.MutableStateChecksumGenProbability = func(domain string) int { return prob }
		s.mockShard.config.MutableStateChecksumVerifyProbability = func(domain string) int { return prob }
		for i := 0; i < 100; i++ {
			shouldGenerate := s.msBuilder.shouldGenerateChecksum()
			shouldVerify := s.msBuilder.shouldVerifyChecksum()
			s.Equal(prob == 100, shouldGenerate)
			s.Equal(prob == 100, shouldVerify)
		}
	}
}

func (s *mutableStateSuite) TestChecksumShouldInvalidate() {
	s.mockShard.config.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 { return 0 }
	s.False(s.msBuilder.shouldInvalidateCheckum())
	s.msBuilder.executionInfo.LastUpdatedTimestamp = time.Now()
	s.mockShard.config.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
		return float64((s.msBuilder.executionInfo.LastUpdatedTimestamp.UnixNano() / int64(time.Second)) + 1)
	}
	s.True(s.msBuilder.shouldInvalidateCheckum())
	s.mockShard.config.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
		return float64((s.msBuilder.executionInfo.LastUpdatedTimestamp.UnixNano() / int64(time.Second)) - 1)
	}
	s.False(s.msBuilder.shouldInvalidateCheckum())
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

func (s *mutableStateSuite) TestEventReapplied() {
	runID := uuid.New()
	eventID := int64(1)
	version := int64(2)
	dedupResource := definition.NewEventReappliedID(runID, eventID, version)
	isReapplied := s.msBuilder.IsResourceDuplicated(dedupResource)
	s.False(isReapplied)
	s.msBuilder.UpdateDuplicatedResource(dedupResource)
	isReapplied = s.msBuilder.IsResourceDuplicated(dedupResource)
	s.True(isReapplied)
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

	s.mockEventsCache.EXPECT().putEvent(
		domainID, execution.GetWorkflowId(), execution.GetRunId(),
		workflowStartEvent.GetEventId(), workflowStartEvent,
	).Times(1)
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
	eventID++ //nolint:ineffassign

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

func (s *mutableStateSuite) newDomainCacheEntry() *cache.DomainCacheEntry {
	return cache.NewDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "mutableStateTest"},
		&persistence.DomainConfig{},
		true,
		&persistence.DomainReplicationConfig{},
		1,
		nil,
	)
}

func (s *mutableStateSuite) buildWorkflowMutableState() *persistence.WorkflowMutableState {
	domainID := testDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(testRunID),
	}
	tl := "testTaskList"
	failoverVersion := int64(300)

	info := &persistence.WorkflowExecutionInfo{
		DomainID:                    domainID,
		WorkflowID:                  we.GetWorkflowId(),
		RunID:                       we.GetRunId(),
		TaskList:                    tl,
		WorkflowTypeName:            "wType",
		WorkflowTimeout:             200,
		DecisionStartToCloseTimeout: 100,
		State:                       persistence.WorkflowStateRunning,
		CloseStatus:                 persistence.WorkflowCloseStatusNone,
		NextEventID:                 int64(101),
		LastProcessedEvent:          int64(99),
		LastUpdatedTimestamp:        time.Now(),
		DecisionVersion:             failoverVersion,
		DecisionScheduleID:          common.EmptyEventID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionTimeout:             100,
	}

	activityInfos := map[int64]*persistence.ActivityInfo{
		5: &persistence.ActivityInfo{
			Version:                failoverVersion,
			ScheduleID:             int64(90),
			ScheduledTime:          time.Now(),
			StartedID:              common.EmptyEventID,
			StartedTime:            time.Now(),
			ActivityID:             "activityID_5",
			ScheduleToStartTimeout: 100,
			ScheduleToCloseTimeout: 200,
			StartToCloseTimeout:    300,
			HeartbeatTimeout:       50,
		},
	}

	timerInfos := map[string]*persistence.TimerInfo{
		"25": &persistence.TimerInfo{
			Version:    failoverVersion,
			TimerID:    "25",
			StartedID:  85,
			ExpiryTime: time.Now().Add(time.Hour),
		},
	}

	childInfos := map[int64]*persistence.ChildExecutionInfo{
		80: &persistence.ChildExecutionInfo{
			Version:               failoverVersion,
			InitiatedID:           80,
			InitiatedEventBatchID: 20,
			InitiatedEvent:        &shared.HistoryEvent{},
			StartedID:             common.EmptyEventID,
			CreateRequestID:       uuid.New(),
			DomainName:            testDomainID,
			WorkflowTypeName:      "code.uber.internal/test/foobar",
		},
	}

	signalInfos := map[int64]*persistence.SignalInfo{
		75: &persistence.SignalInfo{
			Version:               failoverVersion,
			InitiatedID:           75,
			InitiatedEventBatchID: 17,
			SignalRequestID:       uuid.New(),
			SignalName:            "test-signal-75",
			Input:                 []byte("signal-input-75"),
		},
	}

	signalRequestIDs := map[string]struct{}{
		uuid.New(): struct{}{},
	}

	bufferedEvents := []*workflow.HistoryEvent{
		&workflow.HistoryEvent{
			EventId:   common.Int64Ptr(common.BufferedEventID),
			EventType: workflow.EventTypeWorkflowExecutionSignaled.Ptr(),
			Version:   common.Int64Ptr(failoverVersion),
			WorkflowExecutionSignaledEventAttributes: &workflow.WorkflowExecutionSignaledEventAttributes{
				SignalName: common.StringPtr("test-signal-buffered"),
				Input:      []byte("test-signal-buffered-input"),
			},
		},
	}

	replicationState := &persistence.ReplicationState{
		StartVersion:        failoverVersion,
		CurrentVersion:      failoverVersion,
		LastWriteVersion:    common.EmptyVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastReplicationInfo: make(map[string]*persistence.ReplicationInfo),
	}

	versionHistories := &persistence.VersionHistories{
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte("token#1"),
				Items: []*persistence.VersionHistoryItem{
					{EventID: 1, Version: 300},
				},
			},
		},
	}

	return &persistence.WorkflowMutableState{
		ExecutionInfo:       info,
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		ChildExecutionInfos: childInfos,
		SignalInfos:         signalInfos,
		SignalRequestedIDs:  signalRequestIDs,
		BufferedEvents:      bufferedEvents,
		ReplicationState:    replicationState,
		VersionHistories:    versionHistories,
	}
}
