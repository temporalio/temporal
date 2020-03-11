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

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/checksum"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
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
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          0,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
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

	newDecisionCompletedEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   newDecisionStartedEvent.GetEventId() + 1,
		Timestamp: time.Now().UnixNano(),
		EventType: enums.EventTypeDecisionTaskCompleted,
		Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: newDecisionScheduleEvent.GetEventId(),
			StartedEventId:   newDecisionStartedEvent.GetEventId(),
			Identity:         "some random identity",
		}},
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
		enums.DecisionTaskFailedCauseWorkflowWorkerUnhandledFailure,
		[]byte("some random decision failure details"),
		"some random decision failure identity",
		"", "", "", "", 0,
	))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(1, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestShouldBufferEvent() {
	// workflow status events will be assign event ID immediately
	workflowEvents := map[enums.EventType]bool{
		enums.EventTypeWorkflowExecutionStarted:        true,
		enums.EventTypeWorkflowExecutionCompleted:      true,
		enums.EventTypeWorkflowExecutionFailed:         true,
		enums.EventTypeWorkflowExecutionTimedOut:       true,
		enums.EventTypeWorkflowExecutionTerminated:     true,
		enums.EventTypeWorkflowExecutionContinuedAsNew: true,
		enums.EventTypeWorkflowExecutionCanceled:       true,
	}

	// decision events will be assign event ID immediately
	decisionTaskEvents := map[enums.EventType]bool{
		enums.EventTypeDecisionTaskScheduled: true,
		enums.EventTypeDecisionTaskStarted:   true,
		enums.EventTypeDecisionTaskCompleted: true,
		enums.EventTypeDecisionTaskFailed:    true,
		enums.EventTypeDecisionTaskTimedOut:  true,
	}

	// events corresponding to decisions from client will be assign event ID immediately
	decisionEvents := map[enums.EventType]bool{
		enums.EventTypeWorkflowExecutionCompleted:                      true,
		enums.EventTypeWorkflowExecutionFailed:                         true,
		enums.EventTypeWorkflowExecutionCanceled:                       true,
		enums.EventTypeWorkflowExecutionContinuedAsNew:                 true,
		enums.EventTypeActivityTaskScheduled:                           true,
		enums.EventTypeActivityTaskCancelRequested:                     true,
		enums.EventTypeTimerStarted:                                    true,
		enums.EventTypeTimerCanceled:                                   true,
		enums.EventTypeCancelTimerFailed:                               true,
		enums.EventTypeRequestCancelExternalWorkflowExecutionInitiated: true,
		enums.EventTypeMarkerRecorded:                                  true,
		enums.EventTypeStartChildWorkflowExecutionInitiated:            true,
		enums.EventTypeSignalExternalWorkflowExecutionInitiated:        true,
		enums.EventTypeUpsertWorkflowSearchAttributes:                  true,
	}

	// other events will not be assign event ID immediately
	otherEvents := map[enums.EventType]bool{}
OtherEventsLoop:
	for _, eventType := range enums.EventType_value {
		if _, ok := workflowEvents[enums.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		if _, ok := decisionTaskEvents[enums.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		if _, ok := decisionEvents[enums.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		otherEvents[enums.EventType(eventType)] = true
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
	s.Equal(len(enums.DecisionType_value)+1, len(decisionEvents),
		"This assertion will be broken a new decision is added and no corresponding logic added to shouldBufferEvent()")
}

func (s *mutableStateSuite) TestReorderEvents() {
	domainID := testDomainID
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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
		CloseStatus:                 persistence.WorkflowCloseStatusRunning,
		NextEventID:                 int64(8),
		LastProcessedEvent:          int64(3),
		LastUpdatedTimestamp:        time.Now(),
		DecisionVersion:             common.EmptyVersion,
		DecisionScheduleID:          common.EmptyEventID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionTimeout:             100,
	}

	activityInfos := map[int64]*persistence.ActivityInfo{
		5: {
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

	bufferedEvents := []*commonproto.HistoryEvent{
		{
			EventId:   common.BufferedEventID,
			EventType: enums.EventTypeActivityTaskCompleted,
			Version:   1,
			Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
				Result:           activityResult,
				ScheduledEventId: 5,
				StartedEventId:   common.BufferedEventID,
			}},
		},

		{
			EventId:   common.BufferedEventID,
			EventType: enums.EventTypeActivityTaskStarted,
			Version:   1,
			Attributes: &commonproto.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &commonproto.ActivityTaskStartedEventAttributes{
				ScheduledEventId: 5,
			}},
		},
	}

	replicationState := &persistence.ReplicationState{
		StartVersion:        int64(1),
		CurrentVersion:      int64(1),
		LastWriteVersion:    common.EmptyVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastReplicationInfo: make(map[string]*replication.ReplicationInfo),
	}

	dbState := &persistence.WorkflowMutableState{
		ExecutionInfo:    info,
		ActivityInfos:    activityInfos,
		BufferedEvents:   bufferedEvents,
		ReplicationState: replicationState,
	}

	s.msBuilder.Load(dbState)
	s.Equal(enums.EventTypeActivityTaskCompleted, s.msBuilder.bufferedEvents[0].GetEventType())
	s.Equal(enums.EventTypeActivityTaskStarted, s.msBuilder.bufferedEvents[1].GetEventType())

	err := s.msBuilder.FlushBufferedEvents()
	s.Nil(err)
	s.Equal(enums.EventTypeActivityTaskStarted, s.msBuilder.hBuilder.history[0].GetEventType())
	s.Equal(int64(8), s.msBuilder.hBuilder.history[0].GetEventId())
	s.Equal(int64(5), s.msBuilder.hBuilder.history[0].GetActivityTaskStartedEventAttributes().GetScheduledEventId())
	s.Equal(enums.EventTypeActivityTaskCompleted, s.msBuilder.hBuilder.history[1].GetEventType())
	s.Equal(int64(9), s.msBuilder.hBuilder.history[1].GetEventId())
	s.Equal(int64(8), s.msBuilder.hBuilder.history[1].GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(int64(5), s.msBuilder.hBuilder.history[1].GetActivityTaskCompletedEventAttributes().GetScheduledEventId())
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
	var input []*commonproto.HistoryEvent
	output := s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*commonproto.HistoryEvent{}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*commonproto.HistoryEvent{
		{
			EventType: enums.EventTypeActivityTaskCanceled,
		},
		{
			EventType: enums.EventTypeWorkflowExecutionSignaled,
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*commonproto.HistoryEvent{
		{
			EventType: enums.EventTypeActivityTaskCanceled,
		},
		{
			EventType: enums.EventTypeWorkflowExecutionCompleted,
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*commonproto.HistoryEvent{
		{
			EventType: enums.EventTypeWorkflowExecutionCompleted,
		},
		{
			EventType: enums.EventTypeActivityTaskCanceled,
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal([]*commonproto.HistoryEvent{
		{
			EventType: enums.EventTypeWorkflowExecutionCompleted,
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

func (s *mutableStateSuite) prepareTransientDecisionCompletionFirstBatchReplicated(version int64, runID string) (*commonproto.HistoryEvent, *commonproto.HistoryEvent) {
	domainID := testDomainID
	execution := commonproto.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      runID,
	}

	now := time.Now()
	workflowType := "some random workflow type"
	tasklist := "some random tasklist"
	workflowTimeoutSecond := int32(222)
	decisionTimeoutSecond := int32(11)
	decisionAttempt := int64(0)

	eventID := int64(1)
	workflowStartEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeWorkflowExecutionStarted,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &commonproto.WorkflowType{Name: workflowType},
			TaskList:                            &commonproto.TaskList{Name: tasklist},
			Input:                               nil,
			ExecutionStartToCloseTimeoutSeconds: workflowTimeoutSecond,
			TaskStartToCloseTimeoutSeconds:      decisionTimeoutSecond,
		}},
	}
	eventID++

	decisionScheduleEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeDecisionTaskScheduled,
		Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
			TaskList:                   &commonproto.TaskList{Name: tasklist},
			StartToCloseTimeoutSeconds: decisionTimeoutSecond,
			Attempt:                    decisionAttempt,
		}},
	}
	eventID++

	decisionStartedEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeDecisionTaskStarted,
		Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
			ScheduledEventId: decisionScheduleEvent.GetEventId(),
			RequestId:        uuid.New(),
		}},
	}
	eventID++

	_ = &commonproto.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeDecisionTaskFailed,
		Attributes: &commonproto.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &commonproto.DecisionTaskFailedEventAttributes{
			ScheduledEventId: decisionScheduleEvent.GetEventId(),
			StartedEventId:   decisionStartedEvent.GetEventId(),
		}},
	}
	eventID++

	s.mockEventsCache.EXPECT().putEvent(
		domainID, execution.GetWorkflowId(), execution.GetRunId(),
		workflowStartEvent.GetEventId(), workflowStartEvent,
	).Times(1)
	err := s.msBuilder.ReplicateWorkflowExecutionStartedEvent(
		"",
		execution,
		uuid.New(),
		workflowStartEvent,
	)
	s.Nil(err)

	// setup transient decision
	di, err := s.msBuilder.ReplicateDecisionTaskScheduledEvent(
		decisionScheduleEvent.GetVersion(),
		decisionScheduleEvent.GetEventId(),
		decisionScheduleEvent.GetDecisionTaskScheduledEventAttributes().TaskList.GetName(),
		decisionScheduleEvent.GetDecisionTaskScheduledEventAttributes().GetStartToCloseTimeoutSeconds(),
		decisionScheduleEvent.GetDecisionTaskScheduledEventAttributes().GetAttempt(),
		0,
		0,
	)
	s.Nil(err)
	s.NotNil(di)

	di, err = s.msBuilder.ReplicateDecisionTaskStartedEvent(nil,
		decisionStartedEvent.GetVersion(),
		decisionScheduleEvent.GetEventId(),
		decisionStartedEvent.GetEventId(),
		decisionStartedEvent.GetDecisionTaskStartedEventAttributes().GetRequestId(),
		decisionStartedEvent.GetTimestamp(),
	)
	s.Nil(err)
	s.NotNil(di)

	err = s.msBuilder.ReplicateDecisionTaskFailedEvent()
	s.Nil(err)

	decisionAttempt = int64(123)
	newDecisionScheduleEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeDecisionTaskScheduled,
		Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{
			TaskList:                   &commonproto.TaskList{Name: tasklist},
			StartToCloseTimeoutSeconds: decisionTimeoutSecond,
			Attempt:                    decisionAttempt,
		}},
	}
	eventID++

	newDecisionStartedEvent := &commonproto.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		Timestamp: now.UnixNano(),
		EventType: enums.EventTypeDecisionTaskStarted,
		Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{
			ScheduledEventId: decisionScheduleEvent.GetEventId(),
			RequestId:        uuid.New(),
		}},
	}
	eventID++ //nolint:ineffassign

	di, err = s.msBuilder.ReplicateDecisionTaskScheduledEvent(
		newDecisionScheduleEvent.GetVersion(),
		newDecisionScheduleEvent.GetEventId(),
		newDecisionScheduleEvent.GetDecisionTaskScheduledEventAttributes().TaskList.GetName(),
		newDecisionScheduleEvent.GetDecisionTaskScheduledEventAttributes().GetStartToCloseTimeoutSeconds(),
		newDecisionScheduleEvent.GetDecisionTaskScheduledEventAttributes().GetAttempt(),
		0,
		0,
	)
	s.Nil(err)
	s.NotNil(di)

	di, err = s.msBuilder.ReplicateDecisionTaskStartedEvent(nil,
		newDecisionStartedEvent.GetVersion(),
		newDecisionScheduleEvent.GetEventId(),
		newDecisionStartedEvent.GetEventId(),
		newDecisionStartedEvent.GetDecisionTaskStartedEventAttributes().GetRequestId(),
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
	we := commonproto.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
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
		CloseStatus:                 persistence.WorkflowCloseStatusRunning,
		NextEventID:                 int64(101),
		LastProcessedEvent:          int64(99),
		LastUpdatedTimestamp:        time.Now(),
		DecisionVersion:             failoverVersion,
		DecisionScheduleID:          common.EmptyEventID,
		DecisionStartedID:           common.EmptyEventID,
		DecisionTimeout:             100,
	}

	activityInfos := map[int64]*persistence.ActivityInfo{
		5: {
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

	expiryTime := types.TimestampNow()
	expiryTime.Seconds += int64(time.Hour.Seconds())
	timerInfos := map[string]*persistenceblobs.TimerInfo{
		"25": {
			Version:    failoverVersion,
			TimerID:    "25",
			StartedID:  85,
			ExpiryTime: expiryTime,
		},
	}

	childInfos := map[int64]*persistence.ChildExecutionInfo{
		80: {
			Version:               failoverVersion,
			InitiatedID:           80,
			InitiatedEventBatchID: 20,
			InitiatedEvent:        &commonproto.HistoryEvent{},
			StartedID:             common.EmptyEventID,
			CreateRequestID:       uuid.New(),
			DomainName:            testDomainID,
			WorkflowTypeName:      "code.uber.internal/test/foobar",
		},
	}

	signalInfos := map[int64]*persistenceblobs.SignalInfo{
		75: {
			Version:               failoverVersion,
			InitiatedID:           75,
			InitiatedEventBatchID: 17,
			RequestID:             uuid.New(),
			Name:                  "test-signal-75",
			Input:                 []byte("signal-input-75"),
		},
	}

	signalRequestIDs := map[string]struct{}{
		uuid.New(): {},
	}

	bufferedEvents := []*commonproto.HistoryEvent{
		{
			EventId:   common.BufferedEventID,
			EventType: enums.EventTypeWorkflowExecutionSignaled,
			Version:   failoverVersion,
			Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
				SignalName: "test-signal-buffered",
				Input:      []byte("test-signal-buffered-input"),
			}},
		},
	}

	replicationState := &persistence.ReplicationState{
		StartVersion:        failoverVersion,
		CurrentVersion:      failoverVersion,
		LastWriteVersion:    common.EmptyVersion,
		LastWriteEventID:    common.EmptyEventID,
		LastReplicationInfo: make(map[string]*replication.ReplicationInfo),
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
