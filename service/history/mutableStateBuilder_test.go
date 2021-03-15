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

package history

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

type (
	mutableStateSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockConfig      *configs.Config
		mockShard       *shard.ContextTest
		mockEventsCache *events.MockCache

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
	s.mockEventsCache = events.NewMockCache(s.controller)

	s.mockConfig = NewDynamicConfigForTest()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		s.mockConfig,
	)
	// set the checksum probabilities to 100% for exercising during test
	s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }
	s.mockShard.EventsCache = s.mockEventsCache

	s.testScope = s.mockShard.Resource.MetricsScope.(tally.TestScope)
	s.logger = s.mockShard.GetLogger()

	s.msBuilder = newMutableStateBuilderWithVersionHistories(s.mockShard, s.mockEventsCache, s.logger, testLocalNamespaceEntry, time.Now().UTC())
}

func (s *mutableStateSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchReplicated_ReplicateWorkflowTaskCompleted() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)

	newWorkflowTaskCompletedEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   newWorkflowTaskStartedEvent.GetEventId() + 1,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: newWorkflowTaskScheduleEvent.GetEventId(),
			StartedEventId:   newWorkflowTaskStartedEvent.GetEventId(),
			Identity:         "some random identity",
		}},
	}
	err := s.msBuilder.ReplicateWorkflowTaskCompletedEvent(newWorkflowTaskCompletedEvent)
	s.NoError(err)
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchReplicated_FailoverWorkflowTaskTimeout() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)

	s.NotNil(s.msBuilder.AddWorkflowTaskTimedOutEvent(newWorkflowTaskScheduleEvent.GetEventId(), newWorkflowTaskStartedEvent.GetEventId()))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(1, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchReplicated_FailoverWorkflowTaskFailed() {
	version := int64(12)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)

	s.NotNil(s.msBuilder.AddWorkflowTaskFailedEvent(
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		"", "", "", 0,
	))
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(1, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestShouldBufferEvent() {
	// workflow status events will be assign event ID immediately
	workflowEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:          true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:        true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:           true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:        true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:       true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW: true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:         true,
	}

	// workflow task events will be assign event ID immediately
	workflowTaskEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED: true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:   true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED: true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED:    true,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT: true,
	}

	// events corresponding to commands from client will be assign event ID immediately
	commandEvents := map[enumspb.EventType]bool{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:                         true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:                            true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:                          true,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:                  true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:                              true,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:                       true,
		enumspb.EVENT_TYPE_TIMER_STARTED:                                        true,
		enumspb.EVENT_TYPE_TIMER_CANCELED:                                       true,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED: true,
		enumspb.EVENT_TYPE_MARKER_RECORDED:                                      true,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:             true,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED:         true,
		enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:                    true,
	}

	// other events will not be assign event ID immediately
	otherEvents := map[enumspb.EventType]bool{}
OtherEventsLoop:
	for _, eventType := range enumspb.EventType_value {
		if _, ok := workflowEvents[enumspb.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		if _, ok := workflowTaskEvents[enumspb.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		if _, ok := commandEvents[enumspb.EventType(eventType)]; ok {
			continue OtherEventsLoop
		}
		otherEvents[enumspb.EventType(eventType)] = true
	}

	// test workflowEvents, workflowTaskEvents, commandEvents will return true
	for eventType := range workflowEvents {
		s.False(s.msBuilder.shouldBufferEvent(eventType))
	}
	for eventType := range workflowTaskEvents {
		s.False(s.msBuilder.shouldBufferEvent(eventType))
	}
	for eventType := range commandEvents {
		s.False(s.msBuilder.shouldBufferEvent(eventType))
	}
	// other events will return false
	for eventType := range otherEvents {
		s.True(s.msBuilder.shouldBufferEvent(eventType))
	}

	commandTypes := enumspb.CommandType_name
	delete(commandTypes, 0) // Remove Unspecified.
	s.Equal(len(commandTypes), len(commandEvents),
		"This assertion will be broken a new command is added and no corresponding logic added to shouldBufferEvent()")
}

func (s *mutableStateSuite) TestReorderEvents() {
	namespaceID := testNamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskQueue"
	activityID := "activity_id"
	activityResult := payloads.EncodeString("activity_result")

	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                namespaceID,
		WorkflowId:                 we.GetWorkflowId(),
		TaskQueue:                  tl,
		WorkflowTypeName:           "wType",
		WorkflowRunTimeout:         timestamp.DurationFromSeconds(200),
		DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(100),
		LastProcessedEvent:         int64(3),
		LastUpdateTime:             timestamp.TimeNowPtrUtc(),
		WorkflowTaskVersion:        common.EmptyVersion,
		WorkflowTaskScheduleId:     common.EmptyEventID,
		WorkflowTaskStartedId:      common.EmptyEventID,
		WorkflowTaskTimeout:        timestamp.DurationFromSeconds(100),
		WorkflowTaskAttempt:        1,
	}

	state := &persistencespb.WorkflowExecutionState{
		RunId:  we.GetRunId(),
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	activityInfos := map[int64]*persistencespb.ActivityInfo{
		5: {
			Version:                int64(1),
			ScheduleId:             int64(5),
			ScheduledTime:          timestamp.TimePtr(time.Now().UTC()),
			StartedId:              common.EmptyEventID,
			StartedTime:            timestamp.TimePtr(time.Now().UTC()),
			ActivityId:             activityID,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			ScheduleToCloseTimeout: timestamp.DurationFromSeconds(200),
			StartToCloseTimeout:    timestamp.DurationFromSeconds(300),
			HeartbeatTimeout:       timestamp.DurationFromSeconds(50),
		},
	}

	bufferedEvents := []*historypb.HistoryEvent{
		{
			EventId:   common.BufferedEventID,
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Version:   1,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				Result:           activityResult,
				ScheduledEventId: 5,
				StartedEventId:   common.BufferedEventID,
			}},
		},

		{
			EventId:   common.BufferedEventID,
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Version:   1,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: 5,
			}},
		},
	}

	dbState := &persistencespb.WorkflowMutableState{
		ExecutionInfo:  info,
		ExecutionState: state,
		NextEventId:    int64(8),
		ActivityInfos:  activityInfos,
		BufferedEvents: bufferedEvents,
	}

	s.msBuilder.Load(dbState)
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, s.msBuilder.bufferedEvents[0].GetEventType())
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, s.msBuilder.bufferedEvents[1].GetEventType())

	err := s.msBuilder.FlushBufferedEvents()
	s.Nil(err)
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, s.msBuilder.hBuilder.history[0].GetEventType())
	s.Equal(int64(8), s.msBuilder.hBuilder.history[0].GetEventId())
	s.Equal(int64(5), s.msBuilder.hBuilder.history[0].GetActivityTaskStartedEventAttributes().GetScheduledEventId())
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, s.msBuilder.hBuilder.history[1].GetEventType())
	s.Equal(int64(9), s.msBuilder.hBuilder.history[1].GetEventId())
	s.Equal(int64(8), s.msBuilder.hBuilder.history[1].GetActivityTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(int64(5), s.msBuilder.hBuilder.history[1].GetActivityTaskCompletedEventAttributes().GetScheduledEventId())
}

func (s *mutableStateSuite) TestChecksum() {
	testCases := []struct {
		name                 string
		enableBufferedEvents bool
		closeTxFunc          func(ms *mutableStateBuilder) (*persistencespb.Checksum, error)
	}{
		{
			name: "closeTransactionAsSnapshot",
			closeTxFunc: func(ms *mutableStateBuilder) (*persistencespb.Checksum, error) {
				snapshot, _, err := ms.CloseTransactionAsSnapshot(time.Now().UTC(), transactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return snapshot.Checksum, err
			},
		},
		{
			name:                 "closeTransactionAsMutation",
			enableBufferedEvents: true,
			closeTxFunc: func(ms *mutableStateBuilder) (*persistencespb.Checksum, error) {
				mutation, _, err := ms.CloseTransactionAsMutation(time.Now().UTC(), transactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return mutation.Checksum, err
			},
		},
	}

	loadErrorsFunc := func() int64 {
		counter := s.testScope.Snapshot().Counters()["test.mutable_state_checksum_mismatch+namespace=all,operation=WorkflowContext"]
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
			s.msBuilder.namespaceEntry = s.newNamespaceCacheEntry()
			csum, err := tc.closeTxFunc(s.msBuilder)
			s.Nil(err)
			s.NotNil(csum.Value)
			s.Equal(enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY, csum.Flavor)
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
			s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
				return float64((s.msBuilder.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) + 1)
			}
			s.msBuilder.Load(dbState)
			s.Equal(loadErrors, loadErrorsFunc())
			s.Nil(s.msBuilder.checksum)

			// revert the config value for the next test case
			s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
				return float64(0)
			}
		})
	}
}

func (s *mutableStateSuite) TestChecksumProbabilities() {
	for _, prob := range []int{0, 100} {
		s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return prob }
		s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return prob }
		for i := 0; i < 100; i++ {
			shouldGenerate := s.msBuilder.shouldGenerateChecksum()
			shouldVerify := s.msBuilder.shouldVerifyChecksum()
			s.Equal(prob == 100, shouldGenerate)
			s.Equal(prob == 100, shouldVerify)
		}
	}
}

func (s *mutableStateSuite) TestChecksumShouldInvalidate() {
	s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 { return 0 }
	s.False(s.msBuilder.shouldInvalidateCheckum())
	s.msBuilder.executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
		return float64((s.msBuilder.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) + 1)
	}
	s.True(s.msBuilder.shouldInvalidateCheckum())
	s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
		return float64((s.msBuilder.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) - 1)
	}
	s.False(s.msBuilder.shouldInvalidateCheckum())
}

func (s *mutableStateSuite) TestTrimEvents() {
	var input []*historypb.HistoryEvent
	output := s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*historypb.HistoryEvent{}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal(input, output)

	input = []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		},
	}
	output = s.msBuilder.trimEventsAfterWorkflowClose(input)
	s.Equal([]*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}, output)
}

func (s *mutableStateSuite) TestMergeMapOfPayload() {
	var currentMap map[string]*commonpb.Payload
	var newMap map[string]*commonpb.Payload
	resultMap := mergeMapOfPayload(currentMap, newMap)
	s.Equal(make(map[string]*commonpb.Payload), resultMap)

	newMap = map[string]*commonpb.Payload{"key": payload.EncodeString("val")}
	resultMap = mergeMapOfPayload(currentMap, newMap)
	s.Equal(newMap, resultMap)

	currentMap = map[string]*commonpb.Payload{"number": payload.EncodeString("1")}
	resultMap = mergeMapOfPayload(currentMap, newMap)
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

func (s *mutableStateSuite) TestTransientWorkflowTaskSchedule_CurrentVersionChanged() {
	version := int64(2000)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)
	_, _ = s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)
	err := s.msBuilder.ReplicateWorkflowTaskFailedEvent()
	s.NoError(err)

	err = s.msBuilder.UpdateCurrentVersion(version+1, true)
	s.NoError(err)
	versionHistories := s.msBuilder.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, &historyspb.VersionHistoryItem{
		EventId: s.msBuilder.GetNextEventID() - 1,
		Version: version,
	})
	s.NoError(err)

	di, err := s.msBuilder.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc())
	s.NoError(err)
	s.NotNil(di)

	s.Equal(int32(1), s.msBuilder.GetExecutionInfo().WorkflowTaskAttempt)
	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(1, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) TestTransientWorkflowTaskStart_CurrentVersionChanged() {
	version := int64(2000)
	runID := uuid.New()
	s.msBuilder = newMutableStateBuilderWithVersionHistoriesForTest(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)
	_, _ = s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)
	err := s.msBuilder.ReplicateWorkflowTaskFailedEvent()
	s.NoError(err)

	versionHistories := s.msBuilder.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, &historyspb.VersionHistoryItem{
		EventId: s.msBuilder.GetNextEventID() - 1,
		Version: version,
	})
	s.NoError(err)

	di, err := s.msBuilder.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc())
	s.NoError(err)
	s.NotNil(di)

	err = s.msBuilder.UpdateCurrentVersion(version+1, true)
	s.NoError(err)

	_, _, err = s.msBuilder.AddWorkflowTaskStartedEvent(
		s.msBuilder.GetNextEventID(),
		uuid.New(),
		&workflowservice.PollWorkflowTaskQueueRequest{},
	)
	s.NoError(err)

	s.Equal(0, len(s.msBuilder.GetHistoryBuilder().transientHistory))
	s.Equal(2, len(s.msBuilder.GetHistoryBuilder().history))
}

func (s *mutableStateSuite) prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version int64, runID string) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	namespaceID := testNamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      runID,
	}

	now := time.Now().UTC()
	workflowType := "some random workflow type"
	taskqueue := "some random taskqueue"
	workflowTimeout := 222 * time.Second
	runTimeout := 111 * time.Second
	workflowTaskTimeout := 11 * time.Second
	workflowTaskAttempt := int32(1)

	eventID := int64(1)
	workflowStartEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue},
			Input:                    nil,
			WorkflowExecutionTimeout: &workflowTimeout,
			WorkflowRunTimeout:       &runTimeout,
			WorkflowTaskTimeout:      &workflowTaskTimeout,
		}},
	}
	eventID++

	workflowTaskScheduleEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: &workflowTaskTimeout,
			Attempt:             workflowTaskAttempt,
		}},
	}
	eventID++

	workflowTaskStartedEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			RequestId:        uuid.New(),
		}},
	}
	eventID++

	_ = &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			StartedEventId:   workflowTaskStartedEvent.GetEventId(),
		}},
	}
	eventID++

	s.mockEventsCache.EXPECT().PutEvent(
		namespaceID, execution.GetWorkflowId(), execution.GetRunId(),
		workflowStartEvent.GetEventId(), workflowStartEvent,
	)
	err := s.msBuilder.ReplicateWorkflowExecutionStartedEvent(
		"",
		execution,
		uuid.New(),
		workflowStartEvent,
	)
	s.Nil(err)

	// setup transient workflow task
	di, err := s.msBuilder.ReplicateWorkflowTaskScheduledEvent(
		workflowTaskScheduleEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		int32(timestamp.DurationValue(workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout()).Seconds()),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
	)
	s.Nil(err)
	s.NotNil(di)

	di, err = s.msBuilder.ReplicateWorkflowTaskStartedEvent(nil,
		workflowTaskStartedEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskStartedEvent.GetEventId(),
		workflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(workflowTaskStartedEvent.GetEventTime()),
	)
	s.Nil(err)
	s.NotNil(di)

	err = s.msBuilder.ReplicateWorkflowTaskFailedEvent()
	s.Nil(err)

	workflowTaskAttempt = int32(123)
	newWorkflowTaskScheduleEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue},
			StartToCloseTimeout: &workflowTaskTimeout,
			Attempt:             workflowTaskAttempt,
		}},
	}
	eventID++

	newWorkflowTaskStartedEvent := &historypb.HistoryEvent{
		Version:   version,
		EventId:   eventID,
		EventTime: &now,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			RequestId:        uuid.New(),
		}},
	}
	eventID++ // nolint:ineffassign

	di, err = s.msBuilder.ReplicateWorkflowTaskScheduledEvent(
		newWorkflowTaskScheduleEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		int32(timestamp.DurationValue(newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout()).Seconds()),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
	)
	s.Nil(err)
	s.NotNil(di)

	di, err = s.msBuilder.ReplicateWorkflowTaskStartedEvent(nil,
		newWorkflowTaskStartedEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(newWorkflowTaskStartedEvent.GetEventTime()),
	)
	s.Nil(err)
	s.NotNil(di)

	s.msBuilder.nextEventID = eventID

	return newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent
}

func (s *mutableStateSuite) newNamespaceCacheEntry() *cache.NamespaceCacheEntry {
	return cache.NewNamespaceCacheEntryForTest(
		&persistencespb.NamespaceInfo{Name: "mutableStateTest"},
		&persistencespb.NamespaceConfig{},
		true,
		&persistencespb.NamespaceReplicationConfig{},
		1,
		nil,
	)
}

func (s *mutableStateSuite) buildWorkflowMutableState() *persistencespb.WorkflowMutableState {
	namespaceID := testNamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      testRunID,
	}
	tl := "testTaskQueue"
	failoverVersion := int64(300)

	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                namespaceID,
		WorkflowId:                 we.GetWorkflowId(),
		TaskQueue:                  tl,
		WorkflowTypeName:           "wType",
		WorkflowRunTimeout:         timestamp.DurationFromSeconds(200),
		DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(100),
		LastProcessedEvent:         int64(99),
		LastUpdateTime:             timestamp.TimeNowPtrUtc(),
		WorkflowTaskVersion:        failoverVersion,
		WorkflowTaskScheduleId:     common.EmptyEventID,
		WorkflowTaskStartedId:      common.EmptyEventID,
		WorkflowTaskTimeout:        timestamp.DurationFromSeconds(100),
		WorkflowTaskAttempt:        1,
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 1, Version: 300},
					},
				},
			},
		},
	}

	state := &persistencespb.WorkflowExecutionState{
		RunId:  we.GetRunId(),
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	activityInfos := map[int64]*persistencespb.ActivityInfo{
		5: {
			Version:                failoverVersion,
			ScheduleId:             int64(90),
			ScheduledTime:          timestamp.TimePtr(time.Now().UTC()),
			StartedId:              common.EmptyEventID,
			StartedTime:            timestamp.TimePtr(time.Now().UTC()),
			ActivityId:             "activityID_5",
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			ScheduleToCloseTimeout: timestamp.DurationFromSeconds(200),
			StartToCloseTimeout:    timestamp.DurationFromSeconds(300),
			HeartbeatTimeout:       timestamp.DurationFromSeconds(50),
		},
	}

	expiryTime := timestamp.TimeNowPtrUtcAddDuration(time.Hour)
	timerInfos := map[string]*persistencespb.TimerInfo{
		"25": {
			Version:    failoverVersion,
			TimerId:    "25",
			StartedId:  85,
			ExpiryTime: expiryTime,
		},
	}

	childInfos := map[int64]*persistencespb.ChildExecutionInfo{
		80: {
			Version:               failoverVersion,
			InitiatedId:           80,
			InitiatedEventBatchId: 20,
			InitiatedEvent:        &historypb.HistoryEvent{},
			StartedId:             common.EmptyEventID,
			CreateRequestId:       uuid.New(),
			Namespace:             testNamespaceID,
			WorkflowTypeName:      "code.uber.internal/test/foobar",
		},
	}

	signalInfos := map[int64]*persistencespb.SignalInfo{
		75: {
			Version:               failoverVersion,
			InitiatedId:           75,
			InitiatedEventBatchId: 17,
			RequestId:             uuid.New(),
			Name:                  "test-signal-75",
			Input:                 payloads.EncodeString("signal-input-75"),
		},
	}

	signalRequestIDs := []string{
		uuid.New(),
	}

	bufferedEvents := []*historypb.HistoryEvent{
		{
			EventId:   common.BufferedEventID,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   failoverVersion,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: "test-signal-buffered",
				Input:      payloads.EncodeString("test-signal-buffered-input"),
			}},
		},
	}

	return &persistencespb.WorkflowMutableState{
		ExecutionInfo:       info,
		ExecutionState:      state,
		NextEventId:         int64(101),
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		ChildExecutionInfos: childInfos,
		SignalInfos:         signalInfos,
		SignalRequestedIds:  signalRequestIDs,
		BufferedEvents:      bufferedEvents,
	}
}
