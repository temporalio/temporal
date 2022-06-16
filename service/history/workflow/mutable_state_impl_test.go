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

package workflow

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	mutableStateSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockConfig      *configs.Config
		mockShard       *shard.ContextTest
		mockEventsCache *events.MockCache

		mutableState *MutableStateImpl
		logger       log.Logger
		testScope    tally.TestScope
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

	s.mockConfig = tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 1,
			}},
		s.mockConfig,
	)
	// set the checksum probabilities to 100% for exercising during test
	s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }
	s.mockShard.SetEventsCacheForTesting(s.mockEventsCache)

	s.testScope = s.mockShard.Resource.MetricsScope.(tally.TestScope)
	s.logger = s.mockShard.GetLogger()

	s.mutableState = NewMutableState(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, time.Now().UTC())
}

func (s *mutableStateSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchReplicated_ReplicateWorkflowTaskCompleted() {
	version := int64(12)
	runID := uuid.New()
	s.mutableState = TestGlobalMutableState(
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
	s.mutableState.SetHistoryBuilder(NewImmutableHistoryBuilder([]*historypb.HistoryEvent{
		newWorkflowTaskCompletedEvent,
	}))
	err := s.mutableState.ReplicateWorkflowTaskCompletedEvent(newWorkflowTaskCompletedEvent)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.BufferEventSize())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchReplicated_FailoverWorkflowTaskTimeout() {
	version := int64(12)
	runID := uuid.New()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)
	_, err := s.mutableState.AddWorkflowTaskTimedOutEvent(
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.BufferEventSize())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchReplicated_FailoverWorkflowTaskFailed() {
	version := int64(12)
	runID := uuid.New()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)

	newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)

	_, err := s.mutableState.AddWorkflowTaskFailedEvent(
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		"", "", "", 0,
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.BufferEventSize())
}

func (s *mutableStateSuite) TestChecksum() {
	testCases := []struct {
		name                 string
		enableBufferedEvents bool
		closeTxFunc          func(ms *MutableStateImpl) (*persistencespb.Checksum, error)
	}{
		{
			name: "closeTransactionAsSnapshot",
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.Checksum, error) {
				snapshot, _, err := ms.CloseTransactionAsSnapshot(time.Now().UTC(), TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return snapshot.Checksum, err
			},
		},
		{
			name:                 "closeTransactionAsMutation",
			enableBufferedEvents: true,
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.Checksum, error) {
				mutation, _, err := ms.CloseTransactionAsMutation(time.Now().UTC(), TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return mutation.Checksum, err
			},
		},
	}

	loadErrorsFunc := func() int64 {
		counter := s.testScope.Snapshot().Counters()["test.mutable_state_checksum_mismatch+namespace=all,operation=WorkflowContext,service_name=history"]
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
			var err error
			s.mutableState, err = newMutableStateBuilderFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc()) // no errors expected
			s.EqualValues(dbState.Checksum, s.mutableState.checksum)
			s.mutableState.namespaceEntry = s.newNamespaceCacheEntry()
			csum, err := tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.NotNil(csum.Value)
			s.Equal(enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY, csum.Flavor)
			s.Equal(mutableStateChecksumPayloadV1, csum.Version)
			s.EqualValues(csum, s.mutableState.checksum)

			// verify checksum is verified on Load
			dbState.Checksum = csum
			s.mutableState, err = newMutableStateBuilderFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc())

			// generate checksum again and verify its the same
			csum, err = tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.NotNil(csum.Value)
			s.Equal(dbState.Checksum.Value, csum.Value)

			// modify checksum and verify Load fails
			dbState.Checksum.Value[0]++
			s.mutableState, err = newMutableStateBuilderFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors+1, loadErrorsFunc())
			s.EqualValues(dbState.Checksum, s.mutableState.checksum)

			// test checksum is invalidated
			loadErrors = loadErrorsFunc()
			s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
				return float64((s.mutableState.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) + 1)
			}
			s.mutableState, err = newMutableStateBuilderFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc())
			s.Nil(s.mutableState.checksum)

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
			shouldGenerate := s.mutableState.shouldGenerateChecksum()
			shouldVerify := s.mutableState.shouldVerifyChecksum()
			s.Equal(prob == 100, shouldGenerate)
			s.Equal(prob == 100, shouldVerify)
		}
	}
}

func (s *mutableStateSuite) TestChecksumShouldInvalidate() {
	s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 { return 0 }
	s.False(s.mutableState.shouldInvalidateCheckum())
	s.mutableState.executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
		return float64((s.mutableState.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) + 1)
	}
	s.True(s.mutableState.shouldInvalidateCheckum())
	s.mockConfig.MutableStateChecksumInvalidateBefore = func(...dynamicconfig.FilterOption) float64 {
		return float64((s.mutableState.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) - 1)
	}
	s.False(s.mutableState.shouldInvalidateCheckum())
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
	isReapplied := s.mutableState.IsResourceDuplicated(dedupResource)
	s.False(isReapplied)
	s.mutableState.UpdateDuplicatedResource(dedupResource)
	isReapplied = s.mutableState.IsResourceDuplicated(dedupResource)
	s.True(isReapplied)
}

func (s *mutableStateSuite) TestTransientWorkflowTaskSchedule_CurrentVersionChanged() {
	version := int64(2000)
	runID := uuid.New()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)
	_, _ = s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)
	err := s.mutableState.ReplicateWorkflowTaskFailedEvent()
	s.NoError(err)

	err = s.mutableState.UpdateCurrentVersion(version+1, true)
	s.NoError(err)
	versionHistories := s.mutableState.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, &historyspb.VersionHistoryItem{
		EventId: s.mutableState.GetNextEventID() - 1,
		Version: version,
	})
	s.NoError(err)

	wt, err := s.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc())
	s.NoError(err)
	s.NotNil(wt)

	s.Equal(int32(1), s.mutableState.GetExecutionInfo().WorkflowTaskAttempt)
	s.Equal(0, s.mutableState.hBuilder.BufferEventSize())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskStart_CurrentVersionChanged() {
	version := int64(2000)
	runID := uuid.New()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		runID,
	)
	_, _ = s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)
	err := s.mutableState.ReplicateWorkflowTaskFailedEvent()
	s.NoError(err)

	versionHistories := s.mutableState.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, &historyspb.VersionHistoryItem{
		EventId: s.mutableState.GetNextEventID() - 1,
		Version: version,
	})
	s.NoError(err)

	wt, err := s.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc())
	s.NoError(err)
	s.NotNil(wt)

	err = s.mutableState.UpdateCurrentVersion(version+1, true)
	s.NoError(err)

	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		s.mutableState.GetNextEventID(),
		uuid.New(),
		&taskqueuepb.TaskQueue{},
		"random identity",
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.BufferEventSize())
}

func (s *mutableStateSuite) TestSanitizedMutableState() {
	txnID := int64(2000)
	runID := uuid.New()
	mutableState := TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		1000,
		runID,
	)

	mutableState.executionInfo.LastFirstEventTxnId = txnID
	mutableState.executionInfo.ParentClock = &clock.VectorClock{
		ShardId: 1,
		Clock:   1,
	}
	mutableState.pendingChildExecutionInfoIDs = map[int64]*persistencespb.ChildExecutionInfo{1: {
		Clock: &clock.VectorClock{
			ShardId: 1,
			Clock:   1,
		},
	}}

	mutableStateProto := mutableState.CloneToProto()
	sanitizedMutableState, err := NewSanitizedMutableState(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, mutableStateProto)
	s.NoError(err)
	s.Equal(int64(0), sanitizedMutableState.executionInfo.LastFirstEventTxnId)
	s.Nil(sanitizedMutableState.executionInfo.ParentClock)
	for _, childInfo := range sanitizedMutableState.pendingChildExecutionInfoIDs {
		s.Nil(childInfo.Clock)
	}
}

func (s *mutableStateSuite) prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version int64, runID string) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	namespaceID := tests.NamespaceID
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
		events.EventKey{
			NamespaceID: namespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     workflowStartEvent.GetEventId(),
			Version:     version,
		},
		workflowStartEvent,
	)
	err := s.mutableState.ReplicateWorkflowExecutionStartedEvent(
		nil,
		execution,
		uuid.New(),
		workflowStartEvent,
	)
	s.Nil(err)

	// setup transient workflow task
	wt, err := s.mutableState.ReplicateWorkflowTaskScheduledEvent(
		workflowTaskScheduleEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
	)
	s.Nil(err)
	s.NotNil(wt)

	wt, err = s.mutableState.ReplicateWorkflowTaskStartedEvent(nil,
		workflowTaskStartedEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskStartedEvent.GetEventId(),
		workflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(workflowTaskStartedEvent.GetEventTime()),
	)
	s.Nil(err)
	s.NotNil(wt)

	err = s.mutableState.ReplicateWorkflowTaskFailedEvent()
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
	eventID++

	wt, err = s.mutableState.ReplicateWorkflowTaskScheduledEvent(
		newWorkflowTaskScheduleEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
	)
	s.Nil(err)
	s.NotNil(wt)

	wt, err = s.mutableState.ReplicateWorkflowTaskStartedEvent(nil,
		newWorkflowTaskStartedEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(newWorkflowTaskStartedEvent.GetEventTime()),
	)
	s.Nil(err)
	s.NotNil(wt)

	s.mutableState.SetHistoryBuilder(NewImmutableHistoryBuilder([]*historypb.HistoryEvent{
		newWorkflowTaskScheduleEvent,
		newWorkflowTaskStartedEvent,
	}))
	_, _, err = s.mutableState.CloseTransactionAsMutation(time.Now().UTC(), TransactionPolicyPassive)
	s.NoError(err)

	return newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent
}

func (s *mutableStateSuite) newNamespaceCacheEntry() *namespace.Namespace {
	return namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "mutableStateTest"},
		&persistencespb.NamespaceConfig{},
		true,
		&persistencespb.NamespaceReplicationConfig{},
		1,
	)
}

func (s *mutableStateSuite) buildWorkflowMutableState() *persistencespb.WorkflowMutableState {
	namespaceID := tests.NamespaceID
	we := commonpb.WorkflowExecution{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}
	tl := "testTaskQueue"
	failoverVersion := int64(300)

	startTime := timestamp.TimePtr(time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC))
	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                    namespaceID.String(),
		WorkflowId:                     we.GetWorkflowId(),
		TaskQueue:                      tl,
		WorkflowTypeName:               "wType",
		WorkflowRunTimeout:             timestamp.DurationFromSeconds(200),
		DefaultWorkflowTaskTimeout:     timestamp.DurationFromSeconds(100),
		LastWorkflowTaskStartedEventId: int64(99),
		LastUpdateTime:                 timestamp.TimeNowPtrUtc(),
		StartTime:                      startTime,
		ExecutionTime:                  startTime,
		WorkflowTaskVersion:            failoverVersion,
		WorkflowTaskScheduledEventId:   101,
		WorkflowTaskStartedEventId:     102,
		WorkflowTaskTimeout:            timestamp.DurationFromSeconds(100),
		WorkflowTaskAttempt:            1,
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
			ScheduledEventId:       int64(90),
			ScheduledTime:          timestamp.TimePtr(time.Now().UTC()),
			StartedEventId:         common.EmptyEventID,
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
			Version:        failoverVersion,
			TimerId:        "25",
			StartedEventId: 85,
			ExpiryTime:     expiryTime,
		},
	}

	childInfos := map[int64]*persistencespb.ChildExecutionInfo{
		80: {
			Version:               failoverVersion,
			InitiatedEventId:      80,
			InitiatedEventBatchId: 20,
			StartedEventId:        common.EmptyEventID,
			CreateRequestId:       uuid.New(),
			Namespace:             tests.Namespace.String(),
			WorkflowTypeName:      "code.uber.internal/test/foobar",
		},
	}

	signalInfos := map[int64]*persistencespb.SignalInfo{
		75: {
			Version:               failoverVersion,
			InitiatedEventId:      75,
			InitiatedEventBatchId: 17,
			RequestId:             uuid.New(),
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
		NextEventId:         int64(103),
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		ChildExecutionInfos: childInfos,
		SignalInfos:         signalInfos,
		SignalRequestedIds:  signalRequestIDs,
		BufferedEvents:      bufferedEvents,
	}
}
