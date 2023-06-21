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
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"

	"go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	updatespb "go.temporal.io/server/api/update/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/worker_versioning"
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
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		s.mockConfig,
	)
	// set the checksum probabilities to 100% for exercising during test
	s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateActivityFailureSizeLimitWarn = func(namespace string) int { return 1 * 1024 }
	s.mockConfig.MutableStateActivityFailureSizeLimitError = func(namespace string) int { return 2 * 1024 }
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
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
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

	newWorkflowTaskScheduleEvent, _ := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)

	newWorkflowTask := s.mutableState.GetWorkflowTaskByID(newWorkflowTaskScheduleEvent.GetEventId())
	s.NotNil(newWorkflowTask)

	_, err := s.mutableState.AddWorkflowTaskTimedOutEvent(
		newWorkflowTask,
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
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

	newWorkflowTaskScheduleEvent, _ := s.prepareTransientWorkflowTaskCompletionFirstBatchReplicated(version, runID)

	newWorkflowTask := s.mutableState.GetWorkflowTaskByID(newWorkflowTaskScheduleEvent.GetEventId())
	s.NotNil(newWorkflowTask)

	_, err := s.mutableState.AddWorkflowTaskFailedEvent(
		newWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		"", "", "", 0,
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
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
				snapshot, _, err := ms.CloseTransactionAsSnapshot(TransactionPolicyPassive)
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
				mutation, _, err := ms.CloseTransactionAsMutation(TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return mutation.Checksum, err
			},
		},
	}

	loadErrorsFunc := func() int64 {
		counter := s.testScope.Snapshot().Counters()["test.mutable_state_checksum_mismatch+operation=WorkflowContext,service_name=history"]
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
			s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
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
			s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc())

			// generate checksum again and verify its the same
			csum, err = tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.NotNil(csum.Value)
			s.Equal(dbState.Checksum.Value, csum.Value)

			// modify checksum and verify Load fails
			dbState.Checksum.Value[0]++
			s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors+1, loadErrorsFunc())
			s.EqualValues(dbState.Checksum, s.mutableState.checksum)

			// test checksum is invalidated
			loadErrors = loadErrorsFunc()
			s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
				return float64((s.mutableState.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) + 1)
			}
			s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc())
			s.Nil(s.mutableState.checksum)

			// revert the config value for the next test case
			s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
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
	s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 { return 0 }
	s.False(s.mutableState.shouldInvalidateCheckum())
	s.mutableState.executionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
		return float64((s.mutableState.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) + 1)
	}
	s.True(s.mutableState.shouldInvalidateCheckum())
	s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
		return float64((s.mutableState.executionInfo.LastUpdateTime.UnixNano() / int64(time.Second)) - 1)
	}
	s.False(s.mutableState.shouldInvalidateCheckum())
}

func (s *mutableStateSuite) TestContinueAsNewMinBackoff() {
	// set ContinueAsNew min interval to 5s
	s.mockConfig.ContinueAsNewMinInterval = func(namespace string) time.Duration {
		return 5 * time.Second
	}

	// with no backoff, verify min backoff is in [3s, 5s]
	minBackoff := s.mutableState.ContinueAsNewMinBackoff(nil)
	s.NotNil(minBackoff)
	s.True(*minBackoff >= 3*time.Second)
	s.True(*minBackoff <= 5*time.Second)

	// with 2s backoff, verify min backoff is in [3s, 5s]
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(timestamp.DurationPtr(time.Second * 2))
	s.NotNil(minBackoff)
	s.True(*minBackoff >= 3*time.Second)
	s.True(*minBackoff <= 5*time.Second)

	// with 6s backoff, verify min backoff unchanged
	backoff := timestamp.DurationPtr(time.Second * 6)
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(backoff)
	s.NotNil(minBackoff)
	s.True(minBackoff == backoff)

	// set start time to be 3s ago
	s.mutableState.executionInfo.StartTime = timestamp.TimePtr(time.Now().Add(-time.Second * 3))
	// with no backoff, verify min backoff is in [0, 2s]
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(nil)
	s.NotNil(minBackoff)
	s.True(*minBackoff >= 0)
	s.True(*minBackoff <= 2*time.Second, "%v\n", *minBackoff)

	// with 2s backoff, verify min backoff not changed
	backoff = timestamp.DurationPtr(time.Second * 2)
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(backoff)
	s.True(minBackoff == backoff)

	// set start time to be 5s ago
	s.mutableState.executionInfo.StartTime = timestamp.TimePtr(time.Now().Add(-time.Second * 5))
	// with no backoff, verify backoff unchanged (no backoff needed)
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(nil)
	s.Nil(minBackoff)

	// with 2s backoff, verify backoff unchanged
	backoff = timestamp.DurationPtr(time.Second * 2)
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(backoff)
	s.True(minBackoff == backoff)
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

	wt, err := s.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.NotNil(wt)

	s.Equal(int32(1), s.mutableState.GetExecutionInfo().WorkflowTaskAttempt)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
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

	wt, err := s.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
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
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
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
	sanitizedMutableState, err := NewSanitizedMutableState(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, mutableStateProto, 0, 0)
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
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	)
	s.Nil(err)
	s.NotNil(wt)

	wt, err = s.mutableState.ReplicateWorkflowTaskStartedEvent(nil,
		workflowTaskStartedEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskStartedEvent.GetEventId(),
		workflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(workflowTaskStartedEvent.GetEventTime()),
		false,
		123678,
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
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	)
	s.Nil(err)
	s.NotNil(wt)

	wt, err = s.mutableState.ReplicateWorkflowTaskStartedEvent(nil,
		newWorkflowTaskStartedEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(newWorkflowTaskStartedEvent.GetEventTime()),
		false,
		123678,
	)
	s.Nil(err)
	s.NotNil(wt)

	s.mutableState.SetHistoryBuilder(NewImmutableHistoryBuilder([]*historypb.HistoryEvent{
		newWorkflowTaskScheduleEvent,
		newWorkflowTaskStartedEvent,
	}))
	_, _, err = s.mutableState.CloseTransactionAsMutation(TransactionPolicyPassive)
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
		WorkflowTaskType:               enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: math.MaxInt64, Version: 300},
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

func (s *mutableStateSuite) TestUpdateInfos() {
	ctx := context.Background()
	cacheStore := map[events.EventKey]*historypb.HistoryEvent{}
	dbstate := s.buildWorkflowMutableState()
	var err error
	s.mutableState, err = newMutableStateFromDB(
		s.mockShard,
		NewMapEventCache(s.T(), cacheStore),
		s.logger,
		tests.LocalNamespaceEntry,
		dbstate,
		123,
	)
	s.NoError(err)
	err = s.mutableState.UpdateCurrentVersion(
		dbstate.ExecutionInfo.VersionHistories.Histories[0].Items[0].Version, false)
	s.Require().NoError(err)

	acceptedUpdateID := s.T().Name() + "-accepted-update-id"
	acceptedMsgID := s.T().Name() + "-accepted-msg-id"
	for i := 0; i < 2; i++ {
		updateID := fmt.Sprintf("%s-%d", acceptedUpdateID, i)
		_, err := s.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
			updateID,
			fmt.Sprintf("%s-%d", acceptedMsgID, i),
			1,
			&updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: updateID},
			},
		)
		s.Require().NoError(err)
	}
	completedUpdateID := s.T().Name() + "-completed-update-id"
	completedOutcome := &updatepb.Outcome{
		Value: &updatepb.Outcome_Success{Success: testPayloads},
	}
	_, err = s.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
		1234,
		&updatepb.Response{
			Meta:    &updatepb.Meta{UpdateId: completedUpdateID},
			Outcome: completedOutcome,
		},
	)
	s.Require().NoError(err)

	s.Require().Len(cacheStore, 3, "expected 1 completed update + 2 accepted in cache")

	outcome, err := s.mutableState.GetUpdateOutcome(ctx, completedUpdateID)
	s.Require().NoError(err)
	s.Require().Equal(completedOutcome, outcome)

	_, err = s.mutableState.GetUpdateOutcome(ctx, "not_an_update_id")
	s.Require().Error(err)
	s.Require().IsType((*serviceerror.NotFound)(nil), err)

	numCompleted := 0
	numAccepted := 0
	s.mutableState.VisitUpdates(func(updID string, updInfo *updatespb.UpdateInfo) {
		if comp := updInfo.GetCompletion(); comp != nil {
			numCompleted++
		}
		if updInfo.GetAcceptance() != nil {
			numAccepted++
		}
	})
	s.Require().Equal(numCompleted, 1, "expected 1 completed")
	s.Require().Equal(numAccepted, 2, "expected 2 accepted")

	mutation, _, err := s.mutableState.CloseTransactionAsMutation(TransactionPolicyPassive)
	s.Require().NoError(err)
	s.Require().Len(mutation.ExecutionInfo.UpdateInfos, 3,
		"expected 1 completed update + 2 accepted in mutation")
}

func (s *mutableStateSuite) TestReplicateActivityTaskStartedEvent() {
	state := s.buildWorkflowMutableState()

	var err error
	s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
	s.NoError(err)

	var scheduledEventID int64
	var ai *persistencespb.ActivityInfo
	for scheduledEventID, ai = range s.mutableState.GetPendingActivityInfos() {
		break
	}
	s.Nil(ai.LastHeartbeatDetails)

	now := time.Now()
	version := int64(101)
	requestID := "102"
	eventID := int64(104)
	attributes := &historypb.ActivityTaskStartedEventAttributes{
		ScheduledEventId: scheduledEventID,
		RequestId:        requestID,
	}
	err = s.mutableState.ReplicateActivityTaskStartedEvent(&historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: &now,
		Version:   version,
		Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: attributes,
		},
	})
	s.NoError(err)
	s.Assert().Equal(version, ai.Version)
	s.Assert().Equal(eventID, ai.StartedEventId)
	s.NotNil(ai.StartedTime)
	s.Assert().Equal(now, *ai.StartedTime)
	s.Assert().Equal(requestID, ai.RequestId)
	s.Assert().Nil(ai.LastHeartbeatDetails)
}

func (s *mutableStateSuite) TestTotalEntitiesCount() {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	// scheduling, starting & completing workflow task is omitted here

	workflowTaskCompletedEventID := int64(4)
	_, _, err := s.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		&commandpb.ScheduleActivityTaskCommandAttributes{},
		false,
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		uuid.New(),
		&commandpb.StartChildWorkflowExecutionCommandAttributes{},
		namespace.ID(uuid.New()),
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddTimerStartedEvent(
		workflowTaskCompletedEventID,
		&commandpb.StartTimerCommandAttributes{},
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		uuid.New(),
		&commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{},
		namespace.ID(uuid.New()),
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		uuid.New(),
		&commandpb.SignalExternalWorkflowExecutionCommandAttributes{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			},
		},
		namespace.ID(uuid.New()),
	)
	s.NoError(err)

	_, err = s.mutableState.AddWorkflowExecutionUpdateCompletedEvent(1234, &updatepb.Response{})
	s.NoError(err)

	_, err = s.mutableState.AddWorkflowExecutionSignaled(
		"signalName",
		&commonpb.Payloads{},
		"identity",
		&commonpb.Header{},
		false,
	)
	s.NoError(err)

	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
		tests.LocalNamespaceEntry.IsGlobalNamespace(),
		s.mutableState.GetCurrentVersion(),
	).Return(cluster.TestCurrentClusterName)
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)

	mutation, _, err := s.mutableState.CloseTransactionAsMutation(
		TransactionPolicyActive,
	)
	s.NoError(err)

	s.Equal(int64(1), mutation.ExecutionInfo.ActivityCount)
	s.Equal(int64(1), mutation.ExecutionInfo.ChildExecutionCount)
	s.Equal(int64(1), mutation.ExecutionInfo.UserTimerCount)
	s.Equal(int64(1), mutation.ExecutionInfo.RequestCancelExternalCount)
	s.Equal(int64(1), mutation.ExecutionInfo.SignalExternalCount)
	s.Equal(int64(1), mutation.ExecutionInfo.SignalCount)
	s.Equal(int64(1), mutation.ExecutionInfo.UpdateCount)
}

func (s *mutableStateSuite) TestSpeculativeWorkflowTaskNotPersisted() {
	testCases := []struct {
		name                 string
		enableBufferedEvents bool
		closeTxFunc          func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error)
	}{
		{
			name: "CloseTransactionAsSnapshot",
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error) {
				snapshot, _, err := ms.CloseTransactionAsSnapshot(TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return snapshot.ExecutionInfo, err
			},
		},
		{
			name:                 "CloseTransactionAsMutation",
			enableBufferedEvents: true,
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error) {
				mutation, _, err := ms.CloseTransactionAsMutation(TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			if !tc.enableBufferedEvents {
				dbState.BufferedEvents = nil
			}

			var err error
			s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)

			s.mutableState.executionInfo.WorkflowTaskScheduledEventId = s.mutableState.GetNextEventID()
			s.mutableState.executionInfo.WorkflowTaskStartedEventId = s.mutableState.GetNextEventID() + 1

			// Normal WT is persisted as is.
			execInfo, err := tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.Equal(enumsspb.WORKFLOW_TASK_TYPE_NORMAL, execInfo.WorkflowTaskType)
			s.NotEqual(common.EmptyEventID, execInfo.WorkflowTaskScheduledEventId)
			s.NotEqual(common.EmptyEventID, execInfo.WorkflowTaskStartedEventId)

			s.mutableState.executionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE

			// Speculative WT is converted to normal.
			execInfo, err = tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.Equal(enumsspb.WORKFLOW_TASK_TYPE_NORMAL, execInfo.WorkflowTaskType)
			s.NotEqual(common.EmptyEventID, execInfo.WorkflowTaskScheduledEventId)
			s.NotEqual(common.EmptyEventID, execInfo.WorkflowTaskStartedEventId)
		})
	}
}

func (s *mutableStateSuite) TestRetryActivity_TruncateRetryableFailure() {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	// scheduling, starting & completing workflow task is omitted here

	workflowTaskCompletedEventID := int64(4)
	_, activityInfo, err := s.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		&commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:   "5",
			ActivityType: &commonpb.ActivityType{Name: "activity-type"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "task-queue"},
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: timestamp.DurationFromSeconds(1),
			},
		},
		false,
	)
	s.NoError(err)

	_, err = s.mutableState.AddActivityTaskStartedEvent(
		activityInfo,
		activityInfo.ScheduledEventId,
		uuid.New(),
		"worker-identity",
	)
	s.NoError(err)

	failureSizeErrorLimit := s.mockConfig.MutableStateActivityFailureSizeLimitError(
		s.mutableState.namespaceEntry.Name().String(),
	)

	activityFailure := &failurepb.Failure{
		Message: "activity failure with large details",
		Source:  "application",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "application-failure-type",
			NonRetryable: false,
			Details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					{
						Data: make([]byte, failureSizeErrorLimit*2),
					},
				},
			},
		}},
	}
	s.Greater(activityFailure.Size(), failureSizeErrorLimit)

	retryState, err := s.mutableState.RetryActivity(activityInfo, activityFailure)
	s.NoError(err)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, retryState)

	activityInfo, ok := s.mutableState.GetActivityInfo(activityInfo.ScheduledEventId)
	s.True(ok)
	s.LessOrEqual(activityInfo.RetryLastFailure.Size(), failureSizeErrorLimit)
	s.Equal(activityFailure.GetMessage(), activityInfo.RetryLastFailure.Cause.GetMessage())
}

func (s *mutableStateSuite) TestTrackBuildIdFromCompletion() {
	versioned := func(buildId string) *commonpb.WorkerVersionStamp {
		return &commonpb.WorkerVersionStamp{BuildId: buildId, UseVersioning: true}
	}
	versionedSearchAttribute := func(buildIds ...string) []string {
		attrs := []string{}
		for _, buildId := range buildIds {
			attrs = append(attrs, worker_versioning.VersionedBuildIdSearchAttribute(buildId))
		}
		return attrs
	}
	unversioned := func(buildId string) *commonpb.WorkerVersionStamp {
		return &commonpb.WorkerVersionStamp{BuildId: buildId, UseVersioning: false}
	}
	unversionedSearchAttribute := func(buildIds ...string) []string {
		// assumed limit is 2
		attrs := []string{worker_versioning.UnversionedSearchAttribute, worker_versioning.UnversionedBuildIdSearchAttribute(buildIds[len(buildIds)-1])}
		return attrs
	}

	type testCase struct {
		name            string
		searchAttribute func(buildIds ...string) []string
		stamp           func(buildId string) *commonpb.WorkerVersionStamp
	}
	matrix := []testCase{
		{name: "unversioned", searchAttribute: unversionedSearchAttribute, stamp: unversioned},
		{name: "versioned", searchAttribute: versionedSearchAttribute, stamp: versioned},
	}
	for _, c := range matrix {
		s.T().Run(c.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			var err error
			s.mutableState, err = newMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)

			// Max 0
			err = s.mutableState.trackBuildIdFromCompletion(c.stamp("0.1"), 4, WorkflowTaskCompletionLimits{MaxResetPoints: 0, MaxSearchAttributeValueSize: 0})
			s.NoError(err)
			s.Equal([]string{}, s.getBuildIdsFromMutableState())
			s.Equal([]string{}, s.getResetPointsBinaryChecksumsFromMutableState())

			err = s.mutableState.trackBuildIdFromCompletion(c.stamp("0.1"), 4, WorkflowTaskCompletionLimits{MaxResetPoints: 2, MaxSearchAttributeValueSize: 40})
			s.NoError(err)
			s.Equal(c.searchAttribute("0.1"), s.getBuildIdsFromMutableState())
			s.Equal([]string{"0.1"}, s.getResetPointsBinaryChecksumsFromMutableState())

			// Add the same build ID
			err = s.mutableState.trackBuildIdFromCompletion(c.stamp("0.1"), 4, WorkflowTaskCompletionLimits{MaxResetPoints: 2, MaxSearchAttributeValueSize: 40})
			s.NoError(err)
			s.Equal(c.searchAttribute("0.1"), s.getBuildIdsFromMutableState())
			s.Equal([]string{"0.1"}, s.getResetPointsBinaryChecksumsFromMutableState())

			err = s.mutableState.trackBuildIdFromCompletion(c.stamp("0.2"), 4, WorkflowTaskCompletionLimits{MaxResetPoints: 2, MaxSearchAttributeValueSize: 40})
			s.NoError(err)
			s.Equal(c.searchAttribute("0.1", "0.2"), s.getBuildIdsFromMutableState())
			s.Equal([]string{"0.1", "0.2"}, s.getResetPointsBinaryChecksumsFromMutableState())

			// Limit applies
			err = s.mutableState.trackBuildIdFromCompletion(c.stamp("0.3"), 4, WorkflowTaskCompletionLimits{MaxResetPoints: 2, MaxSearchAttributeValueSize: 40})
			s.NoError(err)
			s.Equal(c.searchAttribute("0.2", "0.3"), s.getBuildIdsFromMutableState())
			s.Equal([]string{"0.2", "0.3"}, s.getResetPointsBinaryChecksumsFromMutableState())
		})
	}
}

func (s *mutableStateSuite) getBuildIdsFromMutableState() []string {
	searchAttributes := s.mutableState.executionInfo.SearchAttributes
	if searchAttributes == nil {
		return []string{}
	}

	payload, found := searchAttributes[searchattribute.BuildIds]
	if !found {
		return []string{}
	}
	decoded, err := searchattribute.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	s.NoError(err)
	buildIDs, ok := decoded.([]string)
	s.True(ok)
	return buildIDs
}

func (s *mutableStateSuite) getResetPointsBinaryChecksumsFromMutableState() []string {
	resetPoints := s.mutableState.executionInfo.GetAutoResetPoints().GetPoints()
	binaryChecksums := make([]string, len(resetPoints))
	for i, point := range resetPoints {
		binaryChecksums[i] = point.GetBinaryChecksum()
	}
	return binaryChecksums
}
