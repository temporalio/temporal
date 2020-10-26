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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	historyBuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shardContextTest
		mockEventsCache    *MockeventsCache
		mockNamespaceCache *cache.MockNamespaceCache

		namespaceID    string
		namespaceEntry *cache.NamespaceCacheEntry
		msBuilder      mutableState
		builder        *historyBuilder
		logger         log.Logger
	}
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historyBuilderSuite)
	suite.Run(t, s)
}

func (s *historyBuilderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.logger = log.NewNoop()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.namespaceID = testNamespaceID
	s.namespaceEntry = cache.NewLocalNamespaceCacheEntryForTest(&persistencespb.NamespaceInfo{Id: s.namespaceID}, &persistencespb.NamespaceConfig{}, "", nil)

	s.mockNamespaceCache = s.mockShard.resource.NamespaceCache
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockNamespaceCache.EXPECT().GetNamespace(gomock.Any()).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockEventsCache.EXPECT().putEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.msBuilder = newMutableStateBuilder(s.mockShard, s.mockEventsCache,
		s.logger, testLocalNamespaceEntry)
	s.builder = newHistoryBuilder(s.msBuilder, s.logger)
}

func (s *historyBuilderSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *historyBuilderSuite) TestHistoryBuilderDynamicSuccess() {
	ns := "namespace1"
	id := "dynamic-historybuilder-success-test-workflow-id"
	rid := "dynamic-historybuilder-success-test-run-id"
	wt := "dynamic-historybuilder-success-type"
	tl := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "dynamic-historybuilder-success-taskqueue"}
	identity := "dynamic-historybuilder-success-worker"
	input := payloads.EncodeString("dynamic-historybuilder-success-input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	we := commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	di := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, wtRunning0 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskStartedEvent := s.addWorkflowTaskStartedEvent(2, tl, identity)
	s.validateWorkflowTaskStartedEvent(workflowTaskStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	wtInfo1, wtRunning1 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning1)
	s.NotNil(wtInfo1)
	workflowTaskStartedID1 := wtInfo1.StartedID
	s.Equal(int64(3), workflowTaskStartedID1)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskCompletedEvent := s.addWorkflowTaskCompletedEvent(2, 3, identity)
	s.validateWorkflowTaskCompletedEvent(workflowTaskCompletedEvent, 4, 2, 3, identity)
	s.Equal(int64(5), s.getNextEventID())
	_, wtRunning2 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.False(wtRunning2)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activityTaskQueue := "dynamic-historybuilder-success-activity-taskqueue"
	activityTimeout := time.Duration(60) * time.Second
	queueTimeout := time.Duration(20) * time.Second
	hearbeatTimeout := time.Duration(10) * time.Second

	activity1ID := "activity1"
	activity1Type := "dynamic-historybuilder-success-activity1-type"
	activity1Input := payloads.EncodeString("dynamic-historybuilder-success-activity1-input")
	activity1Result := payloads.EncodeString("dynamic-historybuilder-success-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskQueue, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout, nil, ns)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskQueue, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout, ns)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(common.EmptyEventID, ai0.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity2ID := "activity2"
	activity2Type := "dynamic-historybuilder-success-activity2-type"
	activity2Input := payloads.EncodeString("dynamic-historybuilder-success-activity2-input")
	activity2Failure := failure.NewServerFailure("dynamic-historybuilder-success-activity2-failed", false)
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskQueue, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout, nil, ns)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskQueue, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout, ns)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(common.EmptyEventID, ai2.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity3ID := "activity3"
	activity3Type := "dynamic-historybuilder-success-activity3-type"
	activity3Input := payloads.EncodeString("dynamic-historybuilder-success-activity3-input")
	activity3RetryPolicy := &commonpb.RetryPolicy{
		InitialInterval:        timestamp.DurationPtr(1 * time.Second),
		MaximumAttempts:        3,
		MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
		NonRetryableErrorTypes: []string{"bad-bug"},
		BackoffCoefficient:     1,
	}
	activity3ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity3ID, activity3Type,
		activityTaskQueue, activity3Input, activityTimeout, queueTimeout, hearbeatTimeout, activity3RetryPolicy, ns)
	s.validateActivityTaskScheduledEvent(activity3ScheduledEvent, 7, 4, activity3ID, activity3Type,
		activityTaskQueue, activity3Input, activityTimeout, queueTimeout, hearbeatTimeout, ns)
	s.Equal(int64(8), s.getNextEventID())
	ai2, activity3Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity3Running0)
	s.Equal(common.EmptyEventID, ai2.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskQueue, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, common.BufferedEventID, 5, identity,
		0, nil)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activityStartedEvent, 8, 5, identity,
		0, nil)
	s.Equal(int64(9), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(8), ai3.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 8, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, common.BufferedEventID, 5, 8, activity1Result,
		identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 9, 5, 8, activity1Result,
		identity)
	s.Equal(int64(10), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	di2 := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(di2, 10, tl, taskTimeout)
	s.Equal(int64(11), s.getNextEventID())
	di3, wtRunning3 := s.msBuilder.GetWorkflowTaskInfo(10)
	s.True(wtRunning3)
	s.Equal(common.EmptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskQueue, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, common.BufferedEventID, 6, identity,
		0, nil)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activity2StartedEvent, 11, 6, identity,
		0, nil)
	s.Equal(int64(12), s.getNextEventID())
	ai4, activity2Running1 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running1)
	s.Equal(int64(11), ai4.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity2FailedEvent := s.addActivityTaskFailedEvent(6, 11, activity2Failure, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, common.BufferedEventID, 6, 11, activity2Failure, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskFailedEvent(activity2FailedEvent, 12, 6, 11, activity2Failure, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, identity)
	s.Equal(int64(13), s.getNextEventID())
	_, activity2Running3 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running3)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity3StartedEvent := s.addActivityTaskStartedEvent(7, activityTaskQueue, identity)
	s.validateTransientActivityTaskStartedEvent(activity3StartedEvent, common.TransientEventID, 7, identity)
	s.Equal(int64(13), s.getNextEventID())
	ai5, activity3Running1 := s.msBuilder.GetActivityInfo(7)
	s.True(activity3Running1)
	s.Equal(common.TransientEventID, ai5.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity3Failure := failure.NewServerFailure("dynamic-historybuilder-success-activity3-failed", false)
	s.msBuilder.RetryActivity(ai5, activity3Failure)
	ai6, activity3Running2 := s.msBuilder.GetActivityInfo(7)
	s.Equal(activity3Failure, ai6.RetryLastFailure)
	s.True(activity3Running2)

	activity3StartedEvent2 := s.addActivityTaskStartedEvent(7, activityTaskQueue, identity)
	s.validateTransientActivityTaskStartedEvent(activity3StartedEvent2, common.TransientEventID, 7, identity)
	s.Equal(int64(13), s.getNextEventID())
	ai7, activity3Running3 := s.msBuilder.GetActivityInfo(7)
	s.True(activity3Running3)
	s.Equal(common.TransientEventID, ai7.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activity3Result := payloads.EncodeString("dynamic-historybuilder-success-activity1-result")
	activity3CompletedEvent := s.addActivityTaskCompletedEvent(7, common.TransientEventID, activity3Result, identity)
	s.validateActivityTaskCompletedEvent(activity3CompletedEvent, common.BufferedEventID, 7, common.TransientEventID,
		activity3Result, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activity3CompletedEvent, 14, 7, 13, activity3Result, identity)
	s.Equal(int64(15), s.getNextEventID())
	ai8, activity3Running4 := s.msBuilder.GetActivityInfo(7)
	s.Nil(ai8)
	s.False(activity3Running4)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// Verify the last ActivityTaskStartedEvent which should show the error from the first attempt
	historyEvents := s.msBuilder.GetHistoryBuilder().GetHistory().GetEvents()
	s.Len(historyEvents, 14)
	s.validateActivityTaskStartedEvent(historyEvents[12], 13, 7, identity, 1, activity3Failure)

	markerDetails := map[string]*commonpb.Payloads{
		"data": payloads.EncodeString("dynamic-historybuilder-success-marker-details"),
	}
	markerHeaderField1 := payload.EncodeString("dynamic-historybuilder-success-marker-header1")
	markerHeaderField2 := payload.EncodeString("dynamic-historybuilder-success-marker-header2")
	markerHeader := map[string]*commonpb.Payload{
		"name1": markerHeaderField1,
		"name2": markerHeaderField2,
	}
	markerEvent := s.addMarkerRecordedEvent(4, "testMarker", markerDetails, &markerHeader)
	s.validateMarkerRecordedEvent(markerEvent, 15, 4, "testMarker", markerDetails, &markerHeader)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.Equal(int64(16), s.getNextEventID())
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowStartFailures() {
	id := "historybuilder-workflowstart-failures-test-workflow-id"
	rid := "historybuilder-workflowstart-failures-test-run-id"
	wt := "historybuilder-workflowstart-failures-type"
	tl := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "historybuilder-workflowstart-failures-taskqueue"}
	identity := "historybuilder-workflowstart-failures-worker"
	input := payloads.EncodeString("historybuilder-workflowstart-failures-input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	we := commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	di := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, wtRunning0 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	_, err := s.msBuilder.AddWorkflowExecutionStartedEvent(
		we,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:               we.WorkflowId,
				WorkflowType:             &commonpb.WorkflowType{Name: wt},
				TaskQueue:                tl,
				Input:                    input,
				WorkflowExecutionTimeout: &execTimeout,
				WorkflowRunTimeout:       &runTimeout,
				WorkflowTaskTimeout:      &taskTimeout,
				Identity:                 identity,
			},
		})
	s.NotNil(err)

	s.Equal(int64(3), s.getNextEventID(), s.printHistory())
	di1, wtRunning1 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning1)
	s.Equal(common.EmptyEventID, di1.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowTaskScheduledFailures() {
	id := "historybuilder-workflow-task-scheduled-failures-test-workflow-id"
	rid := "historybuilder-workflow-task-scheduled-failures-test-run-id"
	wt := "historybuilder-workflow-task-scheduled-failures-type"
	tl := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "historybuilder-workflow-task-scheduled-failures-taskqueue"}
	identity := "historybuilder-workflow-task-scheduled-failures-worker"
	input := payloads.EncodeString("historybuilder-workflow-task-scheduled-failures-input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	we := commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	di := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, wtRunning0 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	_, err := s.msBuilder.AddWorkflowTaskScheduledEvent(false)
	s.NotNil(err)
	s.Equal(int64(3), s.getNextEventID())
	di1, wtRunning1 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning1)
	s.Equal(common.EmptyEventID, di1.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowTaskStartedFailures() {
	id := "historybuilder-workflow-task-started-failures-test-workflow-id"
	rid := "historybuilder-workflow-task-started-failures-test-run-id"
	wt := "historybuilder-workflow-task-started-failures-type"
	tl := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "historybuilder-workflow-task-started-failures-taskqueue"}
	identity := "historybuilder-workflow-task-started-failures-worker"
	input := payloads.EncodeString("historybuilder-workflow-task-started-failures-input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	we := commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	_, _, err := s.msBuilder.AddWorkflowTaskStartedEvent(2, uuid.New(), &workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tl,
		Identity:  identity,
	})
	s.NotNil(err)
	s.Equal(int64(2), s.getNextEventID())
	_, wtRunning1 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.False(wtRunning1)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	di := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, wtRunning0 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	_, _, err = s.msBuilder.AddWorkflowTaskStartedEvent(100, uuid.New(), &workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: tl,
		Identity:  identity,
	})
	s.NotNil(err)
	s.Equal(int64(3), s.getNextEventID())
	di2, wtRunning2 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning2)
	s.Equal(common.EmptyEventID, di2.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskStartedEvent2 := s.addWorkflowTaskStartedEvent(2, tl, identity)
	s.validateWorkflowTaskStartedEvent(workflowTaskStartedEvent2, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di3, wtRunning3 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning3)
	s.Equal(int64(3), di3.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderFlushBufferedEvents() {
	ns := "namespace1"
	id := "flush-buffered-events-test-workflow-id"
	rid := "flush-buffered-events-test-run-id"
	wt := "flush-buffered-events-type"
	tl := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "flush-buffered-events-taskqueue"}
	identity := "flush-buffered-events-worker"
	input := payloads.EncodeString("flush-buffered-events-input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	we := commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	// 1 execution started
	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, runTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	// 2 workflow task scheduled
	di := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, wtRunning0 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	// 3 workflow task started
	workflowTaskStartedEvent := s.addWorkflowTaskStartedEvent(2, tl, identity)
	s.validateWorkflowTaskStartedEvent(workflowTaskStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	wtInfo1, wtRunning1 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning1)
	s.NotNil(wtInfo1)
	wtStartedID1 := wtInfo1.StartedID
	s.Equal(int64(3), wtStartedID1)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	// 4 workflow task completed
	workflowTaskCompletedEvent := s.addWorkflowTaskCompletedEvent(2, 3, identity)
	s.validateWorkflowTaskCompletedEvent(workflowTaskCompletedEvent, 4, 2, 3, identity)
	s.Equal(int64(5), s.getNextEventID())
	_, wtRunning2 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.False(wtRunning2)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	activityTaskQueue := "flush-buffered-events-activity-taskqueue"
	activityTimeout := time.Duration(60) * time.Second
	queueTimeout := time.Duration(20) * time.Second
	hearbeatTimeout := time.Duration(10) * time.Second

	// 5 activity1 scheduled
	activity1ID := "activity1"
	activity1Type := "flush-buffered-events-activity1-type"
	activity1Input := payloads.EncodeString("flush-buffered-events-activity1-input")
	activity1Result := payloads.EncodeString("flush-buffered-events-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskQueue, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout, nil, ns)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskQueue, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout, ns)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(common.EmptyEventID, ai0.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 6 activity 2 scheduled
	activity2ID := "activity2"
	activity2Type := "flush-buffered-events-activity2-type"
	activity2Input := payloads.EncodeString("flush-buffered-events-activity2-input")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskQueue, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout, nil, ns)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskQueue, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout, ns)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(common.EmptyEventID, ai2.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 7 activity1 started
	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskQueue, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, common.BufferedEventID, 5, identity,
		0, nil)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity,
		0, nil)
	s.Equal(int64(8), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(7), ai3.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 8 activity1 completed
	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 7, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, common.BufferedEventID, 5, 7, activity1Result, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 8, 5, 7, activity1Result, identity)
	s.Equal(int64(9), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 9 workflow task2 scheduled
	wtInfo2 := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(wtInfo2, 9, tl, taskTimeout)
	s.Equal(int64(10), s.getNextEventID())
	di3, wtRunning3 := s.msBuilder.GetWorkflowTaskInfo(9)
	s.True(wtRunning3)
	s.Equal(common.EmptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 10 workflow task2 started
	workflowTask2StartedEvent := s.addWorkflowTaskStartedEvent(9, tl, identity)
	s.validateWorkflowTaskStartedEvent(workflowTask2StartedEvent, 10, 9, identity)
	s.Equal(int64(11), s.getNextEventID())
	wtInfo2, wt2Running := s.msBuilder.GetWorkflowTaskInfo(9)
	s.True(wt2Running)
	s.NotNil(wtInfo2)
	wt2StartedID := wtInfo2.StartedID
	s.Equal(int64(10), wt2StartedID)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 11 (buffered) activity2 started
	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskQueue, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, common.BufferedEventID, 6, identity,
		0, nil)
	s.Equal(int64(11), s.getNextEventID())
	ai4, activity2Running := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running)
	s.Equal(common.BufferedEventID, ai4.StartedId)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 12 (buffered) activity2 failed
	activity2Failure := failure.NewServerFailure("flush-buffered-events-activity2-failed", false)
	activity2FailedEvent := s.addActivityTaskFailedEvent(6, common.BufferedEventID, activity2Failure, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, common.BufferedEventID, 6, common.BufferedEventID, activity2Failure, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, identity)
	s.Equal(int64(11), s.getNextEventID())
	_, activity2Running2 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running2)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	// 13 (eventId will be 11) workflow task completed
	wt2CompletedEvent := s.addWorkflowTaskCompletedEvent(9, 10, identity)
	s.validateWorkflowTaskCompletedEvent(wt2CompletedEvent, 11, 9, 10, identity)
	s.Equal(int64(11), wt2CompletedEvent.GetEventId())
	s.Equal(int64(12), s.getNextEventID())
	_, wt2Running2 := s.msBuilder.GetWorkflowTaskInfo(2)
	s.False(wt2Running2)
	s.Equal(int64(10), s.getPreviousWorkflowTaskStartedEventID())

	// flush buffered events. 12: Activity2Started, 13: Activity2Failed
	s.NoError(s.msBuilder.FlushBufferedEvents())
	s.Equal(int64(14), s.getNextEventID())
	activity2StartedEvent2 := s.msBuilder.GetHistoryBuilder().history[11]
	s.Equal(int64(12), activity2StartedEvent2.GetEventId())
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, activity2StartedEvent2.GetEventType())

	activity2FailedEvent2 := s.msBuilder.GetHistoryBuilder().history[12]
	s.Equal(int64(13), activity2FailedEvent2.GetEventId())
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, activity2FailedEvent2.GetEventType())
	s.Equal(int64(12), activity2FailedEvent2.GetActivityTaskFailedEventAttributes().GetStartedEventId())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowCancellationRequested() {
	workflowType := "some random workflow type"
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	identity := "some random identity"
	input := payloads.EncodeString("some random workflow input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(
		workflowExecution, workflowType, taskqueue, input, execTimeout, runTimeout, taskTimeout, identity,
	)
	s.validateWorkflowExecutionStartedEvent(
		workflowStartedEvent, workflowType, taskqueue, input, execTimeout, runTimeout, taskTimeout, identity,
	)
	s.Equal(int64(2), s.getNextEventID())

	wtInfo := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(wtInfo, 2, taskqueue, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	wtInfo, wtRunning := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning)
	s.NotNil(wtInfo)
	s.Equal(int64(2), wtInfo.ScheduleID)
	s.Equal(common.EmptyEventID, wtInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskStartedEvent := s.addWorkflowTaskStartedEvent(2, taskqueue, identity)
	s.validateWorkflowTaskStartedEvent(workflowTaskStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	wtInfo, wtRunning = s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning)
	s.NotNil(wtInfo)
	s.Equal(int64(2), wtInfo.ScheduleID)
	s.Equal(int64(3), wtInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskCompletedEvent := s.addWorkflowTaskCompletedEvent(2, 3, identity)
	s.validateWorkflowTaskCompletedEvent(workflowTaskCompletedEvent, 4, 2, 3, identity)
	s.Equal(int64(5), s.getNextEventID())
	wtInfo, wtRunning = s.msBuilder.GetWorkflowTaskInfo(2)
	s.False(wtRunning)
	s.Nil(wtInfo)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	targetNamespace := "some random target namespace"
	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      "some random target run ID",
	}
	cancellationChildWorkflowOnly := true
	cancellationInitiatedEvent := s.addRequestCancelExternalWorkflowExecutionInitiatedEvent(
		4, targetNamespace, targetExecution, cancellationChildWorkflowOnly,
	)
	s.validateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		cancellationInitiatedEvent, 5, 4, targetNamespace, targetExecution, cancellationChildWorkflowOnly,
	)
	s.Equal(int64(6), s.getNextEventID())

	cancellationRequestedEvent := s.addExternalWorkflowExecutionCancelRequested(
		5, targetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(),
	)
	s.validateExternalWorkflowExecutionCancelRequested(cancellationRequestedEvent, common.BufferedEventID, 5, targetNamespace, targetExecution)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateExternalWorkflowExecutionCancelRequested(cancellationRequestedEvent, 6, 5, targetNamespace, targetExecution)
	s.Equal(int64(7), s.getNextEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowCancellationFailed() {
	workflowType := "some random workflow type"
	taskqueue := &taskqueuepb.TaskQueue{Kind: enumspb.TASK_QUEUE_KIND_NORMAL, Name: "some random taskqueue"}
	identity := "some random identity"
	input := payloads.EncodeString("some random workflow input")
	execTimeout := time.Duration(70) * time.Second
	runTimeout := time.Duration(60) * time.Second
	taskTimeout := time.Duration(10) * time.Second
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(
		workflowExecution, workflowType, taskqueue, input, execTimeout, runTimeout, taskTimeout, identity,
	)
	s.validateWorkflowExecutionStartedEvent(
		workflowStartedEvent, workflowType, taskqueue, input, execTimeout, runTimeout, taskTimeout, identity,
	)
	s.Equal(int64(2), s.getNextEventID())

	wtInfo := s.addWorkflowTaskScheduledEvent()
	s.validateWorkflowTaskScheduledEvent(wtInfo, 2, taskqueue, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	wtInfo, wtRunning := s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning)
	s.NotNil(wtInfo)
	s.Equal(int64(2), wtInfo.ScheduleID)
	s.Equal(common.EmptyEventID, wtInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskStartedEvent := s.addWorkflowTaskStartedEvent(2, taskqueue, identity)
	s.validateWorkflowTaskStartedEvent(workflowTaskStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	wtInfo, wtRunning = s.msBuilder.GetWorkflowTaskInfo(2)
	s.True(wtRunning)
	s.NotNil(wtInfo)
	s.Equal(int64(2), wtInfo.ScheduleID)
	s.Equal(int64(3), wtInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousWorkflowTaskStartedEventID())

	workflowTaskCompletedEvent := s.addWorkflowTaskCompletedEvent(2, 3, identity)
	s.validateWorkflowTaskCompletedEvent(workflowTaskCompletedEvent, 4, 2, 3, identity)
	s.Equal(int64(5), s.getNextEventID())
	wtInfo, wtRunning = s.msBuilder.GetWorkflowTaskInfo(2)
	s.False(wtRunning)
	s.Nil(wtInfo)
	s.Equal(int64(3), s.getPreviousWorkflowTaskStartedEventID())

	targetNamespace := "some random target namespace"
	targetExecution := commonpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      "some random target run ID",
	}
	cancellationChildWorkflowOnly := true
	cancellationFailedCause := enumspb.CancelExternalWorkflowExecutionFailedCause(59)
	cancellationInitiatedEvent := s.addRequestCancelExternalWorkflowExecutionInitiatedEvent(
		4, targetNamespace, targetExecution, cancellationChildWorkflowOnly,
	)
	s.validateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		cancellationInitiatedEvent, 5, 4, targetNamespace, targetExecution, cancellationChildWorkflowOnly,
	)
	s.Equal(int64(6), s.getNextEventID())

	cancellationRequestedEvent := s.addRequestCancelExternalWorkflowExecutionFailedEvent(
		4, 5, targetNamespace, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), cancellationFailedCause,
	)
	s.validateRequestCancelExternalWorkflowExecutionFailedEvent(
		cancellationRequestedEvent, common.BufferedEventID, 4, 5, targetNamespace, targetExecution, cancellationFailedCause,
	)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateRequestCancelExternalWorkflowExecutionFailedEvent(
		cancellationRequestedEvent, 6, 4, 5, targetNamespace, targetExecution, cancellationFailedCause,
	)
	s.Equal(int64(7), s.getNextEventID())
}

func (s *historyBuilderSuite) getNextEventID() int64 {
	return s.msBuilder.GetNextEventID()
}

func (s *historyBuilderSuite) getPreviousWorkflowTaskStartedEventID() int64 {
	return s.msBuilder.GetExecutionInfo().LastProcessedEvent
}

func (s *historyBuilderSuite) addWorkflowExecutionStartedEvent(we commonpb.WorkflowExecution, workflowType string,
	taskQueue *taskqueuepb.TaskQueue, input *commonpb.Payloads, executionTimeout, runTimeout, taskTimeout time.Duration, identity string) *historypb.HistoryEvent {

	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               we.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                taskQueue,
		Input:                    input,
		WorkflowExecutionTimeout: &executionTimeout,
		WorkflowRunTimeout:       &runTimeout,
		WorkflowTaskTimeout:      &taskTimeout,
		Identity:                 identity,
	}

	event, err := s.msBuilder.AddWorkflowExecutionStartedEvent(
		we,
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:      1,
			NamespaceId:  s.namespaceID,
			StartRequest: request,
		},
	)
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addWorkflowTaskScheduledEvent() *workflowTaskInfo {
	di, err := s.msBuilder.AddWorkflowTaskScheduledEvent(false)
	s.Nil(err)
	return di
}

func (s *historyBuilderSuite) addWorkflowTaskStartedEvent(scheduleID int64,
	taskQueue *taskqueuepb.TaskQueue, identity string) *historypb.HistoryEvent {
	event, _, err := s.msBuilder.AddWorkflowTaskStartedEvent(scheduleID, uuid.New(), &workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addWorkflowTaskCompletedEvent(scheduleID, startedID int64, identity string) *historypb.HistoryEvent {
	event, err := s.msBuilder.AddWorkflowTaskCompletedEvent(scheduleID, startedID, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: identity,
	}, defaultHistoryMaxAutoResetPoints)
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addActivityTaskScheduledEvent(workflowTaskCompletedID int64, activityID, activityType,
	taskQueue string, input *commonpb.Payloads, timeout, queueTimeout, hearbeatTimeout time.Duration, retryPolicy *commonpb.RetryPolicy, namespace string) (*historypb.HistoryEvent,
	*persistencespb.ActivityInfo) {
	event, ai, err := s.msBuilder.AddActivityTaskScheduledEvent(workflowTaskCompletedID,
		&commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             activityID,
			ActivityType:           &commonpb.ActivityType{Name: activityType},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:                  input,
			ScheduleToCloseTimeout: &timeout,
			ScheduleToStartTimeout: &queueTimeout,
			HeartbeatTimeout:       &hearbeatTimeout,
			StartToCloseTimeout:    timestamp.DurationPtr(1 * time.Second),
			RetryPolicy:            retryPolicy,
			Namespace:              namespace,
		})
	s.Nil(err)
	return event, ai
}

func (s *historyBuilderSuite) addActivityTaskStartedEvent(scheduleID int64, taskQueue,
	identity string) *historypb.HistoryEvent {
	ai, _ := s.msBuilder.GetActivityInfo(scheduleID)
	event, err := s.msBuilder.AddActivityTaskStartedEvent(ai, scheduleID, uuid.New(), identity)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addActivityTaskCompletedEvent(scheduleID, startedID int64, result *commonpb.Payloads,
	identity string) *historypb.HistoryEvent {
	event, err := s.msBuilder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflowservice.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: identity,
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addActivityTaskFailedEvent(scheduleID, startedID int64, failure *failurepb.Failure, retryState enumspb.RetryState, identity string) *historypb.HistoryEvent {
	event, err := s.msBuilder.AddActivityTaskFailedEvent(scheduleID, startedID, failure, retryState, identity)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addMarkerRecordedEvent(workflowTaskCompletedEventID int64, markerName string, details map[string]*commonpb.Payloads, header *map[string]*commonpb.Payload) *historypb.HistoryEvent {
	fields := make(map[string]*commonpb.Payload)
	if header != nil {
		for name, value := range *header {
			fields[name] = value
		}
	}
	event, err := s.msBuilder.AddRecordMarkerEvent(workflowTaskCompletedEventID, &commandpb.RecordMarkerCommandAttributes{
		MarkerName: markerName,
		Details:    details,
		Header: &commonpb.Header{
			Fields: fields,
		},
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64, targetNamespace string, targetExecution commonpb.WorkflowExecution,
	childWorkflowOnly bool) *historypb.HistoryEvent {
	event, _, err := s.msBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		uuid.New(),
		&commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{
			Namespace:         targetNamespace,
			WorkflowId:        targetExecution.WorkflowId,
			RunId:             targetExecution.RunId,
			ChildWorkflowOnly: childWorkflowOnly,
		},
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addExternalWorkflowExecutionCancelRequested(
	initiatedID int64, namespace, workflowID, runID string) *historypb.HistoryEvent {

	event, err := s.msBuilder.AddExternalWorkflowExecutionCancelRequested(
		initiatedID, namespace, workflowID, runID,
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID, initiatedID int64,
	namespace, workflowID, runID string, cause enumspb.CancelExternalWorkflowExecutionFailedCause) *historypb.HistoryEvent {

	event, err := s.msBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID, initiatedID, namespace, workflowID, runID, cause,
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) validateWorkflowExecutionStartedEvent(event *historypb.HistoryEvent, workflowType string,
	taskQueue *taskqueuepb.TaskQueue, input *commonpb.Payloads, executionTimeout, runTimeout, taskTimeout time.Duration, identity string) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, event.EventType)
	s.Equal(common.FirstEventID, event.EventId)
	attributes := event.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowType, attributes.WorkflowType.Name)
	s.Equal(taskQueue, attributes.TaskQueue)
	s.Equal(input, attributes.Input)
	s.Equal(executionTimeout, timestamp.DurationValue(attributes.WorkflowExecutionTimeout))
	s.Equal(runTimeout, timestamp.DurationValue(attributes.WorkflowRunTimeout))
	s.Equal(taskTimeout, timestamp.DurationValue(attributes.WorkflowTaskTimeout))
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateWorkflowTaskScheduledEvent(di *workflowTaskInfo, eventID int64,
	taskQueue *taskqueuepb.TaskQueue, timeout time.Duration) {
	s.NotNil(di)
	s.Equal(eventID, di.ScheduleID)
	s.Equal(taskQueue, di.TaskQueue)
}

func (s *historyBuilderSuite) validateWorkflowTaskStartedEvent(event *historypb.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetWorkflowTaskStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateWorkflowTaskCompletedEvent(event *historypb.HistoryEvent, eventID,
	scheduleID, startedID int64, identity string) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetWorkflowTaskCompletedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(startedID, attributes.StartedEventId)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskScheduledEvent(event *historypb.HistoryEvent, eventID, workflowTaskID int64,
	activityID, activityType, taskQueue string, input *commonpb.Payloads, timeout, queueTimeout, hearbeatTimeout time.Duration, namespace string) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskScheduledEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowTaskID, attributes.WorkflowTaskCompletedEventId)
	s.Equal(activityID, attributes.ActivityId)
	s.Equal(activityType, attributes.ActivityType.Name)
	s.Equal(taskQueue, attributes.TaskQueue.Name)
	s.Equal(input, attributes.Input)
	s.Equal(timeout, timestamp.DurationValue(attributes.ScheduleToCloseTimeout))
	s.Equal(queueTimeout, timestamp.DurationValue(attributes.ScheduleToStartTimeout))
	s.Equal(hearbeatTimeout, timestamp.DurationValue(attributes.HeartbeatTimeout))
	s.Equal(namespace, attributes.Namespace)
}

func (s *historyBuilderSuite) validateActivityTaskStartedEvent(event *historypb.HistoryEvent, eventID, scheduleID int64,
	identity string, attempt int64, lastFailure *failurepb.Failure) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(identity, attributes.Identity)
	s.Equal(lastFailure, attributes.LastFailure)
}

func (s *historyBuilderSuite) validateTransientActivityTaskStartedEvent(event *historypb.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.Nil(event)
	ai, ok := s.msBuilder.GetPendingActivityInfos()[scheduleID]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(scheduleID, ai.ScheduleId)
	s.Equal(identity, ai.StartedIdentity)
}

func (s *historyBuilderSuite) validateActivityTaskCompletedEvent(event *historypb.HistoryEvent, eventID,
	scheduleID, startedID int64, result *commonpb.Payloads, identity string) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskCompletedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(startedID, attributes.StartedEventId)
	s.Equal(result, attributes.Result)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskFailedEvent(event *historypb.HistoryEvent, eventID,
	scheduleID, startedID int64, failure *failurepb.Failure, retryState enumspb.RetryState, identity string) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskFailedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(startedID, attributes.StartedEventId)
	s.Equal(failure, attributes.Failure)
	s.Equal(retryState, attributes.RetryState)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateMarkerRecordedEvent(
	event *historypb.HistoryEvent, eventID, workflowTaskCompletedEventID int64,
	markerName string, details map[string]*commonpb.Payloads, header *map[string]*commonpb.Payload) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_MARKER_RECORDED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetMarkerRecordedEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowTaskCompletedEventID, attributes.WorkflowTaskCompletedEventId)
	s.Equal(markerName, attributes.GetMarkerName())
	s.Equal(details, attributes.GetDetails())
	if header != nil {
		for name, value := range attributes.Header.Fields {
			s.Equal((*header)[name], value)
		}
	} else {
		s.Nil(attributes.Header)
	}
}

func (s *historyBuilderSuite) validateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	event *historypb.HistoryEvent, eventID, workflowTaskCompletedEventID int64,
	namespace string, execution commonpb.WorkflowExecution, childWorkflowOnly bool) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowTaskCompletedEventID, attributes.WorkflowTaskCompletedEventId)
	s.Equal(namespace, attributes.GetNamespace())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
	s.Equal(childWorkflowOnly, attributes.ChildWorkflowOnly)
}

func (s *historyBuilderSuite) validateExternalWorkflowExecutionCancelRequested(
	event *historypb.HistoryEvent, eventID, initiatedEventID int64,
	namespace string, execution commonpb.WorkflowExecution) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetExternalWorkflowExecutionCancelRequestedEventAttributes()
	s.NotNil(attributes)
	s.Equal(initiatedEventID, attributes.GetInitiatedEventId())
	s.Equal(namespace, attributes.GetNamespace())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
}

func (s *historyBuilderSuite) validateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *historypb.HistoryEvent, eventID, workflowTaskCompletedEventID, initiatedEventID int64,
	namespace string, execution commonpb.WorkflowExecution, cause enumspb.CancelExternalWorkflowExecutionFailedCause) {
	s.NotNil(event)
	s.Equal(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowTaskCompletedEventID, attributes.GetWorkflowTaskCompletedEventId())
	s.Equal(initiatedEventID, attributes.GetInitiatedEventId())
	s.Equal(namespace, attributes.GetNamespace())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
	s.Equal(cause, attributes.Cause)
}

func (s *historyBuilderSuite) printHistory() string {
	return s.builder.GetHistory().String()
}
