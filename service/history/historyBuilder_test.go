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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/payload"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
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
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.logger = log.NewNoop()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.namespaceID = testNamespaceID
	s.namespaceEntry = cache.NewLocalNamespaceCacheEntryForTest(&persistenceblobs.NamespaceInfo{Id: primitives.MustParseUUID(s.namespaceID)}, &persistenceblobs.NamespaceConfig{}, "", nil)

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
	id := "dynamic-historybuilder-success-test-workflow-id"
	rid := "dynamic-historybuilder-success-test-run-id"
	wt := "dynamic-historybuilder-success-type"
	tl := "dynamic-historybuilder-success-tasklist"
	identity := "dynamic-historybuilder-success-worker"
	input := payload.EncodeString("dynamic-historybuilder-success-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	di := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di1, decisionRunning1 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning1)
	s.NotNil(di1)
	decisionStartedID1 := di1.StartedID
	s.Equal(int64(3), decisionStartedID1)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionContext := []byte("dynamic-historybuilder-success-context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.getNextEventID())
	_, decisionRunning2 := s.msBuilder.GetDecisionInfo(2)
	s.False(decisionRunning2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityTaskList := "dynamic-historybuilder-success-activity-tasklist"
	activityTimeout := int32(60)
	queueTimeout := int32(20)
	hearbeatTimeout := int32(10)

	activity1ID := "activity1"
	activity1Type := "dynamic-historybuilder-success-activity1-type"
	activity1Input := payload.EncodeString("dynamic-historybuilder-success-activity1-input")
	activity1Result := payload.EncodeString("dynamic-historybuilder-success-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout, nil)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(common.EmptyEventID, ai0.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2ID := "activity2"
	activity2Type := "dynamic-historybuilder-success-activity2-type"
	activity2Input := payload.EncodeString("dynamic-historybuilder-success-activity2-input")
	activity2Reason := "dynamic-historybuilder-success-activity2-failed"
	activity2Details := payload.EncodeString("dynamic-historybuilder-success-activity2-callstack")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout, nil)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(common.EmptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity3ID := "activity3"
	activity3Type := "dynamic-historybuilder-success-activity3-type"
	activity3Input := payload.EncodeString("dynamic-historybuilder-success-activity3-input")
	activity3RetryPolicy := &commonpb.RetryPolicy{
		InitialIntervalInSeconds:    1,
		MaximumAttempts:             3,
		MaximumIntervalInSeconds:    1,
		NonRetriableErrorReasons:    []string{"bad-bug"},
		BackoffCoefficient:          1,
		ExpirationIntervalInSeconds: 100,
	}
	activity3ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity3ID, activity3Type,
		activityTaskList, activity3Input, activityTimeout, queueTimeout, hearbeatTimeout, activity3RetryPolicy)
	s.validateActivityTaskScheduledEvent(activity3ScheduledEvent, 7, 4, activity3ID, activity3Type,
		activityTaskList, activity3Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(8), s.getNextEventID())
	ai2, activity3Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity3Running0)
	s.Equal(common.EmptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, common.BufferedEventID, 5, identity,
		0, "", nil)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activityStartedEvent, 8, 5, identity,
		0, "", nil)
	s.Equal(int64(9), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(8), ai3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 8, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, common.BufferedEventID, 5, 8, activity1Result,
		identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 9, 5, 8, activity1Result,
		identity)
	s.Equal(int64(10), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	di2 := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di2, 10, tl, taskTimeout)
	s.Equal(int64(11), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetDecisionInfo(10)
	s.True(decisionRunning3)
	s.Equal(common.EmptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, common.BufferedEventID, 6, identity,
		0, "", nil)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activity2StartedEvent, 11, 6, identity,
		0, "", nil)
	s.Equal(int64(12), s.getNextEventID())
	ai4, activity2Running1 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running1)
	s.Equal(int64(11), ai4.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2FailedEvent := s.addActivityTaskFailedEvent(6, 11, activity2Reason, activity2Details,
		identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, common.BufferedEventID, 6, 11, activity2Reason,
		activity2Details, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskFailedEvent(activity2FailedEvent, 12, 6, 11, activity2Reason,
		activity2Details, identity)
	s.Equal(int64(13), s.getNextEventID())
	_, activity2Running3 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running3)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity3StartedEvent := s.addActivityTaskStartedEvent(7, activityTaskList, identity)
	s.validateTransientActivityTaskStartedEvent(activity3StartedEvent, common.TransientEventID, 7, identity)
	s.Equal(int64(13), s.getNextEventID())
	ai5, activity3Running1 := s.msBuilder.GetActivityInfo(7)
	s.True(activity3Running1)
	s.Equal(common.TransientEventID, ai5.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity3Reason := "dynamic-historybuilder-success-activity3-failed"
	activity3Details := payload.EncodeString("dynamic-historybuilder-success-activity3-callstack")
	s.msBuilder.RetryActivity(ai5, activity3Reason, activity3Details)
	ai6, activity3Running2 := s.msBuilder.GetActivityInfo(7)
	s.Equal(activity3Reason, ai6.LastFailureReason)
	s.Equal(activity3Details, ai6.LastFailureDetails)
	s.True(activity3Running2)

	activity3StartedEvent2 := s.addActivityTaskStartedEvent(7, activityTaskList, identity)
	s.validateTransientActivityTaskStartedEvent(activity3StartedEvent2, common.TransientEventID, 7, identity)
	s.Equal(int64(13), s.getNextEventID())
	ai7, activity3Running3 := s.msBuilder.GetActivityInfo(7)
	s.True(activity3Running3)
	s.Equal(common.TransientEventID, ai7.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity3Result := payload.EncodeString("dynamic-historybuilder-success-activity1-result")
	activity3CompletedEvent := s.addActivityTaskCompletedEvent(7, common.TransientEventID, activity3Result, identity)
	s.validateActivityTaskCompletedEvent(activity3CompletedEvent, common.BufferedEventID, 7, common.TransientEventID,
		activity3Result, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activity3CompletedEvent, 14, 7, 13, activity3Result, identity)
	s.Equal(int64(15), s.getNextEventID())
	ai8, activity3Running4 := s.msBuilder.GetActivityInfo(7)
	s.Nil(ai8)
	s.False(activity3Running4)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// Verify the last ActivityTaskStartedEvent which should show the error from the first attempt
	historyEvents := s.msBuilder.GetHistoryBuilder().GetHistory().GetEvents()
	s.Len(historyEvents, 14)
	s.validateActivityTaskStartedEvent(historyEvents[12], 13, 7, identity, 1, activity3Reason, activity3Details)

	markerDetails := payload.EncodeString("dynamic-historybuilder-success-marker-details")
	markerHeaderField1 := []byte("dynamic-historybuilder-success-marker-header1")
	markerHeaderField2 := []byte("dynamic-historybuilder-success-marker-header2")
	markerHeader := map[string][]byte{
		"name1": markerHeaderField1,
		"name2": markerHeaderField2,
	}
	markerEvent := s.addMarkerRecordedEvent(4, "testMarker", markerDetails,
		&markerHeader)
	s.validateMarkerRecordedEvent(markerEvent, 15, 4, "testMarker", markerDetails, &markerHeader)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.Equal(int64(16), s.getNextEventID())
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowStartFailures() {
	id := "historybuilder-workflowstart-failures-test-workflow-id"
	rid := "historybuilder-workflowstart-failures-test-run-id"
	wt := "historybuilder-workflowstart-failures-type"
	tl := "historybuilder-workflowstart-failures-tasklist"
	identity := "historybuilder-workflowstart-failures-worker"
	input := payload.EncodeString("historybuilder-workflowstart-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	di := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	_, err := s.msBuilder.AddWorkflowExecutionStartedEvent(
		we,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId: s.namespaceID,
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:                          we.WorkflowId,
				WorkflowType:                        &commonpb.WorkflowType{Name: wt},
				TaskList:                            &tasklistpb.TaskList{Name: tl},
				Input:                               input,
				ExecutionStartToCloseTimeoutSeconds: execTimeout,
				TaskStartToCloseTimeoutSeconds:      taskTimeout,
				Identity:                            identity,
			},
		})
	s.NotNil(err)

	s.Equal(int64(3), s.getNextEventID(), s.printHistory())
	di1, decisionRunning1 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning1)
	s.Equal(common.EmptyEventID, di1.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderDecisionScheduledFailures() {
	id := "historybuilder-decisionscheduled-failures-test-workflow-id"
	rid := "historybuilder-decisionscheduled-failures-test-run-id"
	wt := "historybuilder-decisionscheduled-failures-type"
	tl := "historybuilder-decisionscheduled-failures-tasklist"
	identity := "historybuilder-decisionscheduled-failures-worker"
	input := payload.EncodeString("historybuilder-decisionscheduled-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	di := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	_, err := s.msBuilder.AddDecisionTaskScheduledEvent(false)
	s.NotNil(err)
	s.Equal(int64(3), s.getNextEventID())
	di1, decisionRunning1 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning1)
	s.Equal(common.EmptyEventID, di1.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderDecisionStartedFailures() {
	id := "historybuilder-decisionstarted-failures-test-workflow-id"
	rid := "historybuilder-decisionstarted-failures-test-run-id"
	wt := "historybuilder-decisionstarted-failures-type"
	tl := "historybuilder-decisionstarted-failures-tasklist"
	identity := "historybuilder-decisionstarted-failures-worker"
	input := payload.EncodeString("historybuilder-decisionstarted-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	_, _, err := s.msBuilder.AddDecisionTaskStartedEvent(2, uuid.New(), &workflowservice.PollForDecisionTaskRequest{
		TaskList: &tasklistpb.TaskList{Name: tl},
		Identity: identity,
	})
	s.NotNil(err)
	s.Equal(int64(2), s.getNextEventID())
	_, decisionRunning1 := s.msBuilder.GetDecisionInfo(2)
	s.False(decisionRunning1)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	di := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	_, _, err = s.msBuilder.AddDecisionTaskStartedEvent(100, uuid.New(), &workflowservice.PollForDecisionTaskRequest{
		TaskList: &tasklistpb.TaskList{Name: tl},
		Identity: identity,
	})
	s.NotNil(err)
	s.Equal(int64(3), s.getNextEventID())
	di2, decisionRunning2 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning2)
	s.Equal(common.EmptyEventID, di2.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent2 := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent2, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning3)
	s.Equal(int64(3), di3.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderFlushBufferedEvents() {
	id := "flush-buffered-events-test-workflow-id"
	rid := "flush-buffered-events-test-run-id"
	wt := "flush-buffered-events-type"
	tl := "flush-buffered-events-tasklist"
	identity := "flush-buffered-events-worker"
	input := payload.EncodeString("flush-buffered-events-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := executionpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      rid,
	}

	// 1 execution started
	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	// 2 decision scheduled
	di := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning0)
	s.Equal(common.EmptyEventID, di0.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	// 3 decision started
	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di1, decisionRunning1 := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning1)
	s.NotNil(di1)
	decisionStartedID1 := di1.StartedID
	s.Equal(int64(3), decisionStartedID1)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	// 4 decision completed
	decisionContext := []byte("flush-buffered-events-context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.getNextEventID())
	_, decisionRunning2 := s.msBuilder.GetDecisionInfo(2)
	s.False(decisionRunning2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityTaskList := "flush-buffered-events-activity-tasklist"
	activityTimeout := int32(60)
	queueTimeout := int32(20)
	hearbeatTimeout := int32(10)

	// 5 activity1 scheduled
	activity1ID := "activity1"
	activity1Type := "flush-buffered-events-activity1-type"
	activity1Input := payload.EncodeString("flush-buffered-events-activity1-input")
	activity1Result := payload.EncodeString("flush-buffered-events-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout, nil)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(common.EmptyEventID, ai0.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 6 activity 2 scheduled
	activity2ID := "activity2"
	activity2Type := "flush-buffered-events-activity2-type"
	activity2Input := payload.EncodeString("flush-buffered-events-activity2-input")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout, nil)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(common.EmptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 7 activity1 started
	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, common.BufferedEventID, 5, identity,
		0, "", nil)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity,
		0, "", nil)
	s.Equal(int64(8), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(7), ai3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 8 activity1 completed
	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 7, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, common.BufferedEventID, 5, 7, activity1Result, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 8, 5, 7, activity1Result, identity)
	s.Equal(int64(9), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 9 decision2 scheduled
	di2 := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di2, 9, tl, taskTimeout)
	s.Equal(int64(10), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetDecisionInfo(9)
	s.True(decisionRunning3)
	s.Equal(common.EmptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 10 decision2 started
	decision2StartedEvent := s.addDecisionTaskStartedEvent(9, tl, identity)
	s.validateDecisionTaskStartedEvent(decision2StartedEvent, 10, 9, identity)
	s.Equal(int64(11), s.getNextEventID())
	di2, decision2Running := s.msBuilder.GetDecisionInfo(9)
	s.True(decision2Running)
	s.NotNil(di2)
	decision2StartedID := di2.StartedID
	s.Equal(int64(10), decision2StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 11 (buffered) activity2 started
	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, common.BufferedEventID, 6, identity,
		0, "", nil)
	s.Equal(int64(11), s.getNextEventID())
	ai4, activity2Running := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running)
	s.Equal(common.BufferedEventID, ai4.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 12 (buffered) activity2 failed
	activity2Reason := "flush-buffered-events-activity2-failed"
	activity2Details := payload.EncodeString("flush-buffered-events-activity2-callstack")
	activity2FailedEvent := s.addActivityTaskFailedEvent(6, common.BufferedEventID, activity2Reason, activity2Details, identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, common.BufferedEventID, 6, common.BufferedEventID, activity2Reason,
		activity2Details, identity)
	s.Equal(int64(11), s.getNextEventID())
	_, activity2Running2 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 13 (eventId will be 11) decision completed
	decision2Context := []byte("flush-buffered-events-context")
	decision2CompletedEvent := s.addDecisionTaskCompletedEvent(9, 10, decision2Context, identity)
	s.validateDecisionTaskCompletedEvent(decision2CompletedEvent, 11, 9, 10, decision2Context, identity)
	s.Equal(int64(11), decision2CompletedEvent.GetEventId())
	s.Equal(int64(12), s.getNextEventID())
	_, decision2Running2 := s.msBuilder.GetDecisionInfo(2)
	s.False(decision2Running2)
	s.Equal(int64(10), s.getPreviousDecisionStartedEventID())

	// flush buffered events. 12: Activity2Started, 13: Activity2Failed
	s.NoError(s.msBuilder.FlushBufferedEvents())
	s.Equal(int64(14), s.getNextEventID())
	activity2StartedEvent2 := s.msBuilder.GetHistoryBuilder().history[11]
	s.Equal(int64(12), activity2StartedEvent2.GetEventId())
	s.Equal(eventpb.EventType_ActivityTaskStarted, activity2StartedEvent2.GetEventType())

	activity2FailedEvent2 := s.msBuilder.GetHistoryBuilder().history[12]
	s.Equal(int64(13), activity2FailedEvent2.GetEventId())
	s.Equal(eventpb.EventType_ActivityTaskFailed, activity2FailedEvent2.GetEventType())
	s.Equal(int64(12), activity2FailedEvent2.GetActivityTaskFailedEventAttributes().GetStartedEventId())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowCancellationRequested() {
	workflowType := "some random workflow type"
	tasklist := "some random tasklist"
	identity := "some random identity"
	input := payload.EncodeString("some random workflow input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	workflowExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(
		workflowExecution, workflowType, tasklist, input, execTimeout, taskTimeout, identity,
	)
	s.validateWorkflowExecutionStartedEvent(
		workflowStartedEvent, workflowType, tasklist, input, execTimeout, taskTimeout, identity,
	)
	s.Equal(int64(2), s.getNextEventID())

	decisionInfo := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionInfo, 2, tasklist, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	decisionInfo, decisionRunning := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning)
	s.NotNil(decisionInfo)
	s.Equal(int64(2), decisionInfo.ScheduleID)
	s.Equal(common.EmptyEventID, decisionInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tasklist, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	decisionInfo, decisionRunning = s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning)
	s.NotNil(decisionInfo)
	s.Equal(int64(2), decisionInfo.ScheduleID)
	s.Equal(int64(3), decisionInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionContext := []byte("some random decision context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.getNextEventID())
	decisionInfo, decisionRunning = s.msBuilder.GetDecisionInfo(2)
	s.False(decisionRunning)
	s.Nil(decisionInfo)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	targetNamespace := "some random target namespace"
	targetExecution := executionpb.WorkflowExecution{
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
	tasklist := "some random tasklist"
	identity := "some random identity"
	input := payload.EncodeString("some random workflow input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	workflowExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(
		workflowExecution, workflowType, tasklist, input, execTimeout, taskTimeout, identity,
	)
	s.validateWorkflowExecutionStartedEvent(
		workflowStartedEvent, workflowType, tasklist, input, execTimeout, taskTimeout, identity,
	)
	s.Equal(int64(2), s.getNextEventID())

	decisionInfo := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionInfo, 2, tasklist, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	decisionInfo, decisionRunning := s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning)
	s.NotNil(decisionInfo)
	s.Equal(int64(2), decisionInfo.ScheduleID)
	s.Equal(common.EmptyEventID, decisionInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tasklist, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	decisionInfo, decisionRunning = s.msBuilder.GetDecisionInfo(2)
	s.True(decisionRunning)
	s.NotNil(decisionInfo)
	s.Equal(int64(2), decisionInfo.ScheduleID)
	s.Equal(int64(3), decisionInfo.StartedID)
	s.Equal(common.EmptyEventID, s.getPreviousDecisionStartedEventID())

	decisionContext := []byte("some random decision context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.getNextEventID())
	decisionInfo, decisionRunning = s.msBuilder.GetDecisionInfo(2)
	s.False(decisionRunning)
	s.Nil(decisionInfo)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	targetNamespace := "some random target namespace"
	targetExecution := executionpb.WorkflowExecution{
		WorkflowId: "some random target workflow ID",
		RunId:      "some random target run ID",
	}
	cancellationChildWorkflowOnly := true
	cancellationFailedCause := eventpb.WorkflowExecutionFailedCause(59)
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
	return s.msBuilder.GetExecutionInfo().NextEventID
}

func (s *historyBuilderSuite) getPreviousDecisionStartedEventID() int64 {
	return s.msBuilder.GetExecutionInfo().LastProcessedEvent
}

func (s *historyBuilderSuite) addWorkflowExecutionStartedEvent(we executionpb.WorkflowExecution, workflowType,
	taskList string, input *commonpb.Payload, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *eventpb.HistoryEvent {

	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:                          we.WorkflowId,
		WorkflowType:                        &commonpb.WorkflowType{Name: workflowType},
		TaskList:                            &tasklistpb.TaskList{Name: taskList},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: executionStartToCloseTimeout,
		TaskStartToCloseTimeoutSeconds:      taskStartToCloseTimeout,
		Identity:                            identity,
	}

	event, err := s.msBuilder.AddWorkflowExecutionStartedEvent(
		we,
		&historyservice.StartWorkflowExecutionRequest{
			NamespaceId:  s.namespaceID,
			StartRequest: request,
		},
	)
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addDecisionTaskScheduledEvent() *decisionInfo {
	di, err := s.msBuilder.AddDecisionTaskScheduledEvent(false)
	s.Nil(err)
	return di
}

func (s *historyBuilderSuite) addDecisionTaskStartedEvent(scheduleID int64,
	taskList, identity string) *eventpb.HistoryEvent {
	event, _, err := s.msBuilder.AddDecisionTaskStartedEvent(scheduleID, uuid.New(), &workflowservice.PollForDecisionTaskRequest{
		TaskList: &tasklistpb.TaskList{Name: taskList},
		Identity: identity,
	})
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addDecisionTaskCompletedEvent(scheduleID, startedID int64, context []byte,
	identity string) *eventpb.HistoryEvent {
	event, err := s.msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflowservice.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         identity,
	}, defaultHistoryMaxAutoResetPoints)
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addActivityTaskScheduledEvent(decisionCompletedID int64, activityID, activityType,
	taskList string, input *commonpb.Payload, timeout, queueTimeout, hearbeatTimeout int32, retryPolicy *commonpb.RetryPolicy) (*eventpb.HistoryEvent,
	*persistence.ActivityInfo) {
	event, ai, err := s.msBuilder.AddActivityTaskScheduledEvent(decisionCompletedID,
		&decisionpb.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    activityID,
			ActivityType:                  &commonpb.ActivityType{Name: activityType},
			TaskList:                      &tasklistpb.TaskList{Name: taskList},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: timeout,
			ScheduleToStartTimeoutSeconds: queueTimeout,
			HeartbeatTimeoutSeconds:       hearbeatTimeout,
			StartToCloseTimeoutSeconds:    1,
			RetryPolicy:                   retryPolicy,
		})
	s.Nil(err)
	return event, ai
}

func (s *historyBuilderSuite) addActivityTaskStartedEvent(scheduleID int64, taskList,
	identity string) *eventpb.HistoryEvent {
	ai, _ := s.msBuilder.GetActivityInfo(scheduleID)
	event, err := s.msBuilder.AddActivityTaskStartedEvent(ai, scheduleID, uuid.New(), identity)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addActivityTaskCompletedEvent(scheduleID, startedID int64, result *commonpb.Payload,
	identity string) *eventpb.HistoryEvent {
	event, err := s.msBuilder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflowservice.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: identity,
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addActivityTaskFailedEvent(scheduleID, startedID int64, reason string, details *commonpb.Payload,
	identity string) *eventpb.HistoryEvent {
	event, err := s.msBuilder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflowservice.RespondActivityTaskFailedRequest{
		Reason:   reason,
		Details:  details,
		Identity: identity,
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addMarkerRecordedEvent(decisionCompletedEventID int64, markerName string, details *commonpb.Payload, header *map[string][]byte) *eventpb.HistoryEvent {
	fields := make(map[string][]byte)
	if header != nil {
		for name, value := range *header {
			fields[name] = value
		}
	}
	event, err := s.msBuilder.AddRecordMarkerEvent(decisionCompletedEventID, &decisionpb.RecordMarkerDecisionAttributes{
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
	decisionCompletedEventID int64, targetNamespace string, targetExecution executionpb.WorkflowExecution,
	childWorkflowOnly bool) *eventpb.HistoryEvent {
	event, _, err := s.msBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		decisionCompletedEventID,
		uuid.New(),
		&decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes{
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
	initiatedID int64, namespace, workflowID, runID string) *eventpb.HistoryEvent {

	event, err := s.msBuilder.AddExternalWorkflowExecutionCancelRequested(
		initiatedID, namespace, workflowID, runID,
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addRequestCancelExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID, initiatedID int64,
	namespace, workflowID, runID string, cause eventpb.WorkflowExecutionFailedCause) *eventpb.HistoryEvent {

	event, err := s.msBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		decisionTaskCompletedEventID, initiatedID, namespace, workflowID, runID, cause,
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) validateWorkflowExecutionStartedEvent(event *eventpb.HistoryEvent, workflowType,
	taskList string, input *commonpb.Payload, executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_WorkflowExecutionStarted, event.EventType)
	s.Equal(common.FirstEventID, event.EventId)
	attributes := event.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowType, attributes.WorkflowType.Name)
	s.Equal(taskList, attributes.TaskList.Name)
	s.Equal(input, attributes.Input)
	s.Equal(executionStartToCloseTimeout, attributes.ExecutionStartToCloseTimeoutSeconds)
	s.Equal(taskStartToCloseTimeout, attributes.TaskStartToCloseTimeoutSeconds)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateDecisionTaskScheduledEvent(di *decisionInfo, eventID int64,
	taskList string, timeout int32) {
	s.NotNil(di)
	s.Equal(eventID, di.ScheduleID)
	s.Equal(taskList, di.TaskList)
}

func (s *historyBuilderSuite) validateDecisionTaskStartedEvent(event *eventpb.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_DecisionTaskStarted, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetDecisionTaskStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateDecisionTaskCompletedEvent(event *eventpb.HistoryEvent, eventID,
	scheduleID, startedID int64, context []byte, identity string) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_DecisionTaskCompleted, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetDecisionTaskCompletedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(startedID, attributes.StartedEventId)
	s.Equal(context, attributes.ExecutionContext)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskScheduledEvent(event *eventpb.HistoryEvent, eventID, decisionID int64,
	activityID, activityType, taskList string, input *commonpb.Payload, timeout, queueTimeout, hearbeatTimeout int32) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_ActivityTaskScheduled, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskScheduledEventAttributes()
	s.NotNil(attributes)
	s.Equal(decisionID, attributes.DecisionTaskCompletedEventId)
	s.Equal(activityID, attributes.ActivityId)
	s.Equal(activityType, attributes.ActivityType.Name)
	s.Equal(taskList, attributes.TaskList.Name)
	s.Equal(input, attributes.Input)
	s.Equal(timeout, attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(queueTimeout, attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(hearbeatTimeout, attributes.HeartbeatTimeoutSeconds)
}

func (s *historyBuilderSuite) validateActivityTaskStartedEvent(event *eventpb.HistoryEvent, eventID, scheduleID int64,
	identity string, attempt int64, lastFailureReason string, lastFailureDetails *commonpb.Payload) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_ActivityTaskStarted, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(identity, attributes.Identity)
	s.Equal(lastFailureReason, attributes.LastFailureReason)
	s.Equal(lastFailureDetails, attributes.LastFailureDetails)
}

func (s *historyBuilderSuite) validateTransientActivityTaskStartedEvent(event *eventpb.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.Nil(event)
	ai, ok := s.msBuilder.GetPendingActivityInfos()[scheduleID]
	s.True(ok)
	s.NotNil(ai)
	s.Equal(scheduleID, ai.ScheduleID)
	s.Equal(identity, ai.StartedIdentity)
}

func (s *historyBuilderSuite) validateActivityTaskCompletedEvent(event *eventpb.HistoryEvent, eventID,
	scheduleID, startedID int64, result *commonpb.Payload, identity string) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_ActivityTaskCompleted, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskCompletedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(startedID, attributes.StartedEventId)
	s.Equal(result, attributes.Result)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskFailedEvent(event *eventpb.HistoryEvent, eventID,
	scheduleID, startedID int64, reason string, details *commonpb.Payload, identity string) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_ActivityTaskFailed, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetActivityTaskFailedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.ScheduledEventId)
	s.Equal(startedID, attributes.StartedEventId)
	s.Equal(reason, attributes.Reason)
	s.Equal(details, attributes.Details)
	s.Equal(identity, attributes.Identity)
}

func (s *historyBuilderSuite) validateMarkerRecordedEvent(
	event *eventpb.HistoryEvent, eventID, decisionTaskCompletedEventID int64,
	markerName string, details *commonpb.Payload, header *map[string][]byte) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_MarkerRecorded, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetMarkerRecordedEventAttributes()
	s.NotNil(attributes)
	s.Equal(decisionTaskCompletedEventID, attributes.DecisionTaskCompletedEventId)
	s.Equal(markerName, attributes.GetMarkerName())
	s.Equal(details, attributes.Details)
	if header != nil {
		for name, value := range attributes.Header.Fields {
			s.Equal((*header)[name], value)
		}
	} else {
		s.Nil(attributes.Header)
	}
}

func (s *historyBuilderSuite) validateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	event *eventpb.HistoryEvent, eventID, decisionTaskCompletedEventID int64,
	namespace string, execution executionpb.WorkflowExecution, childWorkflowOnly bool) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_RequestCancelExternalWorkflowExecutionInitiated, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	s.NotNil(attributes)
	s.Equal(decisionTaskCompletedEventID, attributes.DecisionTaskCompletedEventId)
	s.Equal(namespace, attributes.GetNamespace())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
	s.Equal(childWorkflowOnly, attributes.ChildWorkflowOnly)
}

func (s *historyBuilderSuite) validateExternalWorkflowExecutionCancelRequested(
	event *eventpb.HistoryEvent, eventID, initiatedEventID int64,
	namespace string, execution executionpb.WorkflowExecution) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_ExternalWorkflowExecutionCancelRequested, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetExternalWorkflowExecutionCancelRequestedEventAttributes()
	s.NotNil(attributes)
	s.Equal(initiatedEventID, attributes.GetInitiatedEventId())
	s.Equal(namespace, attributes.GetNamespace())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
}

func (s *historyBuilderSuite) validateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *eventpb.HistoryEvent, eventID, decisionTaskCompletedEventID, initiatedEventID int64,
	namespace string, execution executionpb.WorkflowExecution, cause eventpb.WorkflowExecutionFailedCause) {
	s.NotNil(event)
	s.Equal(eventpb.EventType_RequestCancelExternalWorkflowExecutionFailed, event.EventType)
	s.Equal(eventID, event.EventId)
	attributes := event.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()
	s.NotNil(attributes)
	s.Equal(decisionTaskCompletedEventID, attributes.GetDecisionTaskCompletedEventId())
	s.Equal(initiatedEventID, attributes.GetInitiatedEventId())
	s.Equal(namespace, attributes.GetNamespace())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
	s.Equal(cause, attributes.Cause)
}

func (s *historyBuilderSuite) printHistory() string {
	return s.builder.GetHistory().String()
}
