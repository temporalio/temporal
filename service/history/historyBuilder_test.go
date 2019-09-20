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

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyBuilderSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error

		mockDomainCache *cache.DomainCacheMock

		*require.Assertions
		domainID        string
		domainEntry     *cache.DomainCacheEntry
		msBuilder       mutableState
		builder         *historyBuilder
		mockShard       *shardContextImpl
		mockEventsCache *MockEventsCache
		logger          log.Logger
	}
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historyBuilderSuite)
	suite.Run(t, s)
}

func (s *historyBuilderSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockDomainCache = &cache.DomainCacheMock{}
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.domainID = testDomainID
	s.domainEntry = cache.NewLocalDomainCacheEntryForTest(&persistence.DomainInfo{ID: s.domainID}, &persistence.DomainConfig{}, "", nil)
	s.mockShard = &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		timeSource:                clock.NewRealTimeSource(),
		domainCache:               s.mockDomainCache,
	}
	s.mockEventsCache = &MockEventsCache{}
	s.msBuilder = newMutableStateBuilder(s.mockShard, s.mockEventsCache,
		s.logger, testLocalDomainEntry)
	s.builder = newHistoryBuilder(s.msBuilder, s.logger)

	s.mockDomainCache.On("GetDomain", mock.Anything).Return(s.domainEntry, nil).Maybe()
}

func (s *historyBuilderSuite) TestHistoryBuilderDynamicSuccess() {
	id := "dynamic-historybuilder-success-test-workflow-id"
	rid := "dynamic-historybuilder-success-test-run-id"
	wt := "dynamic-historybuilder-success-type"
	tl := "dynamic-historybuilder-success-tasklist"
	identity := "dynamic-historybuilder-success-worker"
	input := []byte("dynamic-historybuilder-success-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
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
	activity1Input := []byte("dynamic-historybuilder-success-activity1-input")
	activity1Result := []byte("dynamic-historybuilder-success-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(common.EmptyEventID, ai0.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2ID := "activity2"
	activity2Type := "dynamic-historybuilder-success-activity2-type"
	activity2Input := []byte("dynamic-historybuilder-success-activity2-input")
	activity2Reason := "dynamic-historybuilder-success-activity2-failed"
	activity2Details := []byte("dynamic-historybuilder-success-activity2-callstack")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(common.EmptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, common.BufferedEventID, 5, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity)
	s.Equal(int64(8), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(7), ai3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 7, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, common.BufferedEventID, 5, 7, activity1Result,
		identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 8, 5, 7, activity1Result,
		identity)
	s.Equal(int64(9), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	di2 := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(di2, 9, tl, taskTimeout)
	s.Equal(int64(10), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetDecisionInfo(9)
	s.True(decisionRunning3)
	s.Equal(common.EmptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, common.BufferedEventID, 6, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activity2StartedEvent, 10, 6, identity)
	s.Equal(int64(11), s.getNextEventID())
	ai4, activity2Running1 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running1)
	s.Equal(int64(10), ai4.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2FailedEvent := s.addActivityTaskFailedEvent(6, 10, activity2Reason, activity2Details,
		identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, common.BufferedEventID, 6, 10, activity2Reason,
		activity2Details, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskFailedEvent(activity2FailedEvent, 11, 6, 10, activity2Reason,
		activity2Details, identity)
	s.Equal(int64(12), s.getNextEventID())
	_, activity2Running2 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	markerDetails := []byte("dynamic-historybuilder-success-marker-details")
	markerHeaderField1 := []byte("dynamic-historybuilder-success-marker-header1")
	markerHeaderField2 := []byte("dynamic-historybuilder-success-marker-header2")
	markerHeader := map[string][]byte{
		"name1": markerHeaderField1,
		"name2": markerHeaderField2,
	}
	markerEvent := s.addMarkerRecordedEvent(4, "testMarker", markerDetails,
		&markerHeader)
	s.validateMarkerRecordedEvent(markerEvent, 12, 4, "testMarker", markerDetails, &markerHeader)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.Equal(int64(13), s.getNextEventID())
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowStartFailures() {
	id := "historybuilder-workflowstart-failures-test-workflow-id"
	rid := "historybuilder-workflowstart-failures-test-run-id"
	wt := "historybuilder-workflowstart-failures-type"
	tl := "historybuilder-workflowstart-failures-tasklist"
	identity := "historybuilder-workflowstart-failures-worker"
	input := []byte("historybuilder-workflowstart-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
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
		&history.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(s.domainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				WorkflowId:                          common.StringPtr(*we.WorkflowId),
				WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(wt)},
				TaskList:                            &workflow.TaskList{Name: common.StringPtr(tl)},
				Input:                               input,
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(execTimeout),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskTimeout),
				Identity:                            common.StringPtr(identity),
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
	input := []byte("historybuilder-decisionscheduled-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
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
	input := []byte("historybuilder-decisionstarted-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	_, _, err := s.msBuilder.AddDecisionTaskStartedEvent(2, uuid.New(), &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(tl)},
		Identity: common.StringPtr(identity),
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

	_, _, err = s.msBuilder.AddDecisionTaskStartedEvent(100, uuid.New(), &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(tl)},
		Identity: common.StringPtr(identity),
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
	input := []byte("flush-buffered-events-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
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
	activity1Input := []byte("flush-buffered-events-activity1-input")
	activity1Result := []byte("flush-buffered-events-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
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
	activity2Input := []byte("flush-buffered-events-activity2-input")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(common.EmptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 7 activity1 started
	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, common.BufferedEventID, 5, identity)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity)
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
	s.validateActivityTaskStartedEvent(activity2StartedEvent, common.BufferedEventID, 6, identity)
	s.Equal(int64(11), s.getNextEventID())
	ai4, activity2Running := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running)
	s.Equal(common.BufferedEventID, ai4.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 12 (buffered) activity2 failed
	activity2Reason := "flush-buffered-events-activity2-failed"
	activity2Details := []byte("flush-buffered-events-activity2-callstack")
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
	s.msBuilder.FlushBufferedEvents()
	s.Equal(int64(14), s.getNextEventID())
	activity2StartedEvent2 := s.msBuilder.GetHistoryBuilder().history[11]
	s.Equal(int64(12), activity2StartedEvent2.GetEventId())
	s.Equal(workflow.EventTypeActivityTaskStarted, activity2StartedEvent2.GetEventType())

	activity2FailedEvent2 := s.msBuilder.GetHistoryBuilder().history[12]
	s.Equal(int64(13), activity2FailedEvent2.GetEventId())
	s.Equal(workflow.EventTypeActivityTaskFailed, activity2FailedEvent2.GetEventType())
	s.Equal(int64(12), activity2FailedEvent2.ActivityTaskFailedEventAttributes.GetStartedEventId())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowCancellationRequested() {
	workflowType := "some random workflow type"
	tasklist := "some random tasklist"
	identity := "some random identity"
	input := []byte("some random workflow input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
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

	targetDomain := "some random target domain"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr("some random target run ID"),
	}
	cancellationChildWorkflowOnly := true
	cancellationInitiatedEvent := s.addRequestCancelExternalWorkflowExecutionInitiatedEvent(
		4, targetDomain, targetExecution, cancellationChildWorkflowOnly,
	)
	s.validateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		cancellationInitiatedEvent, 5, 4, targetDomain, targetExecution, cancellationChildWorkflowOnly,
	)
	s.Equal(int64(6), s.getNextEventID())

	cancellationRequestedEvent := s.addExternalWorkflowExecutionCancelRequested(
		5, targetDomain, targetExecution.GetWorkflowId(), targetExecution.GetRunId(),
	)
	s.validateExternalWorkflowExecutionCancelRequested(cancellationRequestedEvent, common.BufferedEventID, 5, targetDomain, targetExecution)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateExternalWorkflowExecutionCancelRequested(cancellationRequestedEvent, 6, 5, targetDomain, targetExecution)
	s.Equal(int64(7), s.getNextEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowCancellationFailed() {
	workflowType := "some random workflow type"
	tasklist := "some random tasklist"
	identity := "some random identity"
	input := []byte("some random workflow input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random workflow ID"),
		RunId:      common.StringPtr(uuid.New()),
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

	targetDomain := "some random target domain"
	targetExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("some random target workflow ID"),
		RunId:      common.StringPtr("some random target run ID"),
	}
	cancellationChildWorkflowOnly := true
	cancellationFailedCause := workflow.CancelExternalWorkflowExecutionFailedCause(59)
	cancellationInitiatedEvent := s.addRequestCancelExternalWorkflowExecutionInitiatedEvent(
		4, targetDomain, targetExecution, cancellationChildWorkflowOnly,
	)
	s.validateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		cancellationInitiatedEvent, 5, 4, targetDomain, targetExecution, cancellationChildWorkflowOnly,
	)
	s.Equal(int64(6), s.getNextEventID())

	cancellationRequestedEvent := s.addRequestCancelExternalWorkflowExecutionFailedEvent(
		4, 5, targetDomain, targetExecution.GetWorkflowId(), targetExecution.GetRunId(), cancellationFailedCause,
	)
	s.validateRequestCancelExternalWorkflowExecutionFailedEvent(
		cancellationRequestedEvent, common.BufferedEventID, 4, 5, targetDomain, targetExecution, cancellationFailedCause,
	)
	s.Nil(s.msBuilder.FlushBufferedEvents())
	s.validateRequestCancelExternalWorkflowExecutionFailedEvent(
		cancellationRequestedEvent, 6, 4, 5, targetDomain, targetExecution, cancellationFailedCause,
	)
	s.Equal(int64(7), s.getNextEventID())
}

func (s *historyBuilderSuite) getNextEventID() int64 {
	return s.msBuilder.GetExecutionInfo().NextEventID
}

func (s *historyBuilderSuite) getPreviousDecisionStartedEventID() int64 {
	return s.msBuilder.GetExecutionInfo().LastProcessedEvent
}

func (s *historyBuilderSuite) addWorkflowExecutionStartedEvent(we workflow.WorkflowExecution, workflowType,
	taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *workflow.HistoryEvent {
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return()

	request := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:                          common.StringPtr(*we.WorkflowId),
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	}

	event, err := s.msBuilder.AddWorkflowExecutionStartedEvent(
		we,
		&history.StartWorkflowExecutionRequest{
			DomainUUID:   common.StringPtr(s.domainID),
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
	taskList, identity string) *workflow.HistoryEvent {
	event, _, err := s.msBuilder.AddDecisionTaskStartedEvent(scheduleID, uuid.New(), &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addDecisionTaskCompletedEvent(scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	event, err := s.msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	}, defaultHistoryMaxAutoResetPoints)
	s.Nil(err)

	return event
}

func (s *historyBuilderSuite) addActivityTaskScheduledEvent(decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) (*workflow.HistoryEvent,
	*persistence.ActivityInfo) {
	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return()
	event, ai, err := s.msBuilder.AddActivityTaskScheduledEvent(decisionCompletedID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr(activityID),
			ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityType)},
			TaskList:                      &workflow.TaskList{Name: common.StringPtr(taskList)},
			Input:                         input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(hearbeatTimeout),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})
	s.Nil(err)
	return event, ai
}

func (s *historyBuilderSuite) addActivityTaskStartedEvent(scheduleID int64, taskList,
	identity string) *workflow.HistoryEvent {
	ai, _ := s.msBuilder.GetActivityInfo(scheduleID)
	event, err := s.msBuilder.AddActivityTaskStartedEvent(ai, scheduleID, uuid.New(), identity)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addActivityTaskCompletedEvent(scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	event, err := s.msBuilder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addActivityTaskFailedEvent(scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	event, err := s.msBuilder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addMarkerRecordedEvent(decisionCompletedEventID int64, markerName string, details []byte, header *map[string][]byte) *workflow.HistoryEvent {
	fields := make(map[string][]byte)
	if header != nil {
		for name, value := range *header {
			fields[name] = value
		}
	}
	event, err := s.msBuilder.AddRecordMarkerEvent(decisionCompletedEventID, &workflow.RecordMarkerDecisionAttributes{
		MarkerName: common.StringPtr(markerName),
		Details:    details,
		Header: &workflow.Header{
			Fields: fields,
		},
	})
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addRequestCancelExternalWorkflowExecutionInitiatedEvent(
	decisionCompletedEventID int64, targetDomain string, targetExecution workflow.WorkflowExecution,
	childWorkflowOnly bool) *workflow.HistoryEvent {
	event, _, err := s.msBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		decisionCompletedEventID,
		uuid.New(),
		&workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			Domain:            common.StringPtr(targetDomain),
			WorkflowId:        targetExecution.WorkflowId,
			RunId:             targetExecution.RunId,
			ChildWorkflowOnly: common.BoolPtr(childWorkflowOnly),
		},
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addExternalWorkflowExecutionCancelRequested(
	initiatedID int64, domain, workflowID, runID string) *workflow.HistoryEvent {

	event, err := s.msBuilder.AddExternalWorkflowExecutionCancelRequested(
		initiatedID, domain, workflowID, runID,
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) addRequestCancelExternalWorkflowExecutionFailedEvent(
	decisionTaskCompletedEventID, initiatedID int64,
	domain, workflowID, runID string, cause workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent {

	event, err := s.msBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
		decisionTaskCompletedEventID, initiatedID, domain, workflowID, runID, cause,
	)
	s.Nil(err)
	return event
}

func (s *historyBuilderSuite) validateWorkflowExecutionStartedEvent(event *workflow.HistoryEvent, workflowType,
	taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeWorkflowExecutionStarted, *event.EventType)
	s.Equal(common.FirstEventID, *event.EventId)
	attributes := event.WorkflowExecutionStartedEventAttributes
	s.NotNil(attributes)
	s.Equal(workflowType, *attributes.WorkflowType.Name)
	s.Equal(taskList, *attributes.TaskList.Name)
	s.Equal(input, attributes.Input)
	s.Equal(executionStartToCloseTimeout, *attributes.ExecutionStartToCloseTimeoutSeconds)
	s.Equal(taskStartToCloseTimeout, *attributes.TaskStartToCloseTimeoutSeconds)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateDecisionTaskScheduledEvent(di *decisionInfo, eventID int64,
	taskList string, timeout int32) {
	s.NotNil(di)
	s.Equal(eventID, di.ScheduleID)
	s.Equal(taskList, di.TaskList)
}

func (s *historyBuilderSuite) validateDecisionTaskStartedEvent(event *workflow.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeDecisionTaskStarted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.DecisionTaskStartedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateDecisionTaskCompletedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, context []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeDecisionTaskCompleted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.DecisionTaskCompletedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(startedID, *attributes.StartedEventId)
	s.Equal(context, attributes.ExecutionContext)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskScheduledEvent(event *workflow.HistoryEvent, eventID, decisionID int64,
	activityID, activityType, taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskScheduled, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskScheduledEventAttributes
	s.NotNil(attributes)
	s.Equal(decisionID, *attributes.DecisionTaskCompletedEventId)
	s.Equal(activityID, *attributes.ActivityId)
	s.Equal(activityType, *attributes.ActivityType.Name)
	s.Equal(taskList, *attributes.TaskList.Name)
	s.Equal(input, attributes.Input)
	s.Equal(timeout, *attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(queueTimeout, *attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(hearbeatTimeout, *attributes.HeartbeatTimeoutSeconds)
}

func (s *historyBuilderSuite) validateActivityTaskStartedEvent(event *workflow.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskStarted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskStartedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskCompletedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, result []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskCompleted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskCompletedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(startedID, *attributes.StartedEventId)
	s.Equal(result, attributes.Result)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskFailedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, reason string, details []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskFailed, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskFailedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(startedID, *attributes.StartedEventId)
	s.Equal(reason, *attributes.Reason)
	s.Equal(details, attributes.Details)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateMarkerRecordedEvent(
	event *workflow.HistoryEvent, eventID, decisionTaskCompletedEventID int64,
	markerName string, details []byte, header *map[string][]byte) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeMarkerRecorded, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.MarkerRecordedEventAttributes
	s.NotNil(attributes)
	s.Equal(decisionTaskCompletedEventID, *attributes.DecisionTaskCompletedEventId)
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
	event *workflow.HistoryEvent, eventID, decisionTaskCompletedEventID int64,
	domain string, execution workflow.WorkflowExecution, childWorkflowOnly bool) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
	s.NotNil(attributes)
	s.Equal(decisionTaskCompletedEventID, *attributes.DecisionTaskCompletedEventId)
	s.Equal(domain, attributes.GetDomain())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
	s.Equal(childWorkflowOnly, *attributes.ChildWorkflowOnly)
}

func (s *historyBuilderSuite) validateExternalWorkflowExecutionCancelRequested(
	event *workflow.HistoryEvent, eventID, initiatedEventID int64,
	domain string, execution workflow.WorkflowExecution) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeExternalWorkflowExecutionCancelRequested, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ExternalWorkflowExecutionCancelRequestedEventAttributes
	s.NotNil(attributes)
	s.Equal(initiatedEventID, attributes.GetInitiatedEventId())
	s.Equal(domain, attributes.GetDomain())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
}

func (s *historyBuilderSuite) validateRequestCancelExternalWorkflowExecutionFailedEvent(
	event *workflow.HistoryEvent, eventID, decisionTaskCompletedEventID, initiatedEventID int64,
	domain string, execution workflow.WorkflowExecution, cause workflow.CancelExternalWorkflowExecutionFailedCause) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeRequestCancelExternalWorkflowExecutionFailed, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.RequestCancelExternalWorkflowExecutionFailedEventAttributes
	s.NotNil(attributes)
	s.Equal(decisionTaskCompletedEventID, attributes.GetDecisionTaskCompletedEventId())
	s.Equal(initiatedEventID, attributes.GetInitiatedEventId())
	s.Equal(domain, attributes.GetDomain())
	s.Equal(execution.GetWorkflowId(), attributes.WorkflowExecution.GetWorkflowId())
	s.Equal(execution.GetRunId(), attributes.WorkflowExecution.GetRunId())
	s.Equal(cause, *attributes.Cause)
}

func (s *historyBuilderSuite) printHistory() string {
	return s.builder.GetHistory().String()
}
