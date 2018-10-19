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
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	mutableStateSuite struct {
		suite.Suite
		msBuilder *mutableStateBuilder
		logger    bark.Logger
	}
)

func TestMutableStateSuite(t *testing.T) {
	s := new(mutableStateSuite)
	suite.Run(t, s)
}

func (s *mutableStateSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *mutableStateSuite) TearDownSuite() {

}

func (s *mutableStateSuite) SetupTest() {
	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.msBuilder = newMutableStateBuilder(cluster.TestCurrentClusterName, NewConfig(dynamicconfig.NewNopCollection(), 1), s.logger)
}

func (s *mutableStateSuite) TearDownTest() {

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
	domainID := validDomainID
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr(validRunID),
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
