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
	h "github.com/uber/cadence/.gen/go/history"
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

func (s *mutableStateSuite) TestStatsWithStartedEvent() {
	ms := s.createMutableState()
	domainID := "A"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-id"),
		RunId:      common.StringPtr("run_id"),
	}
	workflowType := &workflow.WorkflowType{
		Name: common.StringPtr("test-workflow-type-name"),
	}
	tasklist := &workflow.TaskList{
		Name: common.StringPtr("test-tasklist"),
	}
	startReq := &h.StartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		StartRequest: &workflow.StartWorkflowExecutionRequest{
			WorkflowType:                        workflowType,
			TaskList:                            tasklist,
			Input:                               []byte("test-workflow-input"),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(10),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(5),
			Identity:                            common.StringPtr("test-identity"),
			RequestId:                           common.StringPtr("requestID"),
		},
	}
	expectedSize := len(execution.GetWorkflowId()) + len(workflowType.GetName()) + len(tasklist.GetName())
	ms.AddWorkflowExecutionStartedEvent(execution, startReq)
	stats := ms.GetStats()
	s.validateStats(stats, &mutableStateStats{
		mutableStateSize:  expectedSize,
		executionInfoSize: expectedSize,
	})
}

func (s *mutableStateSuite) TestStatsWithStartedEventWithParent() {
	ms := s.createMutableState()
	domainID := "A"
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-id"),
		RunId:      common.StringPtr("run_id"),
	}
	parentExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("test-workflow-id"),
		RunId:      common.StringPtr("run_id"),
	}
	workflowType := &workflow.WorkflowType{
		Name: common.StringPtr("test-workflow-type-name"),
	}
	tasklist := &workflow.TaskList{
		Name: common.StringPtr("test-tasklist"),
	}
	startReq := &h.StartWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		StartRequest: &workflow.StartWorkflowExecutionRequest{
			WorkflowType:                        workflowType,
			TaskList:                            tasklist,
			Input:                               []byte("test-workflow-input"),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(10),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(5),
			Identity:                            common.StringPtr("test-identity"),
			RequestId:                           common.StringPtr("requestID"),
		},
		ParentExecutionInfo: &h.ParentExecutionInfo{
			Domain:      common.StringPtr(domainID),
			DomainUUID:  common.StringPtr(domainID),
			Execution:   &parentExecution,
			InitiatedId: common.Int64Ptr(10),
		},
	}
	expectedSize := len(execution.GetWorkflowId()) + len(workflowType.GetName()) + len(tasklist.GetName()) +
		len(parentExecution.GetWorkflowId())
	ms.AddWorkflowExecutionStartedEvent(execution, startReq)
	stats := ms.GetStats()
	s.validateStats(stats, &mutableStateStats{
		mutableStateSize:  expectedSize,
		executionInfoSize: expectedSize,
	})
}

func (s *mutableStateSuite) TestStatsForAllInfos() {
	domainID := "A"
	workflowID := "test-workflow-id"
	runID := "run_id"
	workflowType := "test-workflow-type-name"
	tasklist := "test-tasklist"

	ai1 := &persistence.ActivityInfo{
		ScheduleID:     1,
		ActivityID:     "activityA",
		ScheduledEvent: []byte("activityA_scheduled_event"),
		StartedEvent:   []byte("activityA_started_event"),
		Details:        []byte("activityA_details"),
	}

	ai2 := &persistence.ActivityInfo{
		ScheduleID:     2,
		ActivityID:     "activityB",
		ScheduledEvent: []byte("activityB_scheduled_event"),
		StartedEvent:   []byte("activityB_started_event"),
		Details:        []byte("activityB_details"),
	}

	ti1 := &persistence.TimerInfo{
		TimerID:    "timerA",
		StartedID:  3,
		ExpiryTime: time.Now(),
		TaskID:     1000,
	}

	ci1 := &persistence.ChildExecutionInfo{
		InitiatedID:     4,
		InitiatedEvent:  []byte("childA_initiated_event"),
		StartedID:       5,
		StartedEvent:    []byte("childA_started_event"),
		CreateRequestID: "childA_create_request_id",
	}

	si1 := &persistence.SignalInfo{
		InitiatedID:     60,
		SignalRequestID: "signalA_request_id",
		SignalName:      "signalA",
		Input:           []byte("signalA_input"),
		Control:         []byte("signalA_control"),
	}

	bufferedEvent1 := &persistence.SerializedHistoryEventBatch{
		EncodingType: common.EncodingTypeJSON,
		Version:      1,
		Data:         []byte("buffered_event1"),
	}

	bufferedReplicationTask1 := &persistence.BufferedReplicationTask{
		FirstEventID: 80,
		NextEventID:  85,
		Version:      1,
		History: &persistence.SerializedHistoryEventBatch{
			EncodingType: common.EncodingTypeJSON,
			Version:      1,
			Data:         []byte("buffered_replication_task1"),
		},
		NewRunHistory: &persistence.SerializedHistoryEventBatch{
			EncodingType: common.EncodingTypeJSON,
			Version:      1,
			Data:         []byte("buffered_replication_task2"),
		},
	}

	ms := s.createMutableState()
	ms.executionInfo.DomainID = domainID
	ms.executionInfo.WorkflowID = workflowID
	ms.executionInfo.RunID = runID
	ms.executionInfo.WorkflowTypeName = workflowType
	ms.executionInfo.TaskList = tasklist

	ms.pendingActivityInfoIDs = map[int64]*persistence.ActivityInfo{
		1: ai1,
		2: ai2,
	}

	ms.pendingTimerInfoIDs = map[string]*persistence.TimerInfo{
		"timerA": ti1,
	}

	ms.pendingChildExecutionInfoIDs = map[int64]*persistence.ChildExecutionInfo{
		4: ci1,
	}

	ms.pendingSignalInfoIDs = map[int64]*persistence.SignalInfo{
		60: si1,
	}

	ms.bufferedEvents = []*persistence.SerializedHistoryEventBatch{
		bufferedEvent1,
	}

	ms.bufferedReplicationTasks = map[int64]*persistence.BufferedReplicationTask{
		80: bufferedReplicationTask1,
	}

	expectedExecutionInfoSize := len(workflowID) + len(workflowType) + len(tasklist)
	expectedActivityInfoSize := computeActivityInfoSize(ai1) + computeActivityInfoSize(ai2)
	expectedTimerInfoSize := computeTimerInfoSize(ti1)
	expectedChildInfoSize := computeChildInfoSize(ci1)
	expectedSignalInfoSize := computeSignalInfoSize(si1)
	expectedBufferedEventsSize := computeBufferedEventsSize(bufferedEvent1)
	expectedBufferedReplicationTaskSize := computeBufferedReplicationTasksSize(bufferedReplicationTask1)
	totalSize := expectedExecutionInfoSize + expectedActivityInfoSize + expectedTimerInfoSize + expectedChildInfoSize +
		expectedSignalInfoSize + expectedBufferedEventsSize + expectedBufferedReplicationTaskSize
	stats := ms.GetStats()
	s.validateStats(stats, &mutableStateStats{
		mutableStateSize:              totalSize,
		executionInfoSize:             expectedExecutionInfoSize,
		activityInfoSize:              expectedActivityInfoSize,
		activityInfoCount:             2,
		timerInfoSize:                 expectedTimerInfoSize,
		timerInfoCount:                1,
		childInfoSize:                 expectedChildInfoSize,
		childInfoCount:                1,
		signalInfoSize:                expectedSignalInfoSize,
		signalInfoCount:               1,
		bufferedEventsSize:            expectedBufferedEventsSize,
		bufferedEventsCount:           1,
		bufferedReplicationTasksSize:  expectedBufferedReplicationTaskSize,
		bufferedReplicationTasksCount: 1,
	})
}

func (s *mutableStateSuite) validateStats(expectedStats *mutableStateStats, stats *mutableStateStats) {
	s.Equal(expectedStats.mutableStateSize, stats.mutableStateSize, "Unexpected mutable state size")
	s.Equal(expectedStats.executionInfoSize, stats.executionInfoSize, "Unexpected execution info size")
	s.Equal(expectedStats.activityInfoSize, stats.activityInfoSize, "Unexpected activity info size")
	s.Equal(expectedStats.timerInfoSize, stats.timerInfoSize, "Unexpected timer info size")
	s.Equal(expectedStats.childInfoSize, stats.childInfoSize, "Unexpected child info size")
	s.Equal(expectedStats.signalInfoSize, stats.signalInfoSize, "Unexpected signal info size")
	s.Equal(expectedStats.bufferedEventsSize, stats.bufferedEventsSize, "Unexpected buffered events size")
	s.Equal(expectedStats.bufferedReplicationTasksSize, stats.bufferedReplicationTasksSize, "Unexpected buffered replication tasks size")
	s.Equal(expectedStats.activityInfoCount, stats.activityInfoCount, "Unexpected activity info count")
	s.Equal(expectedStats.timerInfoCount, stats.timerInfoCount, "Unexpected mutable state size")
	s.Equal(expectedStats.childInfoCount, stats.childInfoCount, "Unexpected child info count")
	s.Equal(expectedStats.signalInfoCount, stats.signalInfoCount, "Unexpected signal info count")
	s.Equal(expectedStats.requestCancelInfoCount, stats.requestCancelInfoCount, "Unexpected request cancel info count")
	s.Equal(expectedStats.bufferedEventsCount, stats.bufferedEventsCount, "Unexpected buffered events count")
	s.Equal(expectedStats.bufferedReplicationTasksCount, stats.bufferedReplicationTasksCount, "Unexpected buffered replication tasks count")
}

func (s *mutableStateSuite) createMutableState() *mutableStateBuilder {
	currentCluster := ""
	version := int64(1)
	return newMutableStateBuilderWithReplicationState(currentCluster, nil, s.logger, version)
}
