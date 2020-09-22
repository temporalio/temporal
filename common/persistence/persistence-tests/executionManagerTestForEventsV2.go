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

package persistencetests

import (
	"os"
	"runtime/debug"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/checksum"
	"go.temporal.io/server/common/primitives/timestamp"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common"
	p "go.temporal.io/server/common/persistence"
)

type (
	// ExecutionManagerSuiteForEventsV2 contains matching persistence tests
	ExecutionManagerSuiteForEventsV2 struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func failOnPanic(t *testing.T) {
	r := recover()
	if r != nil {
		t.Errorf("test panicked: %v %s", r, debug.Stack())
		t.FailNow()
	}
}

// SetupSuite implementation
func (s *ExecutionManagerSuiteForEventsV2) SetupSuite() {
	defer failOnPanic(s.T())
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// TearDownSuite implementation
func (s *ExecutionManagerSuiteForEventsV2) TearDownSuite() {
	defer failOnPanic(s.T())
	s.TearDownWorkflowStore()
}

// SetupTest implementation
func (s *ExecutionManagerSuiteForEventsV2) SetupTest() {
	defer failOnPanic(s.T())
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ClearTasks()
}

func (s *ExecutionManagerSuiteForEventsV2) newRandomChecksum() checksum.Checksum {
	return checksum.Checksum{
		Flavor:  checksum.FlavorIEEECRC32OverProto3Binary,
		Version: 22,
		Value:   []byte(uuid.NewRandom()),
	}
}

func (s *ExecutionManagerSuiteForEventsV2) assertChecksumsEqual(expected checksum.Checksum, actual checksum.Checksum) {
	if !actual.Flavor.IsValid() {
		// not all stores support checksum persistence today
		// if its not supported, assert that everything is zero'd out
		expected = checksum.Checksum{}
	}
	s.EqualValues(expected, actual)
}

// TestWorkflowCreation test
func (s *ExecutionManagerSuiteForEventsV2) TestWorkflowCreation() {
	defer failOnPanic(s.T())
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-eventsv2-workflow",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}

	csum := s.newRandomChecksum()

	_, err0 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  "taskQueue",
				WorkflowTypeName:           "wType",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(20),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(13),
				ExecutionState: &persistenceblobs.WorkflowExecutionState{
					RunId:           workflowExecution.GetRunId(),
					CreateRequestId: uuid.New(),
					State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
				NextEventId:            3,
				LastProcessedEvent:     0,
				WorkflowTaskScheduleId: 2,
				WorkflowTaskStartedId:  common.EmptyEventID,
				WorkflowTaskTimeout:    timestamp.DurationFromSeconds(1),
				EventBranchToken:       []byte("branchToken1"),
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			TransferTasks: []p.Task{
				&p.WorkflowTask{
					TaskID:              s.GetNextSequenceNumber(),
					NamespaceID:         namespaceID,
					TaskQueue:           "taskQueue",
					ScheduleID:          2,
					VisibilityTimestamp: time.Now().UTC(),
				},
			},
			TimerTasks: nil,
			Checksum:   csum,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	s.NoError(err0)

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal([]byte("branchToken1"), info0.EventBranchToken)
	s.assertChecksumsEqual(csum, state0.Checksum)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.NextEventId = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := timestamp.TimePtr(time.Date(1978, 8, 22, 12, 59, 59, 999999, time.UTC))
	timerID := "id_1"
	timerInfos := []*persistenceblobs.TimerInfo{{
		Version:    3345,
		TimerId:    timerID,
		ExpiryTime: currentTime,
		TaskStatus: 2,
		StartedId:  5,
	}}
	updatedInfo.EventBranchToken = []byte("branchToken2")

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, []int64{int64(4)}, nil, int64(3), nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].GetTimerId())
	s.Equal(currentTime, state.TimerInfos[timerID].ExpiryTime)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskStatus)
	s.Equal(int64(5), state.TimerInfos[timerID].GetStartedId())
	s.assertChecksumsEqual(testWorkflowChecksum, state.Checksum)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedStats, nil, nil, nil, int64(5), nil, nil, nil, nil, []string{timerID})
	s.NoError(err2)

	state, err2 = s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.TimerInfos))
	info1 := state.ExecutionInfo
	s.Equal([]byte("branchToken2"), info1.EventBranchToken)
	s.assertChecksumsEqual(testWorkflowChecksum, state.Checksum)
}

// TestWorkflowCreationWithVersionHistories test
func (s *ExecutionManagerSuiteForEventsV2) TestWorkflowCreationWithVersionHistories() {
	defer failOnPanic(s.T())
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-eventsv2-workflow-version-history",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	versionHistory := p.NewVersionHistory(
		[]byte{1},
		[]*p.VersionHistoryItem{p.NewVersionHistoryItem(1, 0)},
	)
	versionHistories := p.NewVersionHistories(versionHistory)

	csum := s.newRandomChecksum()

	_, err0 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		RangeID: s.ShardInfo.GetRangeId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  "taskQueue",
				WorkflowTypeName:           "wType",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(20),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(13),
				ExecutionState: &persistenceblobs.WorkflowExecutionState{
					RunId:           workflowExecution.GetRunId(),
					CreateRequestId: uuid.New(),
					State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
				NextEventId:            common.EmptyEventID,
				LastProcessedEvent:     0,
				WorkflowTaskScheduleId: 2,
				WorkflowTaskStartedId:  common.EmptyEventID,
				WorkflowTaskTimeout:    timestamp.DurationFromSeconds(1),
				EventBranchToken:       nil,
			},
			ExecutionStats:   &persistenceblobs.ExecutionStats{},
			VersionHistories: versionHistories,
			TransferTasks: []p.Task{
				&p.WorkflowTask{
					TaskID:              s.GetNextSequenceNumber(),
					NamespaceID:         namespaceID,
					TaskQueue:           "taskQueue",
					ScheduleID:          2,
					VisibilityTimestamp: time.Now().UTC(),
				},
			},
			TimerTasks: nil,
			Checksum:   csum,
		},
	})

	s.NoError(err0)

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(versionHistories, state0.VersionHistories)
	s.assertChecksumsEqual(csum, state0.Checksum)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.LastProcessedEvent = int64(2)
	currentTime := timestamp.TimePtr(time.Date(1978, 8, 22, 12, 59, 59, 999999, time.UTC))
	timerID := "id_1"
	timerInfos := []*persistenceblobs.TimerInfo{{
		Version:    3345,
		TimerId:    timerID,
		ExpiryTime: currentTime,
		TaskStatus: 2,
		StartedId:  5,
	}}
	versionHistory, err := versionHistories.GetCurrentVersionHistory()
	s.NoError(err)
	err = versionHistory.AddOrUpdateItem(p.NewVersionHistoryItem(2, 0))
	s.NoError(err)

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedStats, versionHistories, []int64{int64(4)}, nil, common.EmptyEventID, nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].GetTimerId())
	s.Equal(currentTime, state.TimerInfos[timerID].ExpiryTime)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskStatus)
	s.Equal(int64(5), state.TimerInfos[timerID].GetStartedId())
	s.Equal(state.VersionHistories, versionHistories)
	s.assertChecksumsEqual(testWorkflowChecksum, state.Checksum)
}

//TestContinueAsNew test
func (s *ExecutionManagerSuiteForEventsV2) TestContinueAsNew() {
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "continue-as-new-workflow-test",
		RunId:      "551c88d2-d9e6-404f-8131-9eec14f36643",
	}

	_, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)
	updatedInfo.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedInfo.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	updatedInfo.NextEventId = int64(5)
	updatedInfo.LastProcessedEvent = int64(2)

	newWorkflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "continue-as-new-workflow-test",
		RunId:      "64c7e15a-3fd7-4182-9c6f-6f25a4fa2614",
	}

	newworkflowTask := &p.WorkflowTask{
		TaskID:      s.GetNextSequenceNumber(),
		NamespaceID: updatedInfo.NamespaceId,
		TaskQueue:   updatedInfo.TaskQueue,
		ScheduleID:  int64(2),
	}

	_, err2 := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionStats:      updatedStats,
			TransferTasks:       []p.Task{newworkflowTask},
			TimerTasks:          nil,
			Condition:           info0.NextEventId,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
		},
		NewWorkflowSnapshot: &p.WorkflowSnapshot{
			ExecutionInfo: &p.WorkflowExecutionInfo{
				NamespaceId:                updatedInfo.NamespaceId,
				WorkflowId:                 newWorkflowExecution.GetWorkflowId(),
				TaskQueue:                  updatedInfo.TaskQueue,
				WorkflowTypeName:           updatedInfo.WorkflowTypeName,
				WorkflowRunTimeout:         updatedInfo.WorkflowRunTimeout,
				DefaultWorkflowTaskTimeout: updatedInfo.DefaultWorkflowTaskTimeout,
				ExecutionState: &persistenceblobs.WorkflowExecutionState{
					RunId:           newWorkflowExecution.GetRunId(),
					CreateRequestId: uuid.New(),
					State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
				NextEventId:            info0.NextEventId,
				LastProcessedEvent:     common.EmptyEventID,
				WorkflowTaskScheduleId: int64(2),
				WorkflowTaskStartedId:  common.EmptyEventID,
				WorkflowTaskTimeout:    timestamp.DurationFromSeconds(1),
				EventBranchToken:       []byte("branchToken1"),
			},
			ExecutionStats: &persistenceblobs.ExecutionStats{},
			TransferTasks:  nil,
			TimerTasks:     nil,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	s.NoError(err2)

	prevExecutionState, err3 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err3)
	prevExecutionInfo := prevExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, prevExecutionInfo.ExecutionState.State)
	s.Equal(int64(5), prevExecutionInfo.NextEventId)
	s.Equal(int64(2), prevExecutionInfo.LastProcessedEvent)

	newExecutionState, err4 := s.GetWorkflowExecutionInfo(namespaceID, newWorkflowExecution)
	s.NoError(err4)
	newExecutionInfo := newExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, newExecutionInfo.ExecutionState.State)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, newExecutionInfo.ExecutionState.Status)
	s.Equal(int64(3), newExecutionInfo.NextEventId)
	s.Equal(common.EmptyEventID, newExecutionInfo.LastProcessedEvent)
	s.Equal(int64(2), newExecutionInfo.WorkflowTaskScheduleId)
	s.Equal([]byte("branchToken1"), newExecutionInfo.EventBranchToken)

	newRunID, err5 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.WorkflowId)
	s.NoError(err5)
	s.Equal(newWorkflowExecution.RunId, newRunID)
}

// TestWorkflowResetNoCurrNoReplicate test
func (s *ExecutionManagerSuiteForEventsV2) TestWorkflowResetNoCurrNoReplicate() {
	namespaceID := uuid.New()
	runID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-reset-workflow-with-replication-state-test",
		RunId:      runID,
	}

	currentTime := time.Date(1978, 8, 22, 12, 59, 59, 999999, time.UTC)
	txTasks := []p.Task{
		&p.WorkflowTimeoutTask{
			TaskID:              s.GetNextSequenceNumber(),
			VisibilityTimestamp: currentTime,
		}}

	task0, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "taskQueue", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, txTasks)
	s.NoError(err0)
	s.NotNil(task0, "Expected non empty task identifier.")

	taskD, err := s.GetTransferTasks(2, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")
	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, taskD[0].TaskType)
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)
	taskD, err = s.GetTransferTasks(2, false)
	s.Equal(0, len(taskD), "Expected 0 workflow task.")

	taskT, err := s.GetTimerIndexTasks(2, false)
	s.Equal(1, len(taskT), "Expected 1 timer task.")
	s.Equal(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, taskT[0].TaskType)
	err = s.CompleteTimerTask(*taskT[0].VisibilityTime, taskT[0].GetTaskId())
	s.NoError(err)
	taskT, err = s.GetTimerIndexTasks(2, false)
	s.Equal(0, len(taskT), "Expected 0 timer task.")

	state0, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(namespaceID, info0.NamespaceId)
	s.Equal("taskQueue", info0.TaskQueue)
	s.Equal("wType", info0.WorkflowTypeName)
	s.EqualValues(int64(20), info0.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info0.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(int64(3), info0.NextEventId)
	s.Equal(int64(0), info0.LastProcessedEvent)
	s.Equal(int64(2), info0.WorkflowTaskScheduleId)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedStats := copyExecutionStats(state0.ExecutionStats)

	newRunID := uuid.New()
	newExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowExecution.WorkflowId,
		RunId:      newRunID,
	}
	insertInfo := copyWorkflowExecutionInfo(info0)
	insertStats := copyExecutionStats(state0.ExecutionStats)
	insertInfo.ExecutionState.RunId = newRunID
	insertInfo.NextEventId = int64(50)
	insertInfo.LastProcessedEvent = int64(20)
	insertInfo.EventBranchToken = []byte("branchToken4")

	insertTransTasks := []p.Task{
		&p.WorkflowTask{
			TaskID:              s.GetNextSequenceNumber(),
			NamespaceID:         namespaceID,
			VisibilityTimestamp: time.Now().UTC(),
			ScheduleID:          13,
			Version:             200,
		},
	}

	insertTimerTasks := []p.Task{
		&p.WorkflowTimeoutTask{
			TaskID:              s.GetNextSequenceNumber(),
			VisibilityTimestamp: time.Now().UTC().Add(time.Minute),
			Version:             201,
		},
	}

	insertTimerInfos := []*persistenceblobs.TimerInfo{{
		Version:    100,
		TimerId:    "id101",
		ExpiryTime: &currentTime,
		TaskStatus: 102,
		StartedId:  103,
	}}

	insertActivityInfos := []*persistenceblobs.ActivityInfo{{
		Version:        110,
		ScheduleId:     111,
		StartedId:      112,
		ActivityId:     uuid.New(),
		ScheduledEvent: &historypb.HistoryEvent{EventId: 1},
	}}

	insertRequestCancelInfos := []*persistenceblobs.RequestCancelInfo{{
		Version:         120,
		InitiatedId:     121,
		CancelRequestId: uuid.New(),
	}}

	err = s.ResetWorkflowExecution(3,
		insertInfo, insertStats, insertActivityInfos, insertTimerInfos, nil, insertRequestCancelInfos, nil, nil, insertTransTasks, insertTimerTasks, nil,
		false, updatedInfo, updatedStats, nil, nil, info0.GetRunId(), -1000)
	s.NoError(err)

	//////////////////////////////
	// start verifying resetWF
	///////////////////////////////

	// transfer tasks
	taskD, err = s.GetTransferTasks(3, false)
	s.Equal(1, len(taskD), "Expected 1 workflow task.")

	s.EqualValues(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK, taskD[0].TaskType)
	s.Equal(int64(200), taskD[0].Version)
	err = s.CompleteTransferTask(taskD[0].GetTaskId())
	s.NoError(err)
	taskD, err = s.GetTransferTasks(2, false)
	s.Equal(0, len(taskD), "Expected 0 workflow task.")

	// timer tasks
	taskT, err = s.GetTimerIndexTasks(3, false)
	s.Equal(1, len(taskT), "Expected 1 timer task.")
	s.EqualValues(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, taskT[0].TaskType)
	s.Equal(int64(201), taskT[0].Version)
	err = s.CompleteTimerTask(*taskT[0].VisibilityTime, taskT[0].GetTaskId())
	s.NoError(err)
	taskT, err = s.GetTimerIndexTasks(2, false)
	s.Equal(0, len(taskT), "Expected 0 timer task.")

	// check current run
	currRunID, err := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.GetWorkflowId())
	s.Nil(err)
	s.Equal(newExecution.GetRunId(), currRunID)

	// the previous execution
	state1, err1 := s.GetWorkflowExecutionInfo(namespaceID, workflowExecution)
	s.NoError(err1)
	info1 := state1.ExecutionInfo
	s.NotNil(info1, "Valid Workflow info expected.")
	s.Equal(int64(3), info1.NextEventId)
	s.Equal(int64(0), info1.LastProcessedEvent)
	s.Equal(namespaceID, info1.NamespaceId)
	s.Equal("taskQueue", info1.TaskQueue)
	s.Equal("wType", info1.WorkflowTypeName)
	s.EqualValues(int64(20), info1.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info1.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(int64(2), info1.WorkflowTaskScheduleId)

	// the current execution
	state2, err2 := s.GetWorkflowExecutionInfo(namespaceID, newExecution)
	s.NoError(err2)
	info2 := state2.ExecutionInfo

	s.NotNil(info2, "Valid Workflow info expected.")
	s.Equal(int64(50), info2.NextEventId)
	s.Equal(int64(20), info2.LastProcessedEvent)
	s.Equal([]byte("branchToken4"), info2.EventBranchToken)
	s.Equal(namespaceID, info2.NamespaceId)
	s.Equal("taskQueue", info2.TaskQueue)
	s.Equal("wType", info2.WorkflowTypeName)
	s.EqualValues(int64(20), info2.WorkflowRunTimeout.Seconds())
	s.EqualValues(13, int64(info2.DefaultWorkflowTaskTimeout.Seconds()))
	s.Equal(int64(2), info2.WorkflowTaskScheduleId)

	timerInfos2 := state2.TimerInfos
	actInfos2 := state2.ActivityInfos
	reqCanInfos2 := state2.RequestCancelInfos
	childInfos2 := state2.ChildExecutionInfos
	sigInfos2 := state2.SignalInfos
	sigReqIDs2 := state2.SignalRequestedIDs

	s.Equal(1, len(timerInfos2))
	s.Equal(1, len(actInfos2))
	s.Equal(1, len(reqCanInfos2))
	s.Equal(0, len(childInfos2))
	s.Equal(0, len(sigInfos2))
	s.Equal(0, len(sigReqIDs2))

	s.Equal(int64(100), timerInfos2["id101"].Version)
	s.Equal(int64(110), actInfos2[111].Version)
	s.Equal(int64(120), reqCanInfos2[121].Version)
}
