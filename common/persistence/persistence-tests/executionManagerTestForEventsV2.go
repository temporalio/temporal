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
	"runtime/debug"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
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

func (s *ExecutionManagerSuiteForEventsV2) newRandomChecksum() *persistencespb.Checksum {
	return &persistencespb.Checksum{
		Flavor:  enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY,
		Version: 22,
		Value:   uuid.NewRandom(),
	}
}

func (s *ExecutionManagerSuiteForEventsV2) assertChecksumsEqual(expected *persistencespb.Checksum, actual *persistencespb.Checksum) {
	if actual.GetFlavor() != enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY {
		// not all stores support checksum persistence today
		// if its not supported, assert that everything is zero'd out
		expected = nil
	}
	s.Equal(expected, actual)
}

// TestWorkflowCreation test
func (s *ExecutionManagerSuiteForEventsV2) TestWorkflowCreation() {
	defer failOnPanic(s.T())
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "test-eventsv2-workflow",
		RunId:      "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	csum := s.newRandomChecksum()

	_, err0 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  "taskQueue",
				WorkflowTypeName:           "wType",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(20),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(13),
				LastWorkflowTaskStartId:    0,
				WorkflowTaskScheduleId:     2,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				ExecutionStats:             &persistencespb.ExecutionStats{},
			},
			NextEventID: 3,
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			TransferTasks: []tasks.Task{
				&tasks.WorkflowTask{
					WorkflowKey:         workflowKey,
					TaskID:              s.GetNextSequenceNumber(),
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

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.assertChecksumsEqual(csum, state0.Checksum)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	currentTime := timestamp.TimePtr(time.Date(1978, 8, 22, 12, 59, 59, 999999, time.UTC))
	timerID := "id_1"
	timerInfos := []*persistencespb.TimerInfo{{
		Version:    3345,
		TimerId:    timerID,
		ExpiryTime: currentTime,
		TaskStatus: 2,
		StartedId:  5,
	}}

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), []int64{int64(4)}, nil, int64(3), nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].GetTimerId())
	s.Equal(currentTime, state.TimerInfos[timerID].ExpiryTime)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskStatus)
	s.Equal(int64(5), state.TimerInfos[timerID].GetStartedId())
	s.assertChecksumsEqual(testWorkflowChecksum, state.Checksum)

	err2 = s.UpdateWorkflowExecution(updatedInfo, updatedState, int64(5), nil, nil, int64(5), nil, nil, nil, nil, []string{timerID})
	s.NoError(err2)

	state, err2 = s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err2)
	s.NotNil(state, "expected valid state.")
	s.Equal(0, len(state.TimerInfos))
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
	versionHistory := versionhistory.NewVersionHistory(
		[]byte{1},
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(1, 0)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	csum := s.newRandomChecksum()

	_, err0 := s.ExecutionManager.CreateWorkflowExecution(&p.CreateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		RangeID: s.ShardInfo.GetRangeId(),
		NewWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                namespaceID,
				WorkflowId:                 workflowExecution.GetWorkflowId(),
				TaskQueue:                  "taskQueue",
				WorkflowTypeName:           "wType",
				WorkflowRunTimeout:         timestamp.DurationFromSeconds(20),
				DefaultWorkflowTaskTimeout: timestamp.DurationFromSeconds(13),
				LastWorkflowTaskStartId:    0,
				WorkflowTaskScheduleId:     2,
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				VersionHistories:           versionHistories,
				ExecutionStats:             &persistencespb.ExecutionStats{},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           workflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID: common.EmptyEventID,
			TransferTasks: []tasks.Task{
				&tasks.WorkflowTask{
					WorkflowKey: definition.NewWorkflowKey(
						namespaceID,
						workflowExecution.WorkflowId,
						workflowExecution.RunId,
					),
					TaskID:              s.GetNextSequenceNumber(),
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

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	s.NotNil(info0, "Valid Workflow info expected.")
	s.Equal(versionHistories, state0.ExecutionInfo.VersionHistories)
	s.assertChecksumsEqual(csum, state0.Checksum)

	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedInfo.LastWorkflowTaskStartId = int64(2)
	currentTime := timestamp.TimePtr(time.Date(1978, 8, 22, 12, 59, 59, 999999, time.UTC))
	timerID := "id_1"
	timerInfos := []*persistencespb.TimerInfo{{
		Version:    3345,
		TimerId:    timerID,
		ExpiryTime: currentTime,
		TaskStatus: 2,
		StartedId:  5,
	}}
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, versionhistory.NewVersionHistoryItem(2, 0))
	s.NoError(err)
	updatedInfo.VersionHistories = versionHistories

	err2 := s.UpdateWorkflowExecution(updatedInfo, updatedState, state0.NextEventId, []int64{int64(4)}, nil, common.EmptyEventID, nil, nil, nil, timerInfos, nil)
	s.NoError(err2)

	state, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	s.NotNil(state, "expected valid state.")
	s.Equal(1, len(state.TimerInfos))
	s.Equal(int64(3345), state.TimerInfos[timerID].Version)
	s.Equal(timerID, state.TimerInfos[timerID].GetTimerId())
	s.Equal(currentTime, state.TimerInfos[timerID].ExpiryTime)
	s.Equal(int64(2), state.TimerInfos[timerID].TaskStatus)
	s.Equal(int64(5), state.TimerInfos[timerID].GetStartedId())
	s.Equal(versionHistories, state.ExecutionInfo.VersionHistories)
	s.assertChecksumsEqual(testWorkflowChecksum, state.Checksum)
}

// TestContinueAsNew test
func (s *ExecutionManagerSuiteForEventsV2) TestContinueAsNew() {
	namespaceID := uuid.New()
	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "continue-as-new-workflow-test",
		RunId:      "551c88d2-d9e6-404f-8131-9eec14f36643",
	}
	workflowKey := definition.NewWorkflowKey(
		namespaceID,
		workflowExecution.WorkflowId,
		workflowExecution.RunId,
	)

	_, err0 := s.CreateWorkflowExecution(namespaceID, workflowExecution, "queue1", "wType", timestamp.DurationFromSeconds(20), timestamp.DurationFromSeconds(13), 3, 0, 2, nil)
	s.NoError(err0)

	state0, err1 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err1)
	info0 := state0.ExecutionInfo
	updatedInfo := copyWorkflowExecutionInfo(info0)
	updatedState := copyWorkflowExecutionState(state0.ExecutionState)
	updatedState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	updatedState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	updatedInfo.LastWorkflowTaskStartId = int64(2)

	newWorkflowExecution := commonpb.WorkflowExecution{
		WorkflowId: "continue-as-new-workflow-test",
		RunId:      "64c7e15a-3fd7-4182-9c6f-6f25a4fa2614",
	}

	newworkflowTask := &tasks.WorkflowTask{
		WorkflowKey: workflowKey,
		TaskID:      s.GetNextSequenceNumber(),
		TaskQueue:   updatedInfo.TaskQueue,
		ScheduleID:  int64(2),
	}

	_, err2 := s.ExecutionManager.UpdateWorkflowExecution(&p.UpdateWorkflowExecutionRequest{
		ShardID: s.ShardInfo.GetShardId(),
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:       updatedInfo,
			ExecutionState:      updatedState,
			NextEventID:         int64(5),
			TransferTasks:       []tasks.Task{newworkflowTask},
			TimerTasks:          nil,
			Condition:           state0.NextEventId,
			UpsertActivityInfos: nil,
			DeleteActivityInfos: nil,
			UpsertTimerInfos:    nil,
			DeleteTimerInfos:    nil,
		},
		NewWorkflowSnapshot: &p.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId:                updatedInfo.NamespaceId,
				WorkflowId:                 newWorkflowExecution.GetWorkflowId(),
				TaskQueue:                  updatedInfo.TaskQueue,
				WorkflowTypeName:           updatedInfo.WorkflowTypeName,
				WorkflowRunTimeout:         updatedInfo.WorkflowRunTimeout,
				DefaultWorkflowTaskTimeout: updatedInfo.DefaultWorkflowTaskTimeout,
				LastWorkflowTaskStartId:    common.EmptyEventID,
				WorkflowTaskScheduleId:     int64(2),
				WorkflowTaskStartedId:      common.EmptyEventID,
				WorkflowTaskTimeout:        timestamp.DurationFromSeconds(1),
				ExecutionStats:             &persistencespb.ExecutionStats{},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:           newWorkflowExecution.GetRunId(),
				CreateRequestId: uuid.New(),
				State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			NextEventID:   state0.NextEventId,
			TransferTasks: nil,
			TimerTasks:    nil,
		},
		RangeID: s.ShardInfo.GetRangeId(),
	})

	s.NoError(err2)

	prevExecutionState, err3 := s.GetWorkflowMutableState(namespaceID, workflowExecution)
	s.NoError(err3)
	prevExecutionInfo := prevExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, prevExecutionState.ExecutionState.State)
	s.Equal(int64(5), prevExecutionState.NextEventId)
	s.Equal(int64(2), prevExecutionInfo.LastWorkflowTaskStartId)

	newExecutionState, err4 := s.GetWorkflowMutableState(namespaceID, newWorkflowExecution)
	s.NoError(err4)
	newExecutionInfo := newExecutionState.ExecutionInfo
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, newExecutionState.ExecutionState.State)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, newExecutionState.ExecutionState.Status)
	s.Equal(int64(3), newExecutionState.NextEventId)
	s.Equal(common.EmptyEventID, newExecutionInfo.LastWorkflowTaskStartId)
	s.Equal(int64(2), newExecutionInfo.WorkflowTaskScheduleId)

	newRunID, err5 := s.GetCurrentWorkflowRunID(namespaceID, workflowExecution.WorkflowId)
	s.NoError(err5)
	s.Equal(newWorkflowExecution.RunId, newRunID)
}
