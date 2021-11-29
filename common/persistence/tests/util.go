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

package tests

import (
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/service/history/tasks"
)

func randomSnapshot(
	namespaceID string,
	workflowID string,
	runID string,
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
) *p.WorkflowSnapshot {
	return &p.WorkflowSnapshot{
		ExecutionInfo:  randomExecutionInfo(namespaceID, workflowID, lastWriteVersion),
		ExecutionState: randomExecutionState(runID, state, status),

		NextEventID: rand.Int63(),

		ActivityInfos:       randomInt64ActivityInfoMap(),
		TimerInfos:          randomStringTimerInfoMap(),
		ChildExecutionInfos: randomInt64ChildExecutionInfoMap(),
		RequestCancelInfos:  randomInt64RequestCancelInfoMap(),
		SignalInfos:         randomInt64SignalInfoMap(),
		SignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},

		TransferTasks:    []tasks.Task{},
		ReplicationTasks: []tasks.Task{},
		TimerTasks:       []tasks.Task{},
		VisibilityTasks:  []tasks.Task{},

		Condition:       rand.Int63(),
		DBRecordVersion: dbRecordVersion,
	}
}

func randomMutation(
	namespaceID string,
	workflowID string,
	runID string,
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
) *p.WorkflowMutation {
	mutation := &p.WorkflowMutation{
		ExecutionInfo:  randomExecutionInfo(namespaceID, workflowID, lastWriteVersion),
		ExecutionState: randomExecutionState(runID, state, status),

		NextEventID: rand.Int63(),

		UpsertActivityInfos:       randomInt64ActivityInfoMap(),
		DeleteActivityInfos:       map[int64]struct{}{rand.Int63(): {}},
		UpsertTimerInfos:          randomStringTimerInfoMap(),
		DeleteTimerInfos:          map[string]struct{}{uuid.New().String(): {}},
		UpsertChildExecutionInfos: randomInt64ChildExecutionInfoMap(),
		DeleteChildExecutionInfos: map[int64]struct{}{rand.Int63(): {}},
		UpsertRequestCancelInfos:  randomInt64RequestCancelInfoMap(),
		DeleteRequestCancelInfos:  map[int64]struct{}{rand.Int63(): {}},
		UpsertSignalInfos:         randomInt64SignalInfoMap(),
		DeleteSignalInfos:         map[int64]struct{}{rand.Int63(): {}},
		UpsertSignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		DeleteSignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		//NewBufferedEvents: see below
		//ClearBufferedEvents: see below

		TransferTasks:    []tasks.Task{},
		ReplicationTasks: []tasks.Task{},
		TimerTasks:       []tasks.Task{},
		VisibilityTasks:  []tasks.Task{},

		Condition:       rand.Int63(),
		DBRecordVersion: dbRecordVersion,
	}

	switch rand.Int63() % 3 {
	case 0:
		mutation.ClearBufferedEvents = true
		mutation.NewBufferedEvents = nil
	case 1:
		mutation.ClearBufferedEvents = false
		mutation.NewBufferedEvents = nil
	case 2:
		mutation.ClearBufferedEvents = false
		mutation.NewBufferedEvents = []*historypb.HistoryEvent{randomHistoryEvent()}
	default:
		panic("broken test")
	}
	return mutation
}

func randomExecutionInfo(
	namespaceID string,
	workflowID string,
	lastWriteVersion int64,
) *persistencespb.WorkflowExecutionInfo {
	var executionInfo persistencespb.WorkflowExecutionInfo
	_ = gofakeit.Struct(&executionInfo)
	executionInfo.NamespaceId = namespaceID
	executionInfo.WorkflowId = workflowID
	executionInfo.VersionHistories = randomVersionHistory(lastWriteVersion)
	return &executionInfo
}

func randomExecutionState(
	runID string,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
) *persistencespb.WorkflowExecutionState {
	return &persistencespb.WorkflowExecutionState{
		CreateRequestId: uuid.New().String(),
		RunId:           runID,
		State:           state,
		Status:          status,
	}
}

func randomInt64ActivityInfoMap() map[int64]*persistencespb.ActivityInfo {
	return map[int64]*persistencespb.ActivityInfo{
		rand.Int63(): randomActivityInfo(),
	}
}

func randomStringTimerInfoMap() map[string]*persistencespb.TimerInfo {
	return map[string]*persistencespb.TimerInfo{
		uuid.New().String(): randomTimerInfo(),
	}
}

func randomInt64ChildExecutionInfoMap() map[int64]*persistencespb.ChildExecutionInfo {
	return map[int64]*persistencespb.ChildExecutionInfo{
		rand.Int63(): randomChildExecutionInfo(),
	}
}

func randomInt64RequestCancelInfoMap() map[int64]*persistencespb.RequestCancelInfo {
	return map[int64]*persistencespb.RequestCancelInfo{
		rand.Int63(): randomRequestCancelInfo(),
	}
}

func randomInt64SignalInfoMap() map[int64]*persistencespb.SignalInfo {
	return map[int64]*persistencespb.SignalInfo{
		rand.Int63(): randomSignalInfo(),
	}
}

func randomActivityInfo() *persistencespb.ActivityInfo {
	// cannot use gofakeit due to RetryLastFailure is of type Failure
	// and Failure can contain another Failure -> stack overflow
	return &persistencespb.ActivityInfo{
		Version:                rand.Int63(),
		ScheduledEventBatchId:  rand.Int63(),
		ScheduledTime:          randomTime(),
		StartedId:              rand.Int63(),
		StartedTime:            randomTime(),
		ActivityId:             uuid.New().String(),
		RequestId:              uuid.New().String(),
		ScheduleToStartTimeout: randomDuration(),
		ScheduleToCloseTimeout: randomDuration(),
		StartToCloseTimeout:    randomDuration(),
		HeartbeatTimeout:       randomDuration(),

		// other fields omitted, above should be enough for tests
	}
}

func randomTimerInfo() *persistencespb.TimerInfo {
	var timerInfo persistencespb.TimerInfo
	_ = gofakeit.Struct(&timerInfo)
	return &timerInfo
}

func randomChildExecutionInfo() *persistencespb.ChildExecutionInfo {
	var childExecutionInfo persistencespb.ChildExecutionInfo
	_ = gofakeit.Struct(&childExecutionInfo)
	return &childExecutionInfo
}

func randomRequestCancelInfo() *persistencespb.RequestCancelInfo {
	var requestCancelInfo persistencespb.RequestCancelInfo
	_ = gofakeit.Struct(&requestCancelInfo)
	return &requestCancelInfo
}

func randomSignalInfo() *persistencespb.SignalInfo {
	var signalInfo persistencespb.SignalInfo
	_ = gofakeit.Struct(&signalInfo)
	return &signalInfo
}

func randomHistoryEvent() *historypb.HistoryEvent {
	var historyEvent historypb.HistoryEvent
	_ = gofakeit.Struct(&historyEvent)
	return &historyEvent
}

func randomResetPoints() *workflowpb.ResetPoints {
	return &workflowpb.ResetPoints{Points: []*workflowpb.ResetPointInfo{{
		BinaryChecksum:               uuid.New().String(),
		RunId:                        uuid.New().String(),
		FirstWorkflowTaskCompletedId: rand.Int63(),
		CreateTime:                   randomTime(),
		ExpireTime:                   randomTime(),
		Resettable:                   rand.Int31()%2 == 0,
	}}}
}

func randomStringPayloadMap() map[string]*commonpb.Payload {
	return map[string]*commonpb.Payload{
		uuid.New().String(): randomPayload(),
	}
}

func randomPayload() *commonpb.Payload {
	var payload commonpb.Payload
	_ = gofakeit.Struct(&payload)
	return &payload
}

func randomVersionHistory(
	lastWriteVersion int64,
) *historyspb.VersionHistories {
	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: shuffle.Bytes([]byte("random branch token")),
			Items: []*historyspb.VersionHistoryItem{{
				EventId: rand.Int63(),
				Version: lastWriteVersion,
			}},
		}},
	}
}

func randomTime() *time.Time {
	time := time.Unix(0, rand.Int63())
	return &time
}

func randomDuration() *time.Duration {
	duration := time.Duration(rand.Int63())
	return &duration
}
