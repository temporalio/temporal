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

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/service/history/tasks"
)

func RandomShardInfo(
	shardID int32,
	rangeID int64,
) *persistencespb.ShardInfo {
	var shardInfo persistencespb.ShardInfo
	_ = fakedata.FakeStruct(&shardInfo)
	shardInfo.ShardId = shardID
	shardInfo.RangeId = rangeID
	return &shardInfo
}

func RandomSnapshot(
	namespaceID string,
	workflowID string,
	runID string,
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
	historyBranchUtil p.HistoryBranchUtil,
) *p.WorkflowSnapshot {
	return &p.WorkflowSnapshot{
		ExecutionInfo:  RandomExecutionInfo(namespaceID, workflowID, lastWriteVersion, historyBranchUtil),
		ExecutionState: RandomExecutionState(runID, state, status),

		NextEventID: rand.Int63(),

		ActivityInfos:       RandomInt64ActivityInfoMap(),
		TimerInfos:          RandomStringTimerInfoMap(),
		ChildExecutionInfos: RandomInt64ChildExecutionInfoMap(),
		RequestCancelInfos:  RandomInt64RequestCancelInfoMap(),
		SignalInfos:         RandomInt64SignalInfoMap(),
		SignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},

		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer:    {},
			tasks.CategoryTimer:       {},
			tasks.CategoryReplication: {},
			tasks.CategoryVisibility:  {},
		},

		Condition:       rand.Int63(),
		DBRecordVersion: dbRecordVersion,
	}
}

func RandomMutation(
	namespaceID string,
	workflowID string,
	runID string,
	lastWriteVersion int64,
	state enumsspb.WorkflowExecutionState,
	status enumspb.WorkflowExecutionStatus,
	dbRecordVersion int64,
	historyBranchUtil p.HistoryBranchUtil,
) *p.WorkflowMutation {
	mutation := &p.WorkflowMutation{
		ExecutionInfo:  RandomExecutionInfo(namespaceID, workflowID, lastWriteVersion, historyBranchUtil),
		ExecutionState: RandomExecutionState(runID, state, status),

		NextEventID: rand.Int63(),

		UpsertActivityInfos:       RandomInt64ActivityInfoMap(),
		DeleteActivityInfos:       map[int64]struct{}{rand.Int63(): {}},
		UpsertTimerInfos:          RandomStringTimerInfoMap(),
		DeleteTimerInfos:          map[string]struct{}{uuid.New().String(): {}},
		UpsertChildExecutionInfos: RandomInt64ChildExecutionInfoMap(),
		DeleteChildExecutionInfos: map[int64]struct{}{rand.Int63(): {}},
		UpsertRequestCancelInfos:  RandomInt64RequestCancelInfoMap(),
		DeleteRequestCancelInfos:  map[int64]struct{}{rand.Int63(): {}},
		UpsertSignalInfos:         RandomInt64SignalInfoMap(),
		DeleteSignalInfos:         map[int64]struct{}{rand.Int63(): {}},
		UpsertSignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		DeleteSignalRequestedIDs:  map[string]struct{}{uuid.New().String(): {}},
		// NewBufferedEvents: see below
		// ClearBufferedEvents: see below

		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer:    {},
			tasks.CategoryTimer:       {},
			tasks.CategoryReplication: {},
			tasks.CategoryVisibility:  {},
		},

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
		mutation.NewBufferedEvents = []*historypb.HistoryEvent{RandomHistoryEvent()}
	default:
		panic("broken test")
	}
	return mutation
}

func RandomExecutionInfo(
	namespaceID string,
	workflowID string,
	lastWriteVersion int64,
	historyBranchUtil p.HistoryBranchUtil,
) *persistencespb.WorkflowExecutionInfo {
	var executionInfo persistencespb.WorkflowExecutionInfo
	_ = fakedata.FakeStruct(&executionInfo)
	executionInfo.NamespaceId = namespaceID
	executionInfo.WorkflowId = workflowID
	executionInfo.VersionHistories = RandomVersionHistory(lastWriteVersion, historyBranchUtil)
	return &executionInfo
}

func RandomExecutionState(
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

func RandomInt64ActivityInfoMap() map[int64]*persistencespb.ActivityInfo {
	return map[int64]*persistencespb.ActivityInfo{
		rand.Int63(): RandomActivityInfo(),
	}
}

func RandomStringTimerInfoMap() map[string]*persistencespb.TimerInfo {
	return map[string]*persistencespb.TimerInfo{
		uuid.New().String(): RandomTimerInfo(),
	}
}

func RandomInt64ChildExecutionInfoMap() map[int64]*persistencespb.ChildExecutionInfo {
	return map[int64]*persistencespb.ChildExecutionInfo{
		rand.Int63(): RandomChildExecutionInfo(),
	}
}

func RandomInt64RequestCancelInfoMap() map[int64]*persistencespb.RequestCancelInfo {
	return map[int64]*persistencespb.RequestCancelInfo{
		rand.Int63(): RandomRequestCancelInfo(),
	}
}

func RandomInt64SignalInfoMap() map[int64]*persistencespb.SignalInfo {
	return map[int64]*persistencespb.SignalInfo{
		rand.Int63(): RandomSignalInfo(),
	}
}

func RandomActivityInfo() *persistencespb.ActivityInfo {
	var activityInfo persistencespb.ActivityInfo
	_ = fakedata.FakeStruct(&activityInfo)
	return &activityInfo
}

func RandomTimerInfo() *persistencespb.TimerInfo {
	var timerInfo persistencespb.TimerInfo
	_ = fakedata.FakeStruct(&timerInfo)
	return &timerInfo
}

func RandomChildExecutionInfo() *persistencespb.ChildExecutionInfo {
	var childExecutionInfo persistencespb.ChildExecutionInfo
	_ = fakedata.FakeStruct(&childExecutionInfo)
	return &childExecutionInfo
}

func RandomRequestCancelInfo() *persistencespb.RequestCancelInfo {
	var requestCancelInfo persistencespb.RequestCancelInfo
	_ = fakedata.FakeStruct(&requestCancelInfo)
	return &requestCancelInfo
}

func RandomSignalInfo() *persistencespb.SignalInfo {
	var signalInfo persistencespb.SignalInfo
	_ = fakedata.FakeStruct(&signalInfo)
	return &signalInfo
}

func RandomHistoryEvent() *historypb.HistoryEvent {
	var historyEvent historypb.HistoryEvent
	_ = fakedata.FakeStruct(&historyEvent)
	return &historyEvent
}

func RandomResetPoints() *workflowpb.ResetPoints {
	return &workflowpb.ResetPoints{Points: []*workflowpb.ResetPointInfo{{
		BinaryChecksum:               uuid.New().String(),
		RunId:                        uuid.New().String(),
		FirstWorkflowTaskCompletedId: rand.Int63(),
		CreateTime:                   RandomTime(),
		ExpireTime:                   RandomTime(),
		Resettable:                   rand.Int31()%2 == 0,
	}}}
}

func RandomStringPayloadMap() map[string]*commonpb.Payload {
	return map[string]*commonpb.Payload{
		uuid.New().String(): RandomPayload(),
	}
}

func RandomPayload() *commonpb.Payload {
	var payload commonpb.Payload
	_ = fakedata.FakeStruct(&payload)
	return &payload
}

func RandomVersionHistory(
	lastWriteVersion int64,
	historyBranchUtil p.HistoryBranchUtil,
) *historyspb.VersionHistories {
	randomBranchToken, _ := historyBranchUtil.NewHistoryBranch(
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
		nil,
		nil,
		0,
		0,
		0,
	)
	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: randomBranchToken,
			Items: []*historyspb.VersionHistoryItem{{
				EventId: rand.Int63(),
				Version: lastWriteVersion,
			}},
		}},
	}
}

func RandomTime() *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, rand.Int63()))
}

func RandomDuration() *durationpb.Duration {
	return durationpb.New(time.Duration(rand.Int63()))
}
