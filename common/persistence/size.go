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

package persistence

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/tasks"
)

func statusOfInternalWorkflow(
	internalState *InternalWorkflowMutableState,
	state *persistencespb.WorkflowMutableState,
	historyStatistics *HistoryStatistics,
) *MutableStateStatistics {
	if internalState == nil {
		return nil
	}

	executionInfoSize := sizeOfBlob(internalState.ExecutionInfo)
	executionStateSize := sizeOfBlob(internalState.ExecutionState)

	totalActivityCount := state.ExecutionInfo.ActivityCount
	activityInfoCount := len(internalState.ActivityInfos)
	activityInfoSize := sizeOfInt64BlobMap(internalState.ActivityInfos)

	totalUserTimerCount := state.ExecutionInfo.UserTimerCount
	timerInfoCount := len(internalState.TimerInfos)
	timerInfoSize := sizeOfStringBlobMap(internalState.TimerInfos)

	totalChildExecutionCount := state.ExecutionInfo.ChildExecutionCount
	childExecutionInfoCount := len(internalState.ChildExecutionInfos)
	childExecutionInfoSize := sizeOfInt64BlobMap(internalState.ChildExecutionInfos)

	totalRequestCancelExternalCount := state.ExecutionInfo.RequestCancelExternalCount
	requestCancelInfoCount := len(internalState.RequestCancelInfos)
	requestCancelInfoSize := sizeOfInt64BlobMap(internalState.RequestCancelInfos)

	totalSignalExternalCount := state.ExecutionInfo.SignalExternalCount
	signalInfoCount := len(internalState.SignalInfos)
	signalInfoSize := sizeOfInt64BlobMap(internalState.SignalInfos)

	totalSignalCount := state.ExecutionInfo.SignalCount
	signalRequestIDCount := len(internalState.SignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSlice(internalState.SignalRequestedIDs)

	bufferedEventsCount := len(internalState.BufferedEvents)
	bufferedEventsSize := sizeOfBlobSlice(internalState.BufferedEvents)

	totalUpdateCount := state.ExecutionInfo.UpdateCount
	updateInfoCount := len(state.ExecutionInfo.UpdateInfos)

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize

	return &MutableStateStatistics{
		TotalSize:         totalSize,
		HistoryStatistics: historyStatistics,

		ExecutionInfoSize:  executionInfoSize,
		ExecutionStateSize: executionStateSize,

		ActivityInfoSize:   activityInfoSize,
		ActivityInfoCount:  activityInfoCount,
		TotalActivityCount: totalActivityCount,

		TimerInfoSize:       timerInfoSize,
		TimerInfoCount:      timerInfoCount,
		TotalUserTimerCount: totalUserTimerCount,

		ChildInfoSize:            childExecutionInfoSize,
		ChildInfoCount:           childExecutionInfoCount,
		TotalChildExecutionCount: totalChildExecutionCount,

		RequestCancelInfoSize:           requestCancelInfoSize,
		RequestCancelInfoCount:          requestCancelInfoCount,
		TotalRequestCancelExternalCount: totalRequestCancelExternalCount,

		SignalInfoSize:           signalInfoSize,
		SignalInfoCount:          signalInfoCount,
		TotalSignalExternalCount: totalSignalExternalCount,

		SignalRequestIDSize:  signalRequestIDSize,
		SignalRequestIDCount: signalRequestIDCount,
		TotalSignalCount:     totalSignalCount,

		BufferedEventsSize:  bufferedEventsSize,
		BufferedEventsCount: bufferedEventsCount,

		UpdateInfoCount:  updateInfoCount,
		TotalUpdateCount: totalUpdateCount,
	}
}

func statusOfInternalWorkflowMutation(
	mutation *InternalWorkflowMutation,
	historyStatistics *HistoryStatistics,
) *MutableStateStatistics {
	if mutation == nil {
		return nil
	}

	executionInfoSize := sizeOfBlob(mutation.ExecutionInfoBlob)
	executionStateSize := sizeOfBlob(mutation.ExecutionStateBlob)

	totalActivityCount := mutation.ExecutionInfo.ActivityCount
	activityInfoCount := len(mutation.UpsertActivityInfos)
	activityInfoCount += len(mutation.DeleteActivityInfos)
	activityInfoSize := sizeOfInt64BlobMap(mutation.UpsertActivityInfos)
	activityInfoSize += sizeOfInt64Set(mutation.DeleteActivityInfos)

	totalUserTimerCount := mutation.ExecutionInfo.UserTimerCount
	timerInfoCount := len(mutation.UpsertTimerInfos)
	timerInfoCount += len(mutation.DeleteTimerInfos)
	timerInfoSize := sizeOfStringBlobMap(mutation.UpsertTimerInfos)
	timerInfoSize += sizeOfStringSet(mutation.DeleteTimerInfos)

	totalChildExecutionCount := mutation.ExecutionInfo.ChildExecutionCount
	childExecutionInfoCount := len(mutation.UpsertChildExecutionInfos)
	childExecutionInfoCount += len(mutation.DeleteChildExecutionInfos)
	childExecutionInfoSize := sizeOfInt64BlobMap(mutation.UpsertChildExecutionInfos)
	childExecutionInfoSize += sizeOfInt64Set(mutation.DeleteChildExecutionInfos)

	totalRequestCancelExternalCount := mutation.ExecutionInfo.RequestCancelExternalCount
	requestCancelInfoCount := len(mutation.UpsertRequestCancelInfos)
	requestCancelInfoCount += len(mutation.DeleteRequestCancelInfos)
	requestCancelInfoSize := sizeOfInt64BlobMap(mutation.UpsertRequestCancelInfos)
	requestCancelInfoSize += sizeOfInt64Set(mutation.DeleteRequestCancelInfos)

	totalSignalExternalCount := mutation.ExecutionInfo.SignalExternalCount
	signalInfoCount := len(mutation.UpsertSignalInfos)
	signalInfoCount += len(mutation.DeleteSignalInfos)
	signalInfoSize := sizeOfInt64BlobMap(mutation.UpsertSignalInfos)
	signalInfoSize += sizeOfInt64Set(mutation.DeleteSignalInfos)

	totalSignalCount := mutation.ExecutionInfo.SignalCount
	signalRequestIDCount := len(mutation.UpsertSignalRequestedIDs)
	signalRequestIDCount += len(mutation.DeleteSignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSet(mutation.UpsertSignalRequestedIDs)
	signalRequestIDSize += sizeOfStringSet(mutation.DeleteSignalRequestedIDs)

	totalUpdateCount := mutation.ExecutionInfo.UpdateCount
	updateInfoCount := len(mutation.ExecutionInfo.UpdateInfos)

	bufferedEventsCount := 0
	bufferedEventsSize := 0
	if mutation.NewBufferedEvents != nil {
		bufferedEventsCount = 1
		bufferedEventsSize = mutation.NewBufferedEvents.Size()
	}

	taskCountByCategory := taskCountsByCategory(&mutation.Tasks)

	// TODO what about checksum?

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize

	return &MutableStateStatistics{
		TotalSize:         totalSize,
		HistoryStatistics: historyStatistics,

		ExecutionInfoSize:  executionInfoSize,
		ExecutionStateSize: executionStateSize,

		ActivityInfoSize:   activityInfoSize,
		ActivityInfoCount:  activityInfoCount,
		TotalActivityCount: totalActivityCount,

		TimerInfoSize:       timerInfoSize,
		TimerInfoCount:      timerInfoCount,
		TotalUserTimerCount: totalUserTimerCount,

		ChildInfoSize:            childExecutionInfoSize,
		ChildInfoCount:           childExecutionInfoCount,
		TotalChildExecutionCount: totalChildExecutionCount,

		RequestCancelInfoSize:           requestCancelInfoSize,
		RequestCancelInfoCount:          requestCancelInfoCount,
		TotalRequestCancelExternalCount: totalRequestCancelExternalCount,

		SignalInfoSize:           signalInfoSize,
		SignalInfoCount:          signalInfoCount,
		TotalSignalExternalCount: totalSignalExternalCount,

		SignalRequestIDSize:  signalRequestIDSize,
		SignalRequestIDCount: signalRequestIDCount,
		TotalSignalCount:     totalSignalCount,

		BufferedEventsSize:  bufferedEventsSize,
		BufferedEventsCount: bufferedEventsCount,

		TaskCountByCategory: taskCountByCategory,

		TotalUpdateCount: totalUpdateCount,
		UpdateInfoCount:  updateInfoCount,
	}
}

func taskCountsByCategory(t *map[tasks.Category][]InternalHistoryTask) map[string]int {
	counts := make(map[string]int)
	for category, tasks := range *t {
		counts[category.Name()] = len(tasks)
	}
	return counts
}

func statusOfInternalWorkflowSnapshot(
	snapshot *InternalWorkflowSnapshot,
	historyStatistics *HistoryStatistics,
) *MutableStateStatistics {
	if snapshot == nil {
		return nil
	}

	executionInfoSize := sizeOfBlob(snapshot.ExecutionInfoBlob)
	executionStateSize := sizeOfBlob(snapshot.ExecutionStateBlob)

	totalActivityCount := snapshot.ExecutionInfo.ActivityCount
	activityInfoCount := len(snapshot.ActivityInfos)
	activityInfoSize := sizeOfInt64BlobMap(snapshot.ActivityInfos)

	totalUserTimerCount := snapshot.ExecutionInfo.UserTimerCount
	timerInfoCount := len(snapshot.TimerInfos)
	timerInfoSize := sizeOfStringBlobMap(snapshot.TimerInfos)

	totalChildExecutionCount := snapshot.ExecutionInfo.ChildExecutionCount
	childExecutionInfoCount := len(snapshot.ChildExecutionInfos)
	childExecutionInfoSize := sizeOfInt64BlobMap(snapshot.ChildExecutionInfos)

	totalRequestCancelExternalCount := snapshot.ExecutionInfo.RequestCancelExternalCount
	requestCancelInfoCount := len(snapshot.RequestCancelInfos)
	requestCancelInfoSize := sizeOfInt64BlobMap(snapshot.RequestCancelInfos)

	totalSignalExternalCount := snapshot.ExecutionInfo.SignalExternalCount
	signalInfoCount := len(snapshot.SignalInfos)
	signalInfoSize := sizeOfInt64BlobMap(snapshot.SignalInfos)

	totalSignalCount := snapshot.ExecutionInfo.SignalCount
	signalRequestIDCount := len(snapshot.SignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSet(snapshot.SignalRequestedIDs)

	totalUpdateCount := snapshot.ExecutionInfo.UpdateCount
	updateInfoCount := len(snapshot.ExecutionInfo.UpdateInfos)

	bufferedEventsCount := 0
	bufferedEventsSize := 0

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize

	taskCountByCategory := taskCountsByCategory(&snapshot.Tasks)

	return &MutableStateStatistics{
		TotalSize:         totalSize,
		HistoryStatistics: historyStatistics,

		ExecutionInfoSize:  executionInfoSize,
		ExecutionStateSize: executionStateSize,

		ActivityInfoSize:   activityInfoSize,
		ActivityInfoCount:  activityInfoCount,
		TotalActivityCount: totalActivityCount,

		TimerInfoSize:       timerInfoSize,
		TimerInfoCount:      timerInfoCount,
		TotalUserTimerCount: totalUserTimerCount,

		ChildInfoSize:            childExecutionInfoSize,
		ChildInfoCount:           childExecutionInfoCount,
		TotalChildExecutionCount: totalChildExecutionCount,

		RequestCancelInfoSize:           requestCancelInfoSize,
		RequestCancelInfoCount:          requestCancelInfoCount,
		TotalRequestCancelExternalCount: totalRequestCancelExternalCount,

		SignalInfoSize:           signalInfoSize,
		SignalInfoCount:          signalInfoCount,
		TotalSignalExternalCount: totalSignalExternalCount,

		SignalRequestIDSize:  signalRequestIDSize,
		SignalRequestIDCount: signalRequestIDCount,
		TotalSignalCount:     totalSignalCount,

		BufferedEventsSize:  bufferedEventsSize,
		BufferedEventsCount: bufferedEventsCount,

		TaskCountByCategory: taskCountByCategory,

		TotalUpdateCount: totalUpdateCount,
		UpdateInfoCount:  updateInfoCount,
	}
}
