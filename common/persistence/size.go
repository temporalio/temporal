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

	payloadSize := sizeOfStringPayloadMap(state.ExecutionInfo.Memo)
	payloadSize += sizeOfStringPayloadMap(state.ExecutionInfo.SearchAttributes)
	payloadSize += getActivityPayloadSize(state.ActivityInfos)
	payloadSize += sizeOfBlobSlice(internalState.BufferedEvents)

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
		PayloadSize:       payloadSize,
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
	internalMutation *InternalWorkflowMutation,
	mutation *WorkflowMutation,
	historyStatistics *HistoryStatistics,
) *MutableStateStatistics {
	if internalMutation == nil {
		return nil
	}

	executionInfoSize := sizeOfBlob(internalMutation.ExecutionInfoBlob)
	executionStateSize := sizeOfBlob(internalMutation.ExecutionStateBlob)

	totalActivityCount := internalMutation.ExecutionInfo.ActivityCount
	activityInfoCount := len(internalMutation.UpsertActivityInfos)
	activityInfoCount += len(internalMutation.DeleteActivityInfos)
	activityInfoSize := sizeOfInt64BlobMap(internalMutation.UpsertActivityInfos)
	activityInfoSize += sizeOfInt64Set(internalMutation.DeleteActivityInfos)

	totalUserTimerCount := internalMutation.ExecutionInfo.UserTimerCount
	timerInfoCount := len(internalMutation.UpsertTimerInfos)
	timerInfoCount += len(internalMutation.DeleteTimerInfos)
	timerInfoSize := sizeOfStringBlobMap(internalMutation.UpsertTimerInfos)
	timerInfoSize += sizeOfStringSet(internalMutation.DeleteTimerInfos)

	totalChildExecutionCount := internalMutation.ExecutionInfo.ChildExecutionCount
	childExecutionInfoCount := len(internalMutation.UpsertChildExecutionInfos)
	childExecutionInfoCount += len(internalMutation.DeleteChildExecutionInfos)
	childExecutionInfoSize := sizeOfInt64BlobMap(internalMutation.UpsertChildExecutionInfos)
	childExecutionInfoSize += sizeOfInt64Set(internalMutation.DeleteChildExecutionInfos)

	totalRequestCancelExternalCount := internalMutation.ExecutionInfo.RequestCancelExternalCount
	requestCancelInfoCount := len(internalMutation.UpsertRequestCancelInfos)
	requestCancelInfoCount += len(internalMutation.DeleteRequestCancelInfos)
	requestCancelInfoSize := sizeOfInt64BlobMap(internalMutation.UpsertRequestCancelInfos)
	requestCancelInfoSize += sizeOfInt64Set(internalMutation.DeleteRequestCancelInfos)

	totalSignalExternalCount := internalMutation.ExecutionInfo.SignalExternalCount
	signalInfoCount := len(internalMutation.UpsertSignalInfos)
	signalInfoCount += len(internalMutation.DeleteSignalInfos)
	signalInfoSize := sizeOfInt64BlobMap(internalMutation.UpsertSignalInfos)
	signalInfoSize += sizeOfInt64Set(internalMutation.DeleteSignalInfos)

	totalSignalCount := internalMutation.ExecutionInfo.SignalCount
	signalRequestIDCount := len(internalMutation.UpsertSignalRequestedIDs)
	signalRequestIDCount += len(internalMutation.DeleteSignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSet(internalMutation.UpsertSignalRequestedIDs)
	signalRequestIDSize += sizeOfStringSet(internalMutation.DeleteSignalRequestedIDs)

	totalUpdateCount := internalMutation.ExecutionInfo.UpdateCount
	updateInfoCount := len(internalMutation.ExecutionInfo.UpdateInfos)

	bufferedEventsCount := 0
	bufferedEventsSize := 0
	if internalMutation.NewBufferedEvents != nil {
		bufferedEventsCount = 1
		bufferedEventsSize = internalMutation.NewBufferedEvents.Size()
	}

	taskCountByCategory := taskCountsByCategory(&internalMutation.Tasks)

	payloadSize := sizeOfStringPayloadMap(internalMutation.ExecutionInfo.Memo)
	payloadSize += sizeOfStringPayloadMap(internalMutation.ExecutionInfo.SearchAttributes)
	payloadSize += getActivityPayloadSize(mutation.UpsertActivityInfos)
	payloadSize += sizeOfBlob(internalMutation.NewBufferedEvents)

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
		PayloadSize:       payloadSize,
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
	internalSnapshot *InternalWorkflowSnapshot,
	snapshot *WorkflowSnapshot,
	historyStatistics *HistoryStatistics,
) *MutableStateStatistics {
	if internalSnapshot == nil {
		return nil
	}

	executionInfoSize := sizeOfBlob(internalSnapshot.ExecutionInfoBlob)
	executionStateSize := sizeOfBlob(internalSnapshot.ExecutionStateBlob)

	totalActivityCount := internalSnapshot.ExecutionInfo.ActivityCount
	activityInfoCount := len(internalSnapshot.ActivityInfos)
	activityInfoSize := sizeOfInt64BlobMap(internalSnapshot.ActivityInfos)

	totalUserTimerCount := internalSnapshot.ExecutionInfo.UserTimerCount
	timerInfoCount := len(internalSnapshot.TimerInfos)
	timerInfoSize := sizeOfStringBlobMap(internalSnapshot.TimerInfos)

	totalChildExecutionCount := internalSnapshot.ExecutionInfo.ChildExecutionCount
	childExecutionInfoCount := len(internalSnapshot.ChildExecutionInfos)
	childExecutionInfoSize := sizeOfInt64BlobMap(internalSnapshot.ChildExecutionInfos)

	totalRequestCancelExternalCount := internalSnapshot.ExecutionInfo.RequestCancelExternalCount
	requestCancelInfoCount := len(internalSnapshot.RequestCancelInfos)
	requestCancelInfoSize := sizeOfInt64BlobMap(internalSnapshot.RequestCancelInfos)

	totalSignalExternalCount := internalSnapshot.ExecutionInfo.SignalExternalCount
	signalInfoCount := len(internalSnapshot.SignalInfos)
	signalInfoSize := sizeOfInt64BlobMap(internalSnapshot.SignalInfos)

	totalSignalCount := internalSnapshot.ExecutionInfo.SignalCount
	signalRequestIDCount := len(internalSnapshot.SignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSet(internalSnapshot.SignalRequestedIDs)

	totalUpdateCount := internalSnapshot.ExecutionInfo.UpdateCount
	updateInfoCount := len(internalSnapshot.ExecutionInfo.UpdateInfos)

	bufferedEventsCount := 0
	bufferedEventsSize := 0

	payloadSize := sizeOfStringPayloadMap(internalSnapshot.ExecutionInfo.Memo)
	payloadSize += sizeOfStringPayloadMap(internalSnapshot.ExecutionInfo.SearchAttributes)
	payloadSize += getActivityPayloadSize(snapshot.ActivityInfos)

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize

	taskCountByCategory := taskCountsByCategory(&internalSnapshot.Tasks)

	return &MutableStateStatistics{
		TotalSize:         totalSize,
		PayloadSize:       payloadSize,
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

func getActivityPayloadSize(activities map[int64]*persistencespb.ActivityInfo) int {
	size := 0
	for _, info := range activities {
		size += info.GetLastHeartbeatDetails().Size()
		size += info.GetRetryLastFailure().Size()
	}
	return size
}
