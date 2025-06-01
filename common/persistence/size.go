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

	chasmTotalSize := sizeOfChasmNodeMap(internalState.ChasmNodes)

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize
	totalSize += chasmTotalSize

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

		ChasmTotalSize: chasmTotalSize,
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

	chasmTotalSize := sizeOfChasmNodeMap(mutation.UpsertChasmNodes)
	chasmTotalSize += sizeOfStringSet(mutation.DeleteChasmNodes)

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
	totalSize += chasmTotalSize

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

		ChasmTotalSize: chasmTotalSize,
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

	chasmTotalSize := sizeOfChasmNodeMap(snapshot.ChasmNodes)

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize
	totalSize += chasmTotalSize

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

		ChasmTotalSize: chasmTotalSize,
	}
}
