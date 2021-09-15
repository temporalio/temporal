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

func statusOfInternalWorkflow(
	state *InternalWorkflowMutableState,
) InternalWorkflowStatus {
	executionInfoSize := sizeOfExecutionInfo(state.ExecutionInfo)
	executionStateSize := sizeOfExecutionState(state.ExecutionState)

	activityInfoCount := len(state.ActivityInfos)
	activityInfoSize := sizeOfActivityInfos(state.ActivityInfos)

	timerInfoCount := len(state.TimerInfos)
	timerInfoSize := sizeOfTimerInfos(state.TimerInfos)

	childExecutionInfoCount := len(state.ChildExecutionInfos)
	childExecutionInfoSize := sizeOfChildWorkflowInfos(state.ChildExecutionInfos)

	requestCancelInfoCount := len(state.RequestCancelInfos)
	requestCancelInfoSize := sizeOfRequestCancelInfos(state.RequestCancelInfos)

	signalInfoCount := len(state.SignalInfos)
	signalInfoSize := sizeOfSignalInfos(state.SignalInfos)

	signalRequestIDCount := len(state.SignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSlice(state.SignalRequestedIDs)

	bufferedEventsCount := len(state.BufferedEvents)
	bufferedEventsSize := sizeOfBlobSlice(state.BufferedEvents)

	totalSize := executionInfoSize
	totalSize += executionStateSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += requestCancelInfoSize
	totalSize += signalInfoSize
	totalSize += signalRequestIDSize
	totalSize += bufferedEventsSize

	return InternalWorkflowStatus{
		TotalSize: totalSize,

		ExecutionInfoSize:  executionInfoSize,
		ExecutionStateSize: executionStateSize,

		ActivityInfoSize:  activityInfoSize,
		ActivityInfoCount: activityInfoCount,

		TimerInfoSize:  timerInfoSize,
		TimerInfoCount: timerInfoCount,

		ChildInfoSize:  childExecutionInfoSize,
		ChildInfoCount: childExecutionInfoCount,

		RequestCancelInfoSize:  requestCancelInfoSize,
		RequestCancelInfoCount: requestCancelInfoCount,

		SignalInfoSize:  signalInfoSize,
		SignalInfoCount: signalInfoCount,

		SignalRequestIDSize:  signalRequestIDSize,
		SignalRequestIDCount: signalRequestIDCount,

		BufferedEventsSize:  bufferedEventsSize,
		BufferedEventsCount: bufferedEventsCount,
	}
}

func statusOfInternalWorkflowMutation(
	mutation *InternalWorkflowMutation,
) InternalWorkflowStatus {
	executionInfoSize := sizeOfExecutionInfo(mutation.ExecutionInfo)
	executionStateSize := sizeOfExecutionState(mutation.ExecutionStateBlob)

	activityInfoCount := len(mutation.UpsertActivityInfos)
	activityInfoCount += len(mutation.DeleteActivityInfos)
	activityInfoSize := sizeOfActivityInfos(mutation.UpsertActivityInfos)
	activityInfoSize += sizeOfActivityInfoIDs(mutation.DeleteActivityInfos)

	timerInfoCount := len(mutation.UpsertTimerInfos)
	timerInfoCount += len(mutation.DeleteTimerInfos)
	timerInfoSize := sizeOfTimerInfos(mutation.UpsertTimerInfos)
	timerInfoSize += sizeOfTimerInfoIDs(mutation.DeleteTimerInfos)

	childExecutionInfoCount := len(mutation.UpsertChildExecutionInfos)
	childExecutionInfoCount += len(mutation.DeleteChildExecutionInfos)
	childExecutionInfoSize := sizeOfChildWorkflowInfos(mutation.UpsertChildExecutionInfos)
	childExecutionInfoSize += sizeOfChildWorkflowInfoIDs(mutation.DeleteChildExecutionInfos)

	requestCancelInfoCount := len(mutation.UpsertRequestCancelInfos)
	requestCancelInfoCount += len(mutation.DeleteRequestCancelInfos)
	requestCancelInfoSize := sizeOfRequestCancelInfos(mutation.UpsertRequestCancelInfos)
	requestCancelInfoSize += sizeOfRequestCancelInfoIDs(mutation.DeleteRequestCancelInfos)

	signalInfoCount := len(mutation.UpsertSignalInfos)
	signalInfoCount += len(mutation.DeleteSignalInfos)
	signalInfoSize := sizeOfSignalInfos(mutation.UpsertSignalInfos)
	signalInfoSize += sizeOfSignalInfoIDs(mutation.DeleteSignalInfos)

	signalRequestIDCount := len(mutation.UpsertSignalRequestedIDs)
	signalRequestIDCount += len(mutation.DeleteSignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSet(mutation.UpsertSignalRequestedIDs)
	signalRequestIDSize += sizeOfStringSet(mutation.DeleteSignalRequestedIDs)

	bufferedEventsCount := 0
	bufferedEventsSize := 0
	if mutation.NewBufferedEvents != nil {
		bufferedEventsCount = 1
		bufferedEventsSize = mutation.NewBufferedEvents.Size()
	}

	// TODO what about tasks?
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

	return InternalWorkflowStatus{
		TotalSize: totalSize,

		ExecutionInfoSize:  executionInfoSize,
		ExecutionStateSize: executionStateSize,

		ActivityInfoSize:  activityInfoSize,
		ActivityInfoCount: activityInfoCount,

		TimerInfoSize:  timerInfoSize,
		TimerInfoCount: timerInfoCount,

		ChildInfoSize:  childExecutionInfoSize,
		ChildInfoCount: childExecutionInfoCount,

		RequestCancelInfoSize:  requestCancelInfoSize,
		RequestCancelInfoCount: requestCancelInfoCount,

		SignalInfoSize:  signalInfoSize,
		SignalInfoCount: signalInfoCount,

		SignalRequestIDSize:  signalRequestIDSize,
		SignalRequestIDCount: signalRequestIDCount,

		BufferedEventsSize:  bufferedEventsSize,
		BufferedEventsCount: bufferedEventsCount,
	}
}

func statusOfInternalWorkflowSnapshot(
	snapshot *InternalWorkflowSnapshot,
) InternalWorkflowStatus {
	executionInfoSize := sizeOfExecutionInfo(snapshot.ExecutionInfo)
	executionStateSize := sizeOfExecutionState(snapshot.ExecutionStateBlob)

	activityInfoCount := len(snapshot.ActivityInfos)
	activityInfoSize := sizeOfActivityInfos(snapshot.ActivityInfos)

	timerInfoCount := len(snapshot.TimerInfos)
	timerInfoSize := sizeOfTimerInfos(snapshot.TimerInfos)

	childExecutionInfoCount := len(snapshot.ChildExecutionInfos)
	childExecutionInfoSize := sizeOfChildWorkflowInfos(snapshot.ChildExecutionInfos)

	requestCancelInfoCount := len(snapshot.RequestCancelInfos)
	requestCancelInfoSize := sizeOfRequestCancelInfos(snapshot.RequestCancelInfos)

	signalInfoCount := len(snapshot.SignalInfos)
	signalInfoSize := sizeOfSignalInfos(snapshot.SignalInfos)

	signalRequestIDCount := len(snapshot.SignalRequestedIDs)
	signalRequestIDSize := sizeOfStringSet(snapshot.SignalRequestedIDs)

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

	return InternalWorkflowStatus{
		TotalSize: totalSize,

		ExecutionInfoSize:  executionInfoSize,
		ExecutionStateSize: executionStateSize,

		ActivityInfoSize:  activityInfoSize,
		ActivityInfoCount: activityInfoCount,

		TimerInfoSize:  timerInfoSize,
		TimerInfoCount: timerInfoCount,

		ChildInfoSize:  childExecutionInfoSize,
		ChildInfoCount: childExecutionInfoCount,

		RequestCancelInfoSize:  requestCancelInfoSize,
		RequestCancelInfoCount: requestCancelInfoCount,

		SignalInfoSize:  signalInfoSize,
		SignalInfoCount: signalInfoCount,

		SignalRequestIDSize:  signalRequestIDSize,
		SignalRequestIDCount: signalRequestIDCount,

		BufferedEventsSize:  bufferedEventsSize,
		BufferedEventsCount: bufferedEventsCount,
	}
}
