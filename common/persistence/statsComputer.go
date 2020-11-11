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

import persistencespb "go.temporal.io/server/api/persistence/v1"

type (
	// statsComputer is to computing struct sizes after serialization
	statsComputer struct{}
)

func (sc *statsComputer) computeMutableStateStats(req *InternalGetWorkflowExecutionResponse) *MutableStateStats {
	executionInfoSize := computeExecutionInfoSize(req.State.ExecutionInfo)

	activityInfoCount := 0
	activityInfoSize := 0
	for _, ai := range req.State.ActivityInfos {
		activityInfoCount++
		activityInfoSize += computeActivityInfoSize(ai)
	}

	timerInfoCount := 0
	timerInfoSize := 0
	for _, ti := range req.State.TimerInfos {
		timerInfoCount++
		timerInfoSize += computeTimerInfoSize(ti)
	}

	childExecutionInfoCount := 0
	childExecutionInfoSize := 0
	for _, ci := range req.State.ChildExecutionInfos {
		childExecutionInfoCount++
		childExecutionInfoSize += computeChildInfoSize(ci)
	}

	signalInfoCount := 0
	signalInfoSize := 0
	for _, si := range req.State.SignalInfos {
		signalInfoCount++
		signalInfoSize += computeSignalInfoSize(si)
	}

	bufferedEventsCount := 0
	bufferedEventsSize := 0

	for _, be := range req.State.BufferedEvents {
		bufferedEventsCount++

		if be == nil {
			continue
		}

		bufferedEventsSize += len(be.Data)
	}

	requestCancelInfoCount := len(req.State.RequestCancelInfos)

	totalSize := executionInfoSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += signalInfoSize
	totalSize += bufferedEventsSize

	return &MutableStateStats{
		MutableStateSize:       totalSize,
		ExecutionInfoSize:      executionInfoSize,
		ActivityInfoSize:       activityInfoSize,
		TimerInfoSize:          timerInfoSize,
		ChildInfoSize:          childExecutionInfoSize,
		SignalInfoSize:         signalInfoSize,
		BufferedEventsSize:     bufferedEventsSize,
		ActivityInfoCount:      activityInfoCount,
		TimerInfoCount:         timerInfoCount,
		ChildInfoCount:         childExecutionInfoCount,
		SignalInfoCount:        signalInfoCount,
		BufferedEventsCount:    bufferedEventsCount,
		RequestCancelInfoCount: requestCancelInfoCount,
	}
}

func (sc *statsComputer) computeMutableStateUpdateStats(req *InternalUpdateWorkflowExecutionRequest) *MutableStateUpdateSessionStats {
	executionInfoSize := computeExecutionInfoSize(req.UpdateWorkflowMutation.ExecutionInfo)

	activityInfoCount := 0
	activityInfoSize := 0
	for _, ai := range req.UpdateWorkflowMutation.UpsertActivityInfos {
		activityInfoCount++
		activityInfoSize += computeActivityInfoSize(ai)
	}

	timerInfoCount := 0
	timerInfoSize := 0
	for _, ti := range req.UpdateWorkflowMutation.UpsertTimerInfos {
		timerInfoCount++
		timerInfoSize += computeTimerInfoSize(ti)
	}

	childExecutionInfoCount := 0
	childExecutionInfoSize := 0
	for _, ci := range req.UpdateWorkflowMutation.UpsertChildExecutionInfos {
		childExecutionInfoCount++
		childExecutionInfoSize += computeChildInfoSize(ci)
	}

	signalInfoCount := 0
	signalInfoSize := 0
	for _, si := range req.UpdateWorkflowMutation.UpsertSignalInfos {
		signalInfoCount++
		signalInfoSize += computeSignalInfoSize(si)
	}

	bufferedEventsSize := 0
	if req.UpdateWorkflowMutation.NewBufferedEvents != nil {
		bufferedEventsSize = len(req.UpdateWorkflowMutation.NewBufferedEvents.Data)
	}

	requestCancelInfoCount := len(req.UpdateWorkflowMutation.UpsertRequestCancelInfos)

	deleteActivityInfoCount := len(req.UpdateWorkflowMutation.DeleteActivityInfos)

	deleteTimerInfoCount := len(req.UpdateWorkflowMutation.DeleteTimerInfos)

	deleteChildInfoCount := len(req.UpdateWorkflowMutation.DeleteChildExecutionInfos)

	deleteSignalInfoCount := len(req.UpdateWorkflowMutation.DeleteSignalInfos)

	deleteRequestCancelInfoCount := len(req.UpdateWorkflowMutation.DeleteRequestCancelInfos)

	totalSize := executionInfoSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += signalInfoSize
	totalSize += bufferedEventsSize

	return &MutableStateUpdateSessionStats{
		MutableStateSize:             totalSize,
		ExecutionInfoSize:            executionInfoSize,
		ActivityInfoSize:             activityInfoSize,
		TimerInfoSize:                timerInfoSize,
		ChildInfoSize:                childExecutionInfoSize,
		SignalInfoSize:               signalInfoSize,
		BufferedEventsSize:           bufferedEventsSize,
		ActivityInfoCount:            activityInfoCount,
		TimerInfoCount:               timerInfoCount,
		ChildInfoCount:               childExecutionInfoCount,
		SignalInfoCount:              signalInfoCount,
		RequestCancelInfoCount:       requestCancelInfoCount,
		DeleteActivityInfoCount:      deleteActivityInfoCount,
		DeleteTimerInfoCount:         deleteTimerInfoCount,
		DeleteChildInfoCount:         deleteChildInfoCount,
		DeleteSignalInfoCount:        deleteSignalInfoCount,
		DeleteRequestCancelInfoCount: deleteRequestCancelInfoCount,
	}
}

func computeExecutionInfoSize(executionInfo *persistencespb.WorkflowExecutionInfo) int {
	if executionInfo == nil {
		return 0
	}

	size := len(executionInfo.WorkflowId)
	size += len(executionInfo.TaskQueue)
	size += len(executionInfo.WorkflowTypeName)
	size += len(executionInfo.ParentWorkflowId)

	return size
}

func computeActivityInfoSize(ai *persistencespb.ActivityInfo) int {
	if ai == nil {
		return 0
	}

	size := len(ai.ActivityId)
	if ai.ScheduledEvent != nil {
		size += ai.ScheduledEvent.Size()
	}
	if ai.StartedEvent != nil {
		size += ai.StartedEvent.Size()
	}
	if ai.LastHeartbeatDetails != nil {
		size += ai.LastHeartbeatDetails.Size()
	}

	return size
}

func computeTimerInfoSize(ti *persistencespb.TimerInfo) int {
	if ti == nil {
		return 0
	}

	size := len(ti.GetTimerId())

	return size
}

func computeChildInfoSize(ci *persistencespb.ChildExecutionInfo) int {
	if ci == nil {
		return 0
	}
	size := 0
	if ci.InitiatedEvent != nil {
		size += ci.InitiatedEvent.Size()
	}
	if ci.StartedEvent != nil {
		size += ci.StartedEvent.Size()
	}
	return size
}

func computeSignalInfoSize(si *persistencespb.SignalInfo) int {
	if si == nil {
		return 0
	}

	size := len(si.Name)
	size += si.GetInput().Size()
	size += len(si.Control)

	return size
}
