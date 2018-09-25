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
	"github.com/uber/cadence/common/persistence"
)

type (
	mutableStateSessionUpdates struct {
		executionInfo                    *persistence.WorkflowExecutionInfo
		newEventsBuilder                 *historyBuilder
		updateActivityInfos              []*persistence.ActivityInfo
		deleteActivityInfos              []int64
		updateTimerInfos                 []*persistence.TimerInfo
		deleteTimerInfos                 []string
		updateChildExecutionInfos        []*persistence.ChildExecutionInfo
		deleteChildExecutionInfo         *int64
		updateCancelExecutionInfos       []*persistence.RequestCancelInfo
		deleteCancelExecutionInfo        *int64
		updateSignalInfos                []*persistence.SignalInfo
		deleteSignalInfo                 *int64
		updateSignalRequestedIDs         []string
		deleteSignalRequestedID          string
		continueAsNew                    *persistence.CreateWorkflowExecutionRequest
		newBufferedEvents                *persistence.SerializedHistoryEventBatch
		clearBufferedEvents              bool
		newBufferedReplicationEventsInfo *persistence.BufferedReplicationTask
		deleteBufferedReplicationEvent   *int64
	}

	sessionUpdateStats struct {
		mutableStateSize int // Total size of mutable state update

		// Breakdown of mutable state size update for more granular stats
		executionInfoSize            int
		activityInfoSize             int
		timerInfoSize                int
		childInfoSize                int
		signalInfoSize               int
		bufferedEventsSize           int
		bufferedReplicationTasksSize int

		// Item counts in this session update
		activityInfoCount      int
		timerInfoCount         int
		childInfoCount         int
		signalInfoCount        int
		requestCancelInfoCount int

		// Deleted item counts in this session update
		deleteActivityInfoCount      int
		deleteTimerInfoCount         int
		deleteChildInfoCount         int
		deleteSignalInfoCount        int
		deleteRequestCancelInfoCount int
	}
)

func (s *mutableStateSessionUpdates) computeStats() *sessionUpdateStats {
	executionInfoSize := computeExecutionInfoSize(s.executionInfo)

	activityInfoCount := 0
	activityInfoSize := 0
	for _, ai := range s.updateActivityInfos {
		activityInfoCount++
		activityInfoSize += computeActivityInfoSize(ai)
	}

	timerInfoCount := 0
	timerInfoSize := 0
	for _, ti := range s.updateTimerInfos {
		timerInfoCount++
		timerInfoSize += computeTimerInfoSize(ti)
	}

	childExecutionInfoCount := 0
	childExecutionInfoSize := 0
	for _, ci := range s.updateChildExecutionInfos {
		childExecutionInfoCount++
		childExecutionInfoSize += computeChildInfoSize(ci)
	}

	signalInfoCount := 0
	signalInfoSize := 0
	for _, si := range s.updateSignalInfos {
		signalInfoCount++
		signalInfoSize += computeSignalInfoSize(si)
	}

	bufferedEventsSize := computeBufferedEventsSize(s.newBufferedEvents)

	bufferedReplicationTasksSize := computeBufferedReplicationTasksSize(s.newBufferedReplicationEventsInfo)

	requestCancelInfoCount := len(s.updateCancelExecutionInfos)

	deleteActivityInfoCount := len(s.deleteActivityInfos)

	deleteTimerInfoCount := len(s.deleteTimerInfos)

	deleteChildInfoCount := 0
	if s.deleteChildExecutionInfo != nil {
		deleteChildInfoCount = 1
	}

	deleteSignalInfoCount := 0
	if s.deleteSignalInfo != nil {
		deleteSignalInfoCount = 1
	}

	deleteRequestCancelInfoCount := 0
	if s.deleteCancelExecutionInfo != nil {
		deleteRequestCancelInfoCount = 1
	}

	totalSize := executionInfoSize
	totalSize += activityInfoSize
	totalSize += timerInfoSize
	totalSize += childExecutionInfoSize
	totalSize += signalInfoSize
	totalSize += bufferedEventsSize
	totalSize += bufferedReplicationTasksSize

	return &sessionUpdateStats{
		mutableStateSize:             totalSize,
		executionInfoSize:            executionInfoSize,
		activityInfoSize:             activityInfoSize,
		timerInfoSize:                timerInfoSize,
		childInfoSize:                childExecutionInfoSize,
		signalInfoSize:               signalInfoSize,
		bufferedEventsSize:           bufferedEventsSize,
		bufferedReplicationTasksSize: bufferedReplicationTasksSize,
		activityInfoCount:            activityInfoCount,
		timerInfoCount:               timerInfoCount,
		childInfoCount:               childExecutionInfoCount,
		signalInfoCount:              signalInfoCount,
		requestCancelInfoCount:       requestCancelInfoCount,
		deleteActivityInfoCount:      deleteActivityInfoCount,
		deleteTimerInfoCount:         deleteTimerInfoCount,
		deleteChildInfoCount:         deleteChildInfoCount,
		deleteSignalInfoCount:        deleteSignalInfoCount,
		deleteRequestCancelInfoCount: deleteRequestCancelInfoCount,
	}
}

func computeExecutionInfoSize(executionInfo *persistence.WorkflowExecutionInfo) int {
	size := len(executionInfo.WorkflowID)
	size += len(executionInfo.TaskList)
	size += len(executionInfo.WorkflowTypeName)
	size += len(executionInfo.ParentWorkflowID)

	return size
}

func computeActivityInfoSize(ai *persistence.ActivityInfo) int {
	size := len(ai.ActivityID)
	size += len(ai.ScheduledEvent)
	size += len(ai.StartedEvent)
	size += len(ai.Details)

	return size
}

func computeTimerInfoSize(ti *persistence.TimerInfo) int {
	size := len(ti.TimerID)

	return size
}

func computeChildInfoSize(ci *persistence.ChildExecutionInfo) int {
	size := len(ci.InitiatedEvent)
	size += len(ci.StartedEvent)

	return size
}

func computeSignalInfoSize(si *persistence.SignalInfo) int {
	size := len(si.SignalName)
	size += len(si.Input)
	size += len(si.Control)

	return size
}

func computeBufferedEventsSize(events *persistence.SerializedHistoryEventBatch) int {
	size := 0
	if events != nil {
		size = len(events.Data)
	}

	return size
}

func computeBufferedReplicationTasksSize(task *persistence.BufferedReplicationTask) int {
	size := 0

	if task != nil {
		if task.History != nil {
			size += len(task.History.Data)
		}

		if task.NewRunHistory != nil {
			size += len(task.NewRunHistory.Data)
		}
	}

	return size
}
