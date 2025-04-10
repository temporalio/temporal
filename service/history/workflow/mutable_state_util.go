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

package workflow

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

func convertSyncActivityInfos(
	now time.Time,
	workflowKey definition.WorkflowKey,
	activityInfos map[int64]*persistencespb.ActivityInfo,
	inputs map[int64]struct{},
) []tasks.Task {
	outputs := make([]tasks.Task, 0, len(inputs))
	for item := range inputs {
		activityInfo, ok := activityInfos[item]
		if ok {
			outputs = append(outputs, &tasks.SyncActivityTask{
				WorkflowKey:         workflowKey,
				Version:             activityInfo.Version,
				ScheduledEventID:    activityInfo.ScheduledEventId,
				VisibilityTimestamp: now,
			})
		}
	}
	return outputs
}

func SanitizeMutableState(
	workflowMutableState *persistencespb.WorkflowMutableState,
) {
	// Some values stored in mutable state are cluster or shard specific.
	// E.g task status (if task is created or not), taskID (derived from shard rangeID), txnID (derived from shard rangeID), etc.
	// Those fields should not be replicated across clusters and should be sanitized.
	sanitizeExecutionInfo(workflowMutableState.ExecutionInfo)

	sanitizeChildExecutionInfo(workflowMutableState.ChildExecutionInfos)

	rootNode := persistencespb.StateMachineNode{
		Children: workflowMutableState.ExecutionInfo.SubStateMachinesByType,
	}
	SanitizeStateMachineNode(&rootNode)
}

func SanitizeMutableStateMutation(
	mutableStateMutation *persistencespb.WorkflowMutableStateMutation,
) {
	sanitizeExecutionInfo(mutableStateMutation.ExecutionInfo)
	sanitizeChildExecutionInfo(mutableStateMutation.UpdatedChildExecutionInfos)
}

func sanitizeExecutionInfo(
	executionInfo *persistencespb.WorkflowExecutionInfo,
) {
	executionInfo.WorkflowExecutionTimerTaskStatus = TimerTaskStatusNone
	executionInfo.LastFirstEventTxnId = common.EmptyEventTaskID
	executionInfo.CloseVisibilityTaskId = common.EmptyEventTaskID
	executionInfo.CloseTransferTaskId = common.EmptyEventTaskID
	// TODO: after adding cluster to clock info, no need to reset clock here
	executionInfo.ParentClock = nil

	// Timer tasks are generated locally, do not sync them.
	executionInfo.StateMachineTimers = nil
	executionInfo.TaskGenerationShardClockTimestamp = common.EmptyEventTaskID
}

func sanitizeChildExecutionInfo(
	pendingChildInfo map[int64]*persistencespb.ChildExecutionInfo,
) {
	for _, childExecutionInfo := range pendingChildInfo {
		childExecutionInfo.Clock = nil
	}
}

func SanitizeStateMachineNode(
	node *persistencespb.StateMachineNode,
) {
	if node == nil {
		return
	}

	// Node TransitionCount is cluster local information used for detecting staleness.
	// Reset it to 1 since we are creating a new mutable state.
	node.TransitionCount = 1

	for _, stateMachineMap := range node.Children {
		for _, childNode := range stateMachineMap.MachinesById {
			SanitizeStateMachineNode(childNode)
		}
	}
}
