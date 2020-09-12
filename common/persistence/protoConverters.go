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
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

func WorkflowExecutionToProto(executionInfo *WorkflowExecutionInfo, startVersion int64, versionHistories *history.VersionHistories) (*persistenceblobs.WorkflowExecutionInfo, *persistenceblobs.WorkflowExecutionState, error) {
	state := &persistenceblobs.WorkflowExecutionState{
		CreateRequestId: executionInfo.ExecutionState.CreateRequestId,
		State:           executionInfo.ExecutionState.State,
		Status:          executionInfo.ExecutionState.Status,
		RunId:           executionInfo.ExecutionState.RunId,
	}

	info := &persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:                       executionInfo.NamespaceId,
		WorkflowId:                        executionInfo.WorkflowId,
		FirstExecutionRunId:               executionInfo.FirstExecutionRunId,
		TaskQueue:                         executionInfo.TaskQueue,
		WorkflowTypeName:                  executionInfo.WorkflowTypeName,
		WorkflowRunTimeout:                executionInfo.WorkflowRunTimeout,
		WorkflowExecutionTimeout:          executionInfo.WorkflowExecutionTimeout,
		DefaultWorkflowTaskTimeout:        executionInfo.DefaultWorkflowTaskTimeout,
		LastFirstEventId:                  executionInfo.LastFirstEventId,
		LastEventTaskId:                   executionInfo.LastEventTaskId,
		LastProcessedEvent:                executionInfo.LastProcessedEvent,
		StartTime:                         executionInfo.StartTime,
		LastUpdateTime:                    executionInfo.LastUpdatedTime,
		WorkflowTaskVersion:               executionInfo.WorkflowTaskVersion,
		WorkflowTaskScheduleId:            executionInfo.WorkflowTaskScheduleId,
		WorkflowTaskStartedId:             executionInfo.WorkflowTaskStartedId,
		WorkflowTaskRequestId:             executionInfo.WorkflowTaskRequestId,
		WorkflowTaskTimeout:               executionInfo.WorkflowTaskTimeout,
		WorkflowTaskAttempt:               executionInfo.WorkflowTaskAttempt,
		WorkflowTaskStartedTime:           executionInfo.WorkflowTaskStartedTimestamp,
		WorkflowTaskScheduledTime:         executionInfo.WorkflowTaskScheduledTimestamp,
		WorkflowTaskOriginalScheduledTime: executionInfo.WorkflowTaskOriginalScheduledTimestamp,
		StickyTaskQueue:                   executionInfo.StickyTaskQueue,
		StickyScheduleToStartTimeout:      executionInfo.StickyScheduleToStartTimeout,
		ClientLibraryVersion:              executionInfo.ClientLibraryVersion,
		ClientFeatureVersion:              executionInfo.ClientFeatureVersion,
		ClientImpl:                        executionInfo.ClientImpl,
		SignalCount:                       executionInfo.SignalCount,
		CronSchedule:                      executionInfo.CronSchedule,
		CompletionEventBatchId:            executionInfo.CompletionEventBatchId,
		HasRetryPolicy:                    executionInfo.HasRetryPolicy,
		Attempt:                           executionInfo.Attempt,
		RetryInitialInterval:              executionInfo.RetryInitialInterval,
		RetryBackoffCoefficient:           executionInfo.RetryBackoffCoefficient,
		RetryMaximumInterval:              executionInfo.RetryMaximumInterval,
		RetryMaximumAttempts:              executionInfo.RetryMaximumAttempts,
		RetryNonRetryableErrorTypes:       executionInfo.RetryNonRetryableErrorTypes,
		EventStoreVersion:                 EventStoreVersion,
		EventBranchToken:                  executionInfo.EventBranchToken,
		AutoResetPoints:                   executionInfo.AutoResetPoints,
		SearchAttributes:                  executionInfo.SearchAttributes,
		Memo:                              executionInfo.Memo,
		CompletionEvent:                   executionInfo.CompletionEvent,
		// Dual write to move away from HistorySize in future
		HistorySize:    executionInfo.ExecutionStats.GetHistorySize(),
		ExecutionStats: executionInfo.ExecutionStats,
	}

	if !timestamp.TimeValue(executionInfo.WorkflowExpirationTime).IsZero() {
		info.RetryExpirationTime = timestamp.TimestampFromTimePtr(executionInfo.WorkflowExpirationTime).ToTime()
	}

	info.StartVersion = startVersion
	info.VersionHistories = versionHistories

	if executionInfo.ParentNamespaceId != "" {
		info.ParentNamespaceId = executionInfo.ParentNamespaceId
		info.ParentWorkflowId = executionInfo.ParentWorkflowId
		info.ParentRunId = executionInfo.ParentRunId
		info.InitiatedId = executionInfo.InitiatedId
		info.CompletionEvent = nil
	}

	if executionInfo.CancelRequested {
		info.CancelRequested = true
		info.CancelRequestId = executionInfo.CancelRequestId
	}
	return info, state, nil
}

func WorkflowExecutionFromProto(info *persistenceblobs.WorkflowExecutionInfo, state *persistenceblobs.WorkflowExecutionState, nextEventID int64) *WorkflowExecutionInfo {
	executionInfo := &WorkflowExecutionInfo{
		ExecutionState: &persistenceblobs.WorkflowExecutionState{
			State:  state.GetState(),
			Status: state.GetStatus(),
			CreateRequestId:                        state.GetCreateRequestId(),
			RunId:                                  state.GetRunId(),
		},
		NamespaceId:                            info.NamespaceId,
		WorkflowId:                             info.WorkflowId,
		FirstExecutionRunId:                    info.FirstExecutionRunId,
		NextEventId:                            nextEventID,
		TaskQueue:                              info.GetTaskQueue(),
		WorkflowTypeName:                       info.GetWorkflowTypeName(),
		WorkflowExecutionTimeout:               info.GetWorkflowExecutionTimeout(),
		WorkflowRunTimeout:                     info.GetWorkflowRunTimeout(),
		DefaultWorkflowTaskTimeout:             info.GetDefaultWorkflowTaskTimeout(),
		LastFirstEventId:                       info.GetLastFirstEventId(),
		LastProcessedEvent:                     info.GetLastProcessedEvent(),
		StartTime:                              info.GetStartTime(),
		LastUpdatedTime:                        info.GetLastUpdateTime(),
		WorkflowTaskVersion:                    info.GetWorkflowTaskVersion(),
		WorkflowTaskScheduleId:                 info.GetWorkflowTaskScheduleId(),
		WorkflowTaskStartedId:                  info.GetWorkflowTaskStartedId(),
		WorkflowTaskRequestId:                  info.GetWorkflowTaskRequestId(),
		WorkflowTaskTimeout:                    info.GetWorkflowTaskTimeout(),
		WorkflowTaskAttempt:                    info.GetWorkflowTaskAttempt(),
		WorkflowTaskStartedTimestamp:           info.GetWorkflowTaskStartedTime(),
		WorkflowTaskScheduledTimestamp:         info.GetWorkflowTaskScheduledTime(),
		WorkflowTaskOriginalScheduledTimestamp: info.GetWorkflowTaskOriginalScheduledTime(),
		StickyTaskQueue:                        info.GetStickyTaskQueue(),
		StickyScheduleToStartTimeout:           info.GetStickyScheduleToStartTimeout(),
		ClientLibraryVersion:                   info.GetClientLibraryVersion(),
		ClientFeatureVersion:                   info.GetClientFeatureVersion(),
		ClientImpl:                             info.GetClientImpl(),
		SignalCount:                            info.GetSignalCount(),
		ExecutionStats:                         info.GetExecutionStats(),
		CronSchedule:                           info.GetCronSchedule(),
		CompletionEventBatchId:                 info.GetCompletionEventBatchId(),
		HasRetryPolicy:                         info.GetHasRetryPolicy(),
		Attempt:                                info.GetAttempt(),
		RetryInitialInterval:                   info.GetRetryInitialInterval(),
		RetryBackoffCoefficient:                info.GetRetryBackoffCoefficient(),
		RetryMaximumInterval:                   info.GetRetryMaximumInterval(),
		RetryMaximumAttempts:                   info.GetRetryMaximumAttempts(),
		EventBranchToken:                       info.GetEventBranchToken(),
		RetryNonRetryableErrorTypes:            info.GetRetryNonRetryableErrorTypes(),
		SearchAttributes:                       info.GetSearchAttributes(),
		Memo:                                   info.GetMemo(),
		CompletionEvent:                        info.GetCompletionEvent(),
		AutoResetPoints:                        info.GetAutoResetPoints(),
	}

	// Back compat for GetHistorySize
	if info.GetHistorySize() >= 0 {
		executionInfo.ExecutionStats = &persistenceblobs.ExecutionStats{HistorySize: info.GetHistorySize()}
	}

	if executionInfo.ExecutionStats == nil {
		executionInfo.ExecutionStats = &persistenceblobs.ExecutionStats{}
	}

	if info.GetRetryExpirationTime() != nil {
		executionInfo.WorkflowExpirationTime = info.GetRetryExpirationTime()
	}

	if info.ParentNamespaceId != "" {
		executionInfo.ParentNamespaceId = info.ParentNamespaceId
		executionInfo.ParentWorkflowId = info.GetParentWorkflowId()
		executionInfo.ParentRunId = info.ParentRunId
		executionInfo.InitiatedId = info.GetInitiatedId()
		if executionInfo.CompletionEvent != nil {
			executionInfo.CompletionEvent = nil
		}
	}

	if info.GetCancelRequested() {
		executionInfo.CancelRequested = true
		executionInfo.CancelRequestId = info.GetCancelRequestId()
	}

	return executionInfo
}
