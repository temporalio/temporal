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
		CreateRequestId: executionInfo.CreateRequestID,
		State:           executionInfo.State,
		Status:          executionInfo.Status,
		RunId:           executionInfo.RunID,
	}

	info := &persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:                 executionInfo.NamespaceID,
		WorkflowId:                  executionInfo.WorkflowID,
		FirstExecutionRunId:               executionInfo.FirstExecutionRunID,
		TaskQueue:                         executionInfo.TaskQueue,
		WorkflowTypeName:                  executionInfo.WorkflowTypeName,
		WorkflowRunTimeout:                executionInfo.WorkflowRunTimeout,
		WorkflowExecutionTimeout:          executionInfo.WorkflowExecutionTimeout,
		DefaultWorkflowTaskTimeout:        executionInfo.DefaultWorkflowTaskTimeout,
		LastFirstEventId:                  executionInfo.LastFirstEventID,
		LastEventTaskId:                   executionInfo.LastEventTaskID,
		LastProcessedEvent:                executionInfo.LastProcessedEvent,
		StartTime:                         executionInfo.StartTimestamp,
		LastUpdateTime:                    executionInfo.LastUpdatedTimestamp,
		WorkflowTaskVersion:               executionInfo.WorkflowTaskVersion,
		WorkflowTaskScheduleId:            executionInfo.WorkflowTaskScheduleID,
		WorkflowTaskStartedId:             executionInfo.WorkflowTaskStartedID,
		WorkflowTaskRequestId:             executionInfo.WorkflowTaskRequestID,
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
		SignalCount:                 executionInfo.SignalCount,
		CronSchedule:                executionInfo.CronSchedule,
		CompletionEventBatchId:      executionInfo.CompletionEventBatchID,
		HasRetryPolicy:              executionInfo.HasRetryPolicy,
		RetryAttempt:                executionInfo.Attempt,
		RetryInitialInterval:        executionInfo.InitialInterval,
		RetryBackoffCoefficient:     executionInfo.BackoffCoefficient,
		RetryMaximumInterval:        executionInfo.MaximumInterval,
		RetryMaximumAttempts:        executionInfo.MaximumAttempts,
		RetryNonRetryableErrorTypes: executionInfo.NonRetryableErrorTypes,
		EventStoreVersion:           EventStoreVersion,
		EventBranchToken:            executionInfo.BranchToken,
		AutoResetPoints:             executionInfo.AutoResetPoints,
		SearchAttributes:            executionInfo.SearchAttributes,
		Memo:                        executionInfo.Memo,
		CompletionEvent:             executionInfo.CompletionEvent,
		// Dual write to move away from HistorySize in future
		HistorySize:    executionInfo.ExecutionStats.GetHistorySize(),
		ExecutionStats: executionInfo.ExecutionStats,
	}

	if !timestamp.TimeValue(executionInfo.WorkflowExpirationTime).IsZero() {
		info.RetryExpirationTime = timestamp.TimestampFromTimePtr(executionInfo.WorkflowExpirationTime).ToTime()
	}

	info.StartVersion = startVersion
	info.VersionHistories = versionHistories

	if executionInfo.ParentNamespaceID != "" {
		info.ParentNamespaceId = executionInfo.ParentNamespaceID
		info.ParentWorkflowId = executionInfo.ParentWorkflowID
		info.ParentRunId = executionInfo.ParentRunID
		info.InitiatedId = executionInfo.InitiatedID
		info.CompletionEvent = nil
	}

	if executionInfo.CancelRequested {
		info.CancelRequested = true
		info.CancelRequestId = executionInfo.CancelRequestID
	}
	return info, state, nil
}

func WorkflowExecutionFromProto(info *persistenceblobs.WorkflowExecutionInfo, state *persistenceblobs.WorkflowExecutionState, nextEventID int64) *WorkflowExecutionInfo {
	executionInfo := &WorkflowExecutionInfo{
		NamespaceID:                            info.NamespaceId,
		WorkflowID:                             info.WorkflowId,
		RunID:                                  state.RunId,
		FirstExecutionRunID:                    info.FirstExecutionRunId,
		NextEventID:                            nextEventID,
		TaskQueue:                              info.GetTaskQueue(),
		WorkflowTypeName:                       info.GetWorkflowTypeName(),
		WorkflowExecutionTimeout:               info.GetWorkflowExecutionTimeout(),
		WorkflowRunTimeout:                     info.GetWorkflowRunTimeout(),
		DefaultWorkflowTaskTimeout:             info.GetDefaultWorkflowTaskTimeout(),
		State:                                  state.GetState(),
		Status:                                 state.GetStatus(),
		LastFirstEventID:                       info.GetLastFirstEventId(),
		LastProcessedEvent:                     info.GetLastProcessedEvent(),
		StartTimestamp:                         info.GetStartTime(),
		LastUpdatedTimestamp:                   info.GetLastUpdateTime(),
		CreateRequestID:                        state.GetCreateRequestId(),
		WorkflowTaskVersion:                    info.GetWorkflowTaskVersion(),
		WorkflowTaskScheduleID:                 info.GetWorkflowTaskScheduleId(),
		WorkflowTaskStartedID:                  info.GetWorkflowTaskStartedId(),
		WorkflowTaskRequestID:                  info.GetWorkflowTaskRequestId(),
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
		CompletionEventBatchID:                 info.GetCompletionEventBatchId(),
		HasRetryPolicy:                         info.GetHasRetryPolicy(),
		Attempt:                                info.GetRetryAttempt(),
		InitialInterval:                        info.GetRetryInitialInterval(),
		BackoffCoefficient:                     info.GetRetryBackoffCoefficient(),
		MaximumInterval:                        info.GetRetryMaximumInterval(),
		MaximumAttempts:                        info.GetRetryMaximumAttempts(),
		BranchToken:                            info.GetEventBranchToken(),
		NonRetryableErrorTypes:                 info.GetRetryNonRetryableErrorTypes(),
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
		executionInfo.ParentNamespaceID = info.ParentNamespaceId
		executionInfo.ParentWorkflowID = info.GetParentWorkflowId()
		executionInfo.ParentRunID = info.ParentRunId
		executionInfo.InitiatedID = info.GetInitiatedId()
		if executionInfo.CompletionEvent != nil {
			executionInfo.CompletionEvent = nil
		}
	}

	if info.GetCancelRequested() {
		executionInfo.CancelRequested = true
		executionInfo.CancelRequestID = info.GetCancelRequestId()
	}

	return executionInfo
}
