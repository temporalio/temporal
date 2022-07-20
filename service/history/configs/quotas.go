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

package configs

import (
	"go.temporal.io/server/common/quotas"
)

var (
	APIToPriority = map[string]int{
		"CloseShard":                             0,
		"GetShard":                               0,
		"DeleteWorkflowExecution":                0,
		"DescribeHistoryHost":                    0,
		"DescribeMutableState":                   0,
		"DescribeWorkflowExecution":              0,
		"GetDLQMessages":                         0,
		"GetDLQReplicationMessages":              0,
		"GetMutableState":                        0,
		"GetReplicationMessages":                 0,
		"MergeDLQMessages":                       0,
		"PollMutableState":                       0,
		"PurgeDLQMessages":                       0,
		"QueryWorkflow":                          0,
		"ReapplyEvents":                          0,
		"RebuildMutableState":                    0,
		"RecordActivityTaskHeartbeat":            0,
		"RecordActivityTaskStarted":              0,
		"RecordChildExecutionCompleted":          0,
		"VerifyChildExecutionCompletionRecorded": 0,
		"RecordWorkflowTaskStarted":              0,
		"RefreshWorkflowTasks":                   0,
		"RemoveSignalMutableState":               0,
		"RemoveTask":                             0,
		"ReplicateEventsV2":                      0,
		"RequestCancelWorkflowExecution":         0,
		"ResetStickyTaskQueue":                   0,
		"ResetWorkflowExecution":                 0,
		"RespondActivityTaskCanceled":            0,
		"RespondActivityTaskCompleted":           0,
		"RespondActivityTaskFailed":              0,
		"RespondWorkflowTaskCompleted":           0,
		"RespondWorkflowTaskFailed":              0,
		"ScheduleWorkflowTask":                   0,
		"VerifyFirstWorkflowTaskScheduled":       0,
		"SignalWithStartWorkflowExecution":       0,
		"SignalWorkflowExecution":                0,
		"StartWorkflowExecution":                 0,
		"SyncActivity":                           0,
		"SyncShardStatus":                        0,
		"TerminateWorkflowExecution":             0,
		"GenerateLastHistoryReplicationTasks":    0,
		"GetReplicationStatus":                   0,
		"DeleteWorkflowVisibilityRecord":         0,
		"UpdateWorkflow":                         0,
	}

	APIPriorities = map[int]struct{}{
		0: {},
	}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RateLimiter)
	for priority := range APIPriorities {
		rateLimiters[priority] = quotas.NewDefaultIncomingRateLimiter(rateFn)
	}
	return quotas.NewPriorityRateLimiter(APIToPriority, rateLimiters)
}
