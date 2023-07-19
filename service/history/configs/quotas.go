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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
)

const (
	// OperatorPriority is used to give precedence to calls coming from web UI or tctl
	OperatorPriority = 0
)

var (
	APIToPriority = map[string]int{
		"CloseShard":                             1,
		"GetShard":                               1,
		"DeleteWorkflowExecution":                1,
		"DescribeHistoryHost":                    1,
		"DescribeMutableState":                   1,
		"DescribeWorkflowExecution":              1,
		"GetDLQMessages":                         1,
		"GetDLQReplicationMessages":              1,
		"GetMutableState":                        1,
		"GetReplicationMessages":                 1,
		"IsActivityTaskValid":                    1,
		"IsWorkflowTaskValid":                    1,
		"MergeDLQMessages":                       1,
		"PollMutableState":                       1,
		"PurgeDLQMessages":                       1,
		"QueryWorkflow":                          1,
		"ReapplyEvents":                          1,
		"RebuildMutableState":                    1,
		"RecordActivityTaskHeartbeat":            1,
		"RecordActivityTaskStarted":              1,
		"RecordChildExecutionCompleted":          1,
		"VerifyChildExecutionCompletionRecorded": 1,
		"RecordWorkflowTaskStarted":              1,
		"RefreshWorkflowTasks":                   1,
		"RemoveSignalMutableState":               1,
		"RemoveTask":                             1,
		"ReplicateEventsV2":                      1,
		"ReplicateWorkflowState":                 1,
		"RequestCancelWorkflowExecution":         1,
		"ResetStickyTaskQueue":                   1,
		"ResetWorkflowExecution":                 1,
		"RespondActivityTaskCanceled":            1,
		"RespondActivityTaskCompleted":           1,
		"RespondActivityTaskFailed":              1,
		"RespondWorkflowTaskCompleted":           1,
		"RespondWorkflowTaskFailed":              1,
		"ScheduleWorkflowTask":                   1,
		"VerifyFirstWorkflowTaskScheduled":       1,
		"SignalWithStartWorkflowExecution":       1,
		"SignalWorkflowExecution":                1,
		"StartWorkflowExecution":                 1,
		"SyncActivity":                           1,
		"SyncShardStatus":                        1,
		"TerminateWorkflowExecution":             1,
		"GenerateLastHistoryReplicationTasks":    1,
		"GetReplicationStatus":                   1,
		"DeleteWorkflowVisibilityRecord":         1,
		"UpdateWorkflowExecution":                1,
		"PollWorkflowExecutionUpdate":            1,
		"StreamWorkflowReplicationMessages":      1,
	}

	APIPrioritiesOrdered = []int{OperatorPriority, 1}
)

func NewPriorityRateLimiter(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RequestRateLimiter {
	rateLimiters := make(map[int]quotas.RequestRateLimiter)
	for priority := range APIPrioritiesOrdered {
		if priority == OperatorPriority {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultIncomingRateLimiter(operatorRateFn(rateFn, operatorRPSRatio)))
		} else {
			rateLimiters[priority] = quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultIncomingRateLimiter(rateFn))
		}
	}
	return quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		if req.CallerType == headers.CallerTypeOperator {
			return OperatorPriority
		}
		if priority, ok := APIToPriority[req.API]; ok {
			return priority
		}
		return APIPrioritiesOrdered[len(APIPrioritiesOrdered)-1]
	}, rateLimiters)
}

func operatorRateFn(
	rateFn quotas.RateFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) quotas.RateFn {
	return func() float64 {
		return operatorRPSRatio() * rateFn()
	}
}
